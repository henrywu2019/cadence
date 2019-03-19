// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package replicator

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber/cadence/common/task"

	farm "github.com/dgryski/go-farm"
	"github.com/uber/cadence/common/definition"

	"github.com/uber/cadence/common/locks"

	"github.com/uber-common/bark"
	h "github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/replicator"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client/history"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/codec"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/xdc"
	"go.uber.org/yarpc/yarpcerrors"
)

type (
	// DomainReplicator is the interface which can replicate the domain
	DomainReplicator interface {
		HandleReceivingTask(task *replicator.DomainTaskAttributes) error
	}

	replicationTaskProcessor struct {
		currentCluster          string
		sourceCluster           string
		topicName               string
		consumerName            string
		client                  messaging.Client
		consumer                messaging.Consumer
		isStarted               int32
		isStopped               int32
		shutdownWG              sync.WaitGroup
		shutdownCh              chan struct{}
		config                  *Config
		logger                  bark.Logger
		metricsClient           metrics.Client
		domainReplicator        DomainReplicator
		historyRereplicator     xdc.HistoryRereplicator
		historyClient           history.Client
		msgEncoder              codec.BinaryEncoder
		rereplicationLock       locks.IDMutex
		sequentialTaskProcessor task.SequentialTaskProcessor
	}
)

const (
	dropSyncShardTaskTimeThreshold = 10 * time.Minute
	rereplicationLockShards        = uint32(32)
)

var (
	// ErrEmptyReplicationTask is the error to indicate empty replication task
	ErrEmptyReplicationTask = &shared.BadRequestError{Message: "empty replication task"}
	// ErrUnknownReplicationTask is the error to indicate unknown replication task type
	ErrUnknownReplicationTask = &shared.BadRequestError{Message: "unknown replication task"}
	// ErrDeserializeReplicationTask is the error to indicate failure to deserialize replication task
	ErrDeserializeReplicationTask = &shared.BadRequestError{Message: "Failed to deserialize replication task"}
)

func newReplicationTaskProcessor(currentCluster, sourceCluster, consumer string, client messaging.Client, config *Config,
	logger bark.Logger, metricsClient metrics.Client, domainReplicator DomainReplicator,
	historyRereplicator xdc.HistoryRereplicator, historyClient history.Client) *replicationTaskProcessor {

	retryableHistoryClient := history.NewRetryableClient(historyClient, common.CreateHistoryServiceRetryPolicy(),
		common.IsWhitelistServiceTransientError)

	return &replicationTaskProcessor{
		currentCluster:      currentCluster,
		sourceCluster:       sourceCluster,
		consumerName:        consumer,
		client:              client,
		shutdownCh:          make(chan struct{}),
		config:              config,
		logger:              logger,
		metricsClient:       metricsClient,
		domainReplicator:    domainReplicator,
		historyRereplicator: historyRereplicator,
		historyClient:       retryableHistoryClient,
		msgEncoder:          codec.NewThriftRWEncoder(),
		rereplicationLock: locks.NewIDMutex(rereplicationLockShards, func(key interface{}) uint32 {
			id, ok := key.(definition.WorkflowIdentifier)
			if !ok {
				return 0
			}
			return farm.Fingerprint32([]byte(id.DomainID + id.WorkflowID + id.RunID))
		}),
		sequentialTaskProcessor: task.NewSequentialTaskProcessor(
			config.ReplicatorConcurrency(),
			replicationSequentialTaskQueueHashFn,
			logger,
		),
	}
}

func (p *replicationTaskProcessor) Start() error {
	if !atomic.CompareAndSwapInt32(&p.isStarted, 0, 1) {
		return nil
	}

	logging.LogReplicationTaskProcessorStartingEvent(p.logger)
	consumer, err := p.client.NewConsumerWithClusterName(p.currentCluster, p.sourceCluster, p.consumerName, p.config.ReplicatorConcurrency())
	if err != nil {
		logging.LogReplicationTaskProcessorStartFailedEvent(p.logger, err)
		return err
	}

	if err := consumer.Start(); err != nil {
		logging.LogReplicationTaskProcessorStartFailedEvent(p.logger, err)
		return err
	}

	p.consumer = consumer
	p.shutdownWG.Add(1)
	go p.processorPump()

	logging.LogReplicationTaskProcessorStartedEvent(p.logger)
	return nil
}

func (p *replicationTaskProcessor) Stop() {
	if !atomic.CompareAndSwapInt32(&p.isStopped, 0, 1) {
		return
	}

	logging.LogReplicationTaskProcessorShuttingDownEvent(p.logger)
	defer logging.LogReplicationTaskProcessorShutdownEvent(p.logger)

	if atomic.LoadInt32(&p.isStarted) == 1 {
		close(p.shutdownCh)
	}

	if success := common.AwaitWaitGroup(&p.shutdownWG, time.Minute); !success {
		logging.LogReplicationTaskProcessorShutdownTimedoutEvent(p.logger)
	}
}

func (p *replicationTaskProcessor) processorPump() {
	defer p.shutdownWG.Done()

	var workerWG sync.WaitGroup
	for workerID := 0; workerID < p.config.ReplicatorConcurrency(); workerID++ {
		workerWG.Add(1)
		go p.messageProcessLoop(&workerWG, workerID)
	}

	select {
	case <-p.shutdownCh:
		// Processor is shutting down, close the underlying consumer
		p.consumer.Stop()
	}

	p.logger.Info("Replication task processor pump shutting down.")
	if success := common.AwaitWaitGroup(&workerWG, 10*time.Second); !success {
		p.logger.Warn("Replication task processor timed out on worker shutdown.")
	}
}

func (p *replicationTaskProcessor) messageProcessLoop(workerWG *sync.WaitGroup, workerID int) {
	defer workerWG.Done()

	for {
		select {
		case msg, ok := <-p.consumer.Messages():
			if !ok {
				p.logger.Info("Worker for replication task processor shutting down.")
				return // channel closed
			}
			p.decodeMsgAndApply(msg, workerID)
		}
	}
}

func (p *replicationTaskProcessor) decodeMsgAndApply(msg messaging.Message, workerID int) {
	logger := p.initLogger(msg)
	replicationTask, err := p.decodeAndValidateMsg(msg, logger)
	if err != nil {
		p.updateFailureMetric(metrics.ReplicatorScope, err)
		logger.WithFields(bark.Fields{
			logging.TagErr:        err,
			logging.TagAttemptEnd: time.Now(),
		}).Error(ErrDeserializeReplicationTask.Error())
		msg.Nack()
	}

	for {
		var scope int
		switch replicationTask.GetTaskType() {
		case replicator.ReplicationTaskTypeDomain:
			logger = logger.WithFields(bark.Fields{
				logging.TagDomainID: replicationTask.DomainTaskAttributes.GetID(),
			})
			scope = metrics.DomainReplicationTaskScope
			err = p.handleDomainReplicationTask(replicationTask, msg, logger)
		case replicator.ReplicationTaskTypeSyncShardStatus:
			scope = metrics.SyncShardTaskScope
			err = p.handleSyncShardTask(replicationTask, msg, logger)
		case replicator.ReplicationTaskTypeSyncActivity:
			scope = metrics.SyncActivityTaskScope
			err = p.handleActivityTask(replicationTask, msg, logger)
		case replicator.ReplicationTaskTypeHistory:
			scope = metrics.HistoryReplicationTaskScope
			err = p.handleHistoryReplicationTask(replicationTask, msg, logger)
		default:
			logger.Error("Unknown task type.")
			scope = metrics.ReplicatorScope
			err = ErrUnknownReplicationTask
		}

		if err != nil {
			p.updateFailureMetric(scope, err)

			if !p.isTransientRetryableError(err) {
				break
			}
		}
		break
	}

	if err != nil {
		logger.WithFields(bark.Fields{
			logging.TagErr:        err,
			logging.TagAttemptEnd: time.Now(),
		}).Error("Error processing replication task.")
		msg.Nack()
	}
}

func (p *replicationTaskProcessor) initLogger(msg messaging.Message) bark.Logger {
	return p.logger.WithFields(bark.Fields{
		logging.TagPartition:    msg.Partition(),
		logging.TagOffset:       msg.Offset(),
		logging.TagAttemptStart: time.Now(),
	})
}

func (p *replicationTaskProcessor) decodeAndValidateMsg(msg messaging.Message, logger bark.Logger) (*replicator.ReplicationTask, error) {
	var replicationTask replicator.ReplicationTask
	err := p.msgEncoder.Decode(msg.Value(), &replicationTask)
	if err != nil {
		// return BadRequestError so processWithRetry can nack the message
		return nil, ErrDeserializeReplicationTask
	}

	if replicationTask.TaskType == nil {
		return nil, ErrEmptyReplicationTask
	}

	return &replicationTask, nil
}

func (p *replicationTaskProcessor) handleDomainReplicationTask(task *replicator.ReplicationTask, msg messaging.Message, logger bark.Logger) error {
	p.metricsClient.IncCounter(metrics.DomainReplicationTaskScope, metrics.ReplicatorMessages)
	sw := p.metricsClient.StartTimer(metrics.DomainReplicationTaskScope, metrics.ReplicatorLatency)
	defer sw.Stop()

	logger.Debugf("Received domain replication task %v.", task.DomainTaskAttributes)
	err := p.domainReplicator.HandleReceivingTask(task.DomainTaskAttributes)
	if err == nil {
		msg.Ack()
	}
	return err
}

func (p *replicationTaskProcessor) handleSyncShardTask(task *replicator.ReplicationTask, msg messaging.Message, logger bark.Logger) error {
	p.metricsClient.IncCounter(metrics.SyncShardTaskScope, metrics.ReplicatorMessages)
	sw := p.metricsClient.StartTimer(metrics.SyncShardTaskScope, metrics.ReplicatorLatency)
	defer sw.Stop()

	attr := task.SyncShardStatusTaskAttributes
	logger.Debugf("Received sync shard task %v.", attr)

	if time.Now().Sub(time.Unix(0, attr.GetTimestamp())) > dropSyncShardTaskTimeThreshold {
		return nil
	}

	req := &h.SyncShardStatusRequest{
		SourceCluster: attr.SourceCluster,
		ShardId:       attr.ShardId,
		Timestamp:     attr.Timestamp,
	}
	ctx, cancel := context.WithTimeout(context.Background(), replicationTimeout)
	defer cancel()
	err := p.historyClient.SyncShardStatus(ctx, req)
	if err == nil {
		msg.Ack()
	}
	return err
}

func (p *replicationTaskProcessor) handleActivityTask(task *replicator.ReplicationTask, msg messaging.Message, logger bark.Logger) error {
	activityReplicationTask := newActivityReplicationTask(task, msg, logger,
		p.config, p.historyClient, p.metricsClient, p.historyRereplicator, p.rereplicationLock)
	return p.sequentialTaskProcessor.Submit(activityReplicationTask)
}

func (p *replicationTaskProcessor) handleHistoryReplicationTask(task *replicator.ReplicationTask, msg messaging.Message, logger bark.Logger) error {
	historyReplicationTask := newHistoryReplicationTask(task, msg, p.sourceCluster, logger,
		p.config, p.historyClient, p.metricsClient, p.historyRereplicator, p.rereplicationLock)
	return p.sequentialTaskProcessor.Submit(historyReplicationTask)
}

func (p *replicationTaskProcessor) updateFailureMetric(scope int, err error) {
	// Always update failure counter for all replicator errors
	p.metricsClient.IncCounter(scope, metrics.ReplicatorFailures)

	// Also update counter to distinguish between type of failures
	switch err := err.(type) {
	case *h.ShardOwnershipLostError:
		p.metricsClient.IncCounter(scope, metrics.CadenceErrShardOwnershipLostCounter)
	case *shared.BadRequestError:
		p.metricsClient.IncCounter(scope, metrics.CadenceErrBadRequestCounter)
	case *shared.DomainNotActiveError:
		p.metricsClient.IncCounter(scope, metrics.CadenceErrDomainNotActiveCounter)
	case *shared.WorkflowExecutionAlreadyStartedError:
		p.metricsClient.IncCounter(scope, metrics.CadenceErrExecutionAlreadyStartedCounter)
	case *shared.EntityNotExistsError:
		p.metricsClient.IncCounter(scope, metrics.CadenceErrEntityNotExistsCounter)
	case *shared.LimitExceededError:
		p.metricsClient.IncCounter(scope, metrics.CadenceErrLimitExceededCounter)
	case *shared.RetryTaskError:
		p.metricsClient.IncCounter(scope, metrics.CadenceErrRetryTaskCounter)
	case *yarpcerrors.Status:
		if err.Code() == yarpcerrors.CodeDeadlineExceeded {
			p.metricsClient.IncCounter(scope, metrics.CadenceErrContextTimeoutCounter)
		}
	}
}

func (p *replicationTaskProcessor) isTransientRetryableError(err error) bool {
	switch err.(type) {
	case *shared.BadRequestError:
		return false
	default:
		return true
	}
}
