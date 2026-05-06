package exporter

import (
	"context"
	"math/rand"
	"observacore/internal/model"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

func StartRetryWorkers(ctx context.Context, retryCh chan model.RetryItem, numWorkers int, maxRetries int, baseBackOff time.Duration, logger *zap.Logger, metrics *model.MetricCount) {
	for i := 0; i < numWorkers; i++ {
		go func(id int) {
			for {
				select {
				case <-ctx.Done():
				case item, ok := <-retryCh:
					if !ok {
						return
					}
					processRetry(ctx, item, retryCh, maxRetries, baseBackOff, logger, metrics)
				}
			}
		}(i)
	}
}

func processRetry(ctx context.Context, item model.RetryItem, retryCh chan<- model.RetryItem, maxRetries int, baseBackOff time.Duration, logger *zap.Logger, metrics *model.MetricCount) {
	jitter := time.Duration(rand.Intn(100)) * time.Millisecond
	backoff := baseBackOff*time.Duration(1<<item.Attempt) + jitter

	select {
	case <-time.After(backoff):
	case <-ctx.Done():
		atomic.AddInt64(&metrics.DroppedCount, int64(len(item.Batch)))
		logger.Warn("dropping_due_to_shutdown", zap.Int("batch_size", len(item.Batch)), zap.Int("attempt", item.Attempt))
		return
	}

	err := tryExport(item.Batch)
	if err == nil {
		atomic.AddInt64(&metrics.RetrySuccess, 1)
		logger.Info("retry_success", zap.Int("attempt", item.Attempt), zap.Int("batch_size", len(item.Batch)))
		return
	}

	logger.Warn("retry_failed", zap.Int("attempt", item.Attempt), zap.Int("batch_size", len(item.Batch)))

	if item.Attempt >= maxRetries {
		atomic.AddInt64(&metrics.DroppedCount, int64(len(item.Batch)))
		logger.Error("dropping_after_max_retries", zap.Int("batch_size", len(item.Batch)))
		return
	}

	select {
	case retryCh <- model.RetryItem{
		Batch:   item.Batch,
		Attempt: item.Attempt + 1,
	}:
		logger.Debug("retry_enqued")
	default:
		atomic.AddInt64(&metrics.DroppedCount, int64(len(item.Batch)))
		logger.Error("full_retry_queue_dropping", zap.Int("batch_size", len(item.Batch)))

	}
}
