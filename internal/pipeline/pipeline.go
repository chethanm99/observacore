package pipeline

import (
	"context"
	batcher "observacore/internal/components/batcher_interface"
	exporter "observacore/internal/components/exporter_interface"
	processor "observacore/internal/components/processor_interface"
	receiver "observacore/internal/components/receiver_interface"
	"observacore/internal/model"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

type Pipeline struct {
	Receiver     receiver.Receiver
	Processors   []processor.Processor
	Batcher      batcher.Batcher
	Exporter     exporter.Exporter
	Metrics      *model.MetricCount
	NumOfWorkers int
	Buffersize   int
	Logger       *zap.Logger
	RetryCh      chan model.RetryItem
}

func (p *Pipeline) Start(ctx context.Context) {
	rawCh := make(chan model.Metric, p.Buffersize)
	processedCh := make(chan model.Metric, p.Buffersize)
	batchedCh := make(chan []model.Metric, p.Buffersize)

	go StartMetricsReporter(ctx, p.Metrics, p.Logger, func() int { return len(p.RetryCh) }, func() int { return len(rawCh) }, func() int { return len(processedCh) })
	go p.Exporter.StartRetryWorkers(ctx, p.Metrics, p.Logger)

	go func() {
		p.Receiver.Start(ctx, rawCh, p.Metrics, p.Logger)
		defer close(rawCh)
	}()

	var procWG sync.WaitGroup
	for i := 0; i < p.NumOfWorkers; i++ {
		procWG.Add(1)

		go func(id int) {
			defer procWG.Done()
			defer func() {
				if r := recover(); r != nil {
					p.Logger.Error("processor panic", zap.Any("error", r), zap.Int("worker", id))
				}
			}()

			for {
				select {
				case metric, ok := <-rawCh:
					if !ok {
						return
					}

					current := metric
					processedSuccessfully := true

					for _, proc := range p.Processors {
						start := time.Now()
						current, processedSuccessfully = proc.Process(current)
						duration := time.Since(start)
						atomic.AddInt64(&p.Metrics.ProcessingLatencyNs, duration.Nanoseconds())

						if !processedSuccessfully {
							atomic.AddInt64(&p.Metrics.FilteredCount, 1)
							break
						}
					}

					if processedSuccessfully {
						atomic.AddInt64(&p.Metrics.ProcessedCount, 1)

						select {
						case processedCh <- current:
						case <-ctx.Done():
							return
						}
					}

				case <-ctx.Done():
					return
				}
			}
		}(i)
	}

	go func() {
		procWG.Wait()
		close(processedCh)
	}()

	go func() {
		ticker := time.NewTicker(p.Batcher.GetInterval())
		defer ticker.Stop()
		defer close(batchedCh)

		for {
			select {
			case metric, ok := <-processedCh:
				if !ok {
					if batch, ok := p.Batcher.Flush(); ok {
						select {
						case batchedCh <- batch:
						case <-ctx.Done():
						}
					}
					return
				}

				if batch, ok := p.Batcher.Add(metric); ok {
					select {
					case batchedCh <- batch:
					case <-ctx.Done():
						return
					}
				}

			case <-ticker.C:
				if batch, ok := p.Batcher.Flush(); ok {
					select {
					case batchedCh <- batch:
					case <-ctx.Done():
						return
					}
				}

			case <-ctx.Done():
				if batch, ok := p.Batcher.Flush(); ok {
					batchedCh <- batch
				}
				return
			}
		}
	}()

	var exportWG sync.WaitGroup
	exportWG.Add(1)

	go func() {
		defer exportWG.Done()
		p.Exporter.Export(ctx, batchedCh, p.Metrics, p.Logger)
	}()

	exportWG.Wait()
	p.logSummary()
}

func (p *Pipeline) logSummary() {
	processed := atomic.LoadInt64(&p.Metrics.ProcessedCount)
	exported := atomic.LoadInt64(&p.Metrics.ExportedCount)

	var successRate float64
	if processed > 0 {
		successRate = float64(exported) / float64(processed)
	}
	p.Logger.Info("pipeline_summary",
		zap.Int64("received", atomic.LoadInt64(&p.Metrics.ReceivedCount)),
		zap.Int64("processed", atomic.LoadInt64(&p.Metrics.ProcessedCount)),
		zap.Int64("filtered", atomic.LoadInt64(&p.Metrics.FilteredCount)),
		zap.Int64("dropped", atomic.LoadInt64(&p.Metrics.DroppedCount)),
		zap.Int64("retried", atomic.LoadInt64(&p.Metrics.RetriedCount)),
		zap.Int64("exported", atomic.LoadInt64(&p.Metrics.ExportedCount)),
		zap.Float64("effective_success_rate", successRate),
	)

	p.Logger.Info("shutdown_complete")
}

func StartMetricsReporter(ctx context.Context, metrics *model.MetricCount, logger *zap.Logger, getRetryQueueLen func() int, getRawQueueLen func() int, getProcessedQueueLen func() int) {
	ticker := time.NewTicker(900 * time.Nanosecond)
	defer ticker.Stop()

	var prevProcessed int64
	var prevExported int64
	var prevRetrySuccess int64
	var prevRetryAttempt int64

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			currProcessed := atomic.LoadInt64(&metrics.ProcessedCount)
			currExported := atomic.LoadInt64(&metrics.ExportedCount)
			currRetrySuccess := atomic.LoadInt64(&metrics.RetrySuccess)
			currRetryAttempt := atomic.LoadInt64(&metrics.RetryAttempts)

			processRate := float64(currProcessed-prevProcessed) / 2.0
			exportedRate := float64(currExported-prevExported) / 2.0
			exportCount := atomic.LoadInt64(&metrics.ExportedCount)
			exportLatency := atomic.LoadInt64(&metrics.ExportLatencyNs)
			totalBatches := atomic.LoadInt64(&metrics.TotalBatches)
			totalProcessingLatency := atomic.LoadInt64(&metrics.ProcessingLatencyNs)

			avgProcessingLatency := float64(0)
			if currProcessed > 0 {
				avgProcessingLatency = float64(totalProcessingLatency) / float64(currProcessed) / 1e6
			}

			avgExportLatency := float64(0)
			if exportCount > 0 {
				avgExportLatency = float64(exportLatency) / float64(exportCount) / 1e6
			}

			failureRate := float64(0)
			if currProcessed > 0 {
				failureRate = 1 - float64(currExported)/float64(currProcessed)
			}

			var retrySuccessRate float64
			attemptDelta := currRetryAttempt - prevRetryAttempt
			successDelta := currRetrySuccess - prevRetrySuccess

			processedDelta := currProcessed - prevProcessed
			exportedDelta := currExported - prevExported

			retryRateWindow := float64(0)
			if processedDelta > 0 {
				retryRateWindow = float64(attemptDelta) / float64(processedDelta)
				if retryRateWindow > 1 {
					retryRateWindow = 1
				}
			}

			var retryRateCumulative float64
			if currProcessed > 0 {
				retryRateCumulative = float64(currRetryAttempt) / float64(currProcessed)
			}

			avgBatchSize := float64(0)
			if totalBatches > 0 {
				avgBatchSize = float64(currExported) / float64(totalBatches)
			}

			var windowSuccessRate float64
			if processedDelta > 0 {
				windowSuccessRate = float64(exportedDelta) / float64(processedDelta)
				if windowSuccessRate > 1 {
					windowSuccessRate = 1
				}
			}

			var cumulativeSuccessRate float64
			if currProcessed > 0 {
				cumulativeSuccessRate = float64(currExported) / float64(currProcessed)
			}

			if attemptDelta > 0 {
				retrySuccessRate = float64(successDelta) / float64(attemptDelta)
			}

			logger.Info("runtime_metrics",
				zap.Float64("processed_per_sec", processRate), zap.Float64("exported_per_sec", exportedRate),
				zap.Int("retry_queue_length", getRetryQueueLen()), zap.Int("raw_channel_depth", getRawQueueLen()), zap.Int("processed_channel_depth", getProcessedQueueLen()),
				zap.Float64("retry_success_rate", retrySuccessRate), zap.Float64("window_success_rate", windowSuccessRate), zap.Float64("cummulative_success_rate", cumulativeSuccessRate),
				zap.Float64("retry_rate_window", retryRateWindow), zap.Float64("retry_rate_cumulative", retryRateCumulative), zap.Float64("failure_rate", failureRate),
				zap.Float64("avg_batch_size", avgBatchSize),
				zap.Float64("avg_processing_latency_ms", avgProcessingLatency), zap.Float64("avg_export_latency_ms", avgExportLatency),
			)
			prevProcessed = currProcessed
			prevExported = currExported
			prevRetrySuccess = currRetrySuccess
			prevRetryAttempt = currRetryAttempt
		}
	}
}
