package exporter

import (
	"context"
	"fmt"
	"observacore/internal/model"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

type ConsoleExporter struct {
	MaxRetries   int
	BaseBackOff  time.Duration
	RetryCh      chan model.RetryItem
	RetryWorkers int
}

func NewConsoleExporter(maxRetries int, baseBackOff time.Duration, retryCh chan model.RetryItem, retryWorkers int) *ConsoleExporter {
	return &ConsoleExporter{
		MaxRetries:   maxRetries,
		BaseBackOff:  baseBackOff,
		RetryCh:      retryCh,
		RetryWorkers: retryWorkers,
	}
}

func (c *ConsoleExporter) Export(ctx context.Context, in <-chan []model.Metric, metrics *model.MetricCount, logger *zap.Logger) {
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Exporter shutting down...")
			return

		case batch, ok := <-in:
			if !ok {
				fmt.Println("All batches processed")
				return
			}
			atomic.AddInt64(&metrics.RetryAttempts, 1)
			start := time.Now()
			err := tryExport(batch)
			duration := time.Since(start)
			atomic.AddInt64(&metrics.ExportLatencyNs, duration.Nanoseconds())
			if err != nil {
				atomic.AddInt64(&metrics.FailedExports, 1)
				fmt.Println("Enqueing to retry")

				select {
				case c.RetryCh <- model.RetryItem{
					Batch:   batch,
					Attempt: 1,
				}:
					atomic.AddInt64(&metrics.RetriedCount, 1)
				default:
					atomic.AddInt64(&metrics.DroppedCount, 1)
					logger.Warn("retry_queue_full_dropping_batch")
				}
				continue
			}

			atomic.AddInt64(&metrics.TotalBatches, 1)
			atomic.AddInt64(&metrics.ExportedCount, int64(len(batch)))
			logger.Info("export_success", zap.String("component", "exporter"), zap.Int("batch_size", len(batch)), zap.Int64("exported_total", metrics.ExportedCount))
			for _, m := range batch {
				latency := time.Since(m.Timestamp)
				atomic.AddInt64(&metrics.EndToEndLatencyNs, latency.Nanoseconds())
				fmt.Printf("ID:%d Name:%s Value:%.2f Timestamp:%s\n", m.ID, m.Name, m.Value, m.Timestamp.Format(time.RFC3339Nano))
			}
			fmt.Println("----")
		}
	}
}

func tryExport(batch []model.Metric) error {
	if time.Now().UnixNano()%10 == 0 {
		return fmt.Errorf("simulated network failure")
	}
	return nil
}
