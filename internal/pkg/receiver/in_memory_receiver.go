package receiver

import (
	"context"
	"fmt"
	"math/rand"
	"observacore/internal/model"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

type InMemoryReceiver struct {
	Interval    time.Duration
	BurstSize   int
	MetricNames []string
}

func NewInMemoryReceiver(interval time.Duration, burstSize int, metricNames []string) *InMemoryReceiver {
	return &InMemoryReceiver{
		Interval:    interval,
		BurstSize:   burstSize,
		MetricNames: metricNames,
	}
}

func (r *InMemoryReceiver) Start(ctx context.Context, out chan<- model.Metric, metrics *model.MetricCount, logger *zap.Logger) {
	id := 1

	ticker := time.NewTicker(r.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for i := 0; i < r.BurstSize; i++ {
				metric := model.Metric{
					ID:        id,
					Name:      r.MetricNames[rand.Intn(len(r.MetricNames))],
					Value:     rand.Float64() * 100,
					Timestamp: time.Now(),
				}

				select {
				case out <- metric:
					atomic.AddInt64(&metrics.ReceivedCount, 1)
					id++
					logger.Debug("metric_received", zap.String("component", "receiver"), zap.Int("id", metric.ID), zap.String("name", metric.Name), zap.Float64("value", metric.Value), zap.String("time_stamp", metric.Timestamp.Format(time.RFC3339Nano)))
				default:
					fmt.Println("Dropping metrics as the buffer is full")
					atomic.AddInt64(&metrics.DroppedCount, 1)
				}
			}
		}
	}
}
