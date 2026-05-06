package receiver

import (
	"context"
	"math/rand"
	"observacore/internal/model"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

type Receiver interface {
	Start(ctx context.Context, out chan<- model.Metric, metrics *model.MetricCount, logger *zap.Logger)
}

type InMemoryReceiver struct {
	Interval time.Duration
}

func NewInMemoryReceiver(interval time.Duration) *InMemoryReceiver {
	return &InMemoryReceiver{
		Interval: interval,
	}
}

func (r *InMemoryReceiver) Start(ctx context.Context, out chan<- model.Metric, metrics *model.MetricCount, logger *zap.Logger) {
	id := 1
	names := []string{"CPU", "Storage", "Disk"}

	ticker := time.NewTicker(r.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			metric := model.Metric{
				ID:        id,
				Name:      names[rand.Intn(len(names))],
				Value:     rand.Float64() * 100,
				Timestamp: time.Now(),
			}

			select {
			case out <- metric:
				atomic.AddInt64(&metrics.ReceivedCount, 1)
				id++
				logger.Debug("metric_received", zap.String("component", "receiver"), zap.Int("id", metric.ID), zap.String("name", metric.Name), zap.Float64("value", metric.Value), zap.String("time_stamp", metric.Timestamp.Format(time.RFC3339Nano)))
			default:
			}
		}
	}
}
