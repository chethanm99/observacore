package receiver

import (
	"context"
	"observacore/internal/model"

	"go.uber.org/zap"
)

type Receiver interface {
	Start(ctx context.Context, out chan<- model.Metric, metrics *model.MetricCount, logger *zap.Logger)
}
