package exporter

import (
	"context"
	"observacore/internal/model"

	"go.uber.org/zap"
)

type Exporter interface {
	Export(ctx context.Context, in <-chan []model.Metric, metrics *model.MetricCount, logger *zap.Logger)
	StartRetryWorkers(ctx context.Context, metrics *model.MetricCount, logger *zap.Logger)
}
