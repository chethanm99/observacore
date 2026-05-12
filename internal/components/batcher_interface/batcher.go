package batcher

import (
	"observacore/internal/model"
	"time"
)

type Batcher interface {
	Add(metric model.Metric) ([]model.Metric, bool)
	Flush() ([]model.Metric, bool)
	GetInterval() time.Duration
}
