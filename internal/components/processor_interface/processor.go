package processor

import (
	"observacore/internal/model"
)

type Processor interface {
	Process(metric model.Metric) (model.Metric, bool)
}
