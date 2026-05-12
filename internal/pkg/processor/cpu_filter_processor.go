package processor

import (
	"observacore/internal/model"
)

type CPUFilterProcessor struct {
	Name      string
	Threshold float64
}

func NewCPUFilterProcessor(name string, threshold float64) *CPUFilterProcessor {
	return &CPUFilterProcessor{
		Name:      name,
		Threshold: threshold,
	}
}

func (c *CPUFilterProcessor) Process(metric model.Metric) (model.Metric, bool) {
	if metric.Name == c.Name && metric.Value > c.Threshold {
		return metric, true
	}
	return model.Metric{}, false
}
