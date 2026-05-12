package factory

import (
	"fmt"
	processor "observacore/internal/components/processor_interface"
	"observacore/internal/config"
	pc "observacore/internal/pkg/processor"
	validator "observacore/internal/validator"
)

func BuildProcessor(cfg config.ProcessorConfig) (processor.Processor, error) {
	switch cfg.Type {
	case "cpu_filter":
		return BuildCPUProcessor(cfg)
	default:
		return nil, fmt.Errorf("invalid processor type %v", cfg.Type)
	}
}

func BuildCPUProcessor(cfg config.ProcessorConfig) (processor.Processor, error) {
	if cfg.Type == "cpu_filter" {
		if err := validator.ValidateCPUProcessor(cfg.Config); err != nil {
			return nil, err
		}
	}

	name, err := validator.GetString(cfg.Config, "metric_name")
	if err != nil {
		return nil, fmt.Errorf("cpu processor: %w", err)
	}

	threshold, err := validator.GetFloat64(cfg.Config, "threshold")
	if err != nil {
		return nil, fmt.Errorf("cpu processor: %w", err)
	}

	p := pc.NewCPUFilterProcessor(
		name,
		threshold,
	)
	return p, nil
}
