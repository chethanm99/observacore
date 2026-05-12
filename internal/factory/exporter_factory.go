package factory

import (
	"fmt"
	exporter "observacore/internal/components/exporter_interface"
	"observacore/internal/config"
	"observacore/internal/model"
	ec "observacore/internal/pkg/exporter"
	validator "observacore/internal/validator"
	"time"
)

func BuildExporter(cfg config.ExporterConfig, retryCh chan model.RetryItem) (exporter.Exporter, error) {
	switch cfg.Type {
	case "console":
		return BuildConsoleExporter(cfg, retryCh)
	default:
		return nil, fmt.Errorf("exporter type %s not supported", cfg.Type)
	}
}

func BuildConsoleExporter(cfg config.ExporterConfig, retryCh chan model.RetryItem) (exporter.Exporter, error) {
	if cfg.Type == "console" {
		if err := validator.ValidateExporterConfig(cfg.Config); err != nil {
			return nil, err
		}
	}
	maxRetries, err := validator.GetExportInt(cfg.Config, "max_retries")
	if err != nil {
		return nil, fmt.Errorf("console exporter: %w", err)
	}

	baseBackOff, err := validator.GetExportInt(cfg.Config, "base_backoff_ms")
	if err != nil {
		return nil, fmt.Errorf("console exporter: %w", err)
	}

	retryWorkers, err := validator.GetBatchInt(cfg.Config, "retry_workers")
	if err != nil {
		return nil, fmt.Errorf("console exporter: %w", err)
	}

	baseBackOffInterval := time.Duration(baseBackOff) * time.Millisecond

	e := ec.NewConsoleExporter(
		maxRetries,
		baseBackOffInterval,
		retryCh,
		retryWorkers,
	)
	return e, nil
}
