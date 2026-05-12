package config

import (
	"fmt"
)

func GetExportInt(cfg map[string]any, key string) (int, error) {
	raw, ok := cfg[key]
	if !ok {
		return 0, fmt.Errorf("missing %s", key)
	}

	switch v := raw.(type) {
	case int:
		return v, nil
	case int64:
		return int(v), nil
	case float64:
		return int(v), nil
	default:
		return 0, fmt.Errorf("%s must be int, got %T", key, raw)
	}
}

func ValidateExporterConfig(cfg map[string]any) error {
	maxRetries, err := GetExportInt(cfg, "max_retries")
	if err != nil {
		return fmt.Errorf("console exporter: %w", err)
	}

	if maxRetries <= 0 {
		return fmt.Errorf("console exporter: max_retries cannot be less than or equal to zero")
	}

	baseBackOff, err := GetExportInt(cfg, "base_backoff_ms")
	if err != nil {
		return fmt.Errorf("console exporter: %w", err)
	}

	if baseBackOff <= 0 {
		return fmt.Errorf("console exporter: base_backoff cannot be less than or equal to zero")
	}

	retryWorkers, err := GetExportInt(cfg, "retry_workers")
	if err != nil {
		return fmt.Errorf("console exporter: %w", err)
	}

	if retryWorkers <= 0 {
		return fmt.Errorf("console exporter: retyr_workers cannot be less than or equal to zero")
	}
	return nil
}
