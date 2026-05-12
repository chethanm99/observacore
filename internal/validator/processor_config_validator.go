package config

import (
	"fmt"
)

func GetString(cfg map[string]any, key string) (string, error) {
	raw, ok := cfg[key]
	if !ok {
		return "", fmt.Errorf("missing %s", key)
	}

	v, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf("%s must be string", key)
	}
	return v, nil
}

func GetFloat64(cfg map[string]any, key string) (float64, error) {
	raw, ok := cfg[key]
	if !ok {
		return 0, fmt.Errorf("missing %s", key)
	}

	v, ok := raw.(float64)
	if !ok {
		return 0, fmt.Errorf("%v must be float", key)
	}
	return v, nil
}

func ValidateCPUProcessor(cfg map[string]any) error {
	name, err := GetString(cfg, "metric_name")
	if err != nil {
		return fmt.Errorf("cpu processor: %w", err)
	}

	if len(name) == 0 {
		return fmt.Errorf("cpu processor: name field cannot be empty")
	}

	threshold, err := GetFloat64(cfg, "threshold")
	if err != nil {
		return fmt.Errorf("cpu processor: %w", err)
	}

	if threshold < 50 {
		return fmt.Errorf("cpu processor: threshold must be greater than or equal to 50")
	}
	return nil
}
