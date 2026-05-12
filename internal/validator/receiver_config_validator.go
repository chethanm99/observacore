package config

import (
	"fmt"
)

func GetInt(cfg map[string]any, key string) (int, error) {
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
		return 0, fmt.Errorf("key %s is type %T not a number", key, raw)
	}
}

func getFloat(cfg map[string]any, key string) (float64, error) {
	raw, ok := cfg[key]
	if !ok {
		return 0, fmt.Errorf("missing %s", key)
	}

	v, ok := raw.(float64)
	if !ok {
		return 0, fmt.Errorf("%s must be float", key)
	}
	return v, nil
}

func GetStringSlice(cfg map[string]any, key string) ([]string, error) {
	raw, ok := cfg[key]
	if !ok {
		return nil, fmt.Errorf("missing %s", key)
	}

	list, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("%s must be a list", key)
	}

	if len(list) == 0 {
		return nil, fmt.Errorf("%s cannot be empty", key)
	}

	result := make([]string, 0, len(list))

	for _, item := range list {
		str, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf("%s must be a string", str)
		}
		result = append(result, str)
	}
	return result, nil
}

func ValidateInMemoryReceiver(cfg map[string]any) error {
	interval, err := GetInt(cfg, "interval_ms")
	if err != nil {
		return fmt.Errorf("in-memory receiver: %w", err)
	}

	if interval <= 0 {
		return fmt.Errorf("in-memory receiver: interval_ms must be > 0")
	}

	burstSize, err := GetInt(cfg, "burst_size")
	if err != nil {
		return fmt.Errorf("in-memory receiver: %w", err)
	}

	if burstSize <= 0 {
		return fmt.Errorf("in-memory receiver: burst_size must be > 0")
	}

	_, err = GetStringSlice(cfg, "metric_names")
	if err != nil {
		return fmt.Errorf("in-memory receiver: %w", err)
	}
	return nil
}
