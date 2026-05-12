package config

import "fmt"

func GetBatchInt(cfg map[string]any, key string) (int, error) {
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

func ValidateBatcher(cfg map[string]any) error {
	batchSize, err := GetBatchInt(cfg, "batch_size")
	if err != nil {
		return fmt.Errorf("simple batcher: %w", err)
	}

	if batchSize <= 0 {
		return fmt.Errorf("simple batcher: batch_size cannot be less than or equal to zero")
	}

	flushInterval, err := GetBatchInt(cfg, "flush_interval_ms")
	if err != nil {
		return fmt.Errorf("simple batcher: %w", err)
	}

	if flushInterval < 0 {
		return fmt.Errorf("simple batcher: flush_interval_ms cannot be less than or equal to zero")
	}
	return nil
}
