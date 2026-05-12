package config

import (
	"fmt"
)

func GetRuntimeInt(cfg map[string]any, key string) (int, error) {
	raw, ok := cfg[key]
	if !ok {
		return 0, fmt.Errorf("missing %s", key)
	}

	v, ok := raw.(int)
	if !ok {
		return 0, fmt.Errorf("%s must be int", key)
	}
	return v, nil
}

func validateRuntimeConfig(cfg map[string]any) error {
	numOfWorkers, err := GetRuntimeInt(cfg, "num_of_workers")
	if err != nil {
		return fmt.Errorf("runtime config: %w", err)
	}

	if numOfWorkers <= 0 {
		return fmt.Errorf("runtime config: invalid num_of_workers value")
	}

	bufferSize, err := GetRuntimeInt(cfg, "buffer_size")
	if err != nil {
		return fmt.Errorf("runtime config: %w", err)
	}

	if bufferSize <= 0 {
		return fmt.Errorf("runtime config: %w", err)
	}
	return nil
}
