package factory

import (
	"fmt"
	batcher "observacore/internal/components/batcher_interface"
	"observacore/internal/config"
	bc "observacore/internal/pkg/batcher"
	validator "observacore/internal/validator"
	"time"
)

func BuildBatcher(cfg config.BatcherConfig) (batcher.Batcher, error) {
	switch cfg.Type {
	case "simple":
		return BuildSimpleBatcher(cfg)
	default:
		return nil, fmt.Errorf("invalid batcher type %v", cfg.Type)
	}
}

func BuildSimpleBatcher(cfg config.BatcherConfig) (batcher.Batcher, error) {
	if cfg.Type == "simple" {
		if err := validator.ValidateBatcher(cfg.Config); err != nil {
			return nil, err
		}
	}
	batchSize, err := validator.GetBatchInt(cfg.Config, "batch_size")
	if err != nil {
		return nil, fmt.Errorf("simple batcher: %v", err)
	}

	flushInterval, err := validator.GetBatchInt(cfg.Config, "flush_interval_ms")
	if err != nil {
		return nil, fmt.Errorf("simple batcher: %v", err)
	}

	flushIntervalDuration := time.Duration(flushInterval) * time.Millisecond
	b := bc.NewBatchBySize(
		batchSize,
		flushIntervalDuration,
	)
	return b, nil
}
