package factory

import (
	"fmt"
	receiver "observacore/internal/components/receiver_interface"
	"observacore/internal/config"
	rc "observacore/internal/pkg/receiver"
	validator "observacore/internal/validator"
	"time"
)

func BuildReceiver(cfg config.ReceiverConfig) (receiver.Receiver, error) {
	switch cfg.Type {
	case "in-memory":
		return BuildInMemoryReceiver(cfg)
	default:
		return nil, fmt.Errorf("invalid receiver type %s", cfg.Type)
	}
}

func BuildInMemoryReceiver(cfg config.ReceiverConfig) (receiver.Receiver, error) {
	if cfg.Type == "in-memory" {
		if err := validator.ValidateInMemoryReceiver(cfg.Config); err != nil {
			return nil, err
		}
	}

	interval, err := validator.GetInt(cfg.Config, "interval_ms")
	if err != nil {
		return nil, fmt.Errorf("in-memory receiver: %w", err)
	}

	burstSize, err := validator.GetInt(cfg.Config, "burst_size")
	if err != nil {
		return nil, fmt.Errorf("in-memory receiver: %w", err)
	}

	metricNames, err := validator.GetStringSlice(cfg.Config, "metric_names")
	if err != nil {
		return nil, fmt.Errorf("in-memory receiver: %w", err)
	}

	intervalDuration := time.Duration(interval) * time.Millisecond

	r := rc.NewInMemoryReceiver(
		intervalDuration,
		burstSize,
		metricNames,
	)
	return r, nil
}
