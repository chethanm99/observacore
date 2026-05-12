package main

import (
	"context"
	processor "observacore/internal/components/processor_interface"
	"observacore/internal/config"
	"observacore/internal/factory"
	"observacore/internal/model"
	"observacore/internal/pipeline"
	"time"

	"go.uber.org/zap"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	logger, _ := zap.NewProduction()
	defer logger.Sync()

	metrics := &model.MetricCount{}
	retryCh := make(chan model.RetryItem, 500)
	cfg, err := config.LoadYAMLConfigFile("config.yaml")
	if err != nil {
		logger.Fatal("failed to load config", zap.Error(err))
	}

	r, err := factory.BuildReceiver(cfg.Pipeline.Receiver)
	if err != nil {
		logger.Fatal("failed to build receiver", zap.Error(err))
	}

	var processors []processor.Processor
	for _, procCfg := range cfg.Pipeline.Processors {
		p, err := factory.BuildProcessor(procCfg)
		if err != nil {
			logger.Fatal("failed to build processor", zap.Error(err))
		}
		processors = append(processors, p)
	}

	b, err := factory.BuildBatcher(cfg.Pipeline.Batcher)
	if err != nil {
		logger.Fatal("failed to build batcher", zap.Error(err))
	}

	e, err := factory.BuildExporter(cfg.Pipeline.Exporter, retryCh)
	if err != nil {
		logger.Fatal("failed to build exporter")
	}

	pl := &pipeline.Pipeline{
		Receiver:     r,
		Processors:   processors,
		Batcher:      b,
		Exporter:     e,
		Logger:       logger,
		Metrics:      metrics,
		NumOfWorkers: cfg.Runtime.NumOfWorkers,
		Buffersize:   cfg.Runtime.Buffersize,
	}

	pl.Start(ctx)
}
