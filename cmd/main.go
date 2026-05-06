package main

import (
	"context"
	"fmt"
	"observacore/internal/components/batcher"
	"observacore/internal/components/exporter"
	"observacore/internal/components/processor"
	"observacore/internal/components/receiver"
	"observacore/internal/model"
	"observacore/internal/pipeline"
	"time"

	"go.uber.org/zap"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	logger, _ := zap.NewProduction()
	defer logger.Sync()

	metrics := &model.MetricCount{}
	retryCh := make(chan model.RetryItem, 500)

	r := receiver.NewInMemoryReceiver(500 * time.Millisecond)
	p := processor.NewCPUFilterProcessor("CPU", 50)
	b := batcher.NewBatchBySize(4)

	e := exporter.NewConsoleExporter()
	e.RetryCh = retryCh

	pl := &pipeline.Pipeline{
		Receiver:     r,
		Processors:   []processor.Processor{p},
		Batcher:      b,
		Exporter:     e,
		Logger:       logger,
		Metrics:      metrics,
		NumOfWorkers: 5,
		Buffersize:   100,
	}

	if pl.Receiver == nil || pl.Logger == nil || pl.Metrics == nil {
		fmt.Println("Receiver:", pl.Receiver)
		fmt.Println("Logger:", pl.Logger)
		fmt.Println("Metrics:", pl.Metrics)

		panic("dependency wiring failed")
	}

	exporter.StartRetryWorkers(ctx, retryCh, 5, 3, 100*time.Millisecond, logger, metrics)
	pl.Start(ctx)
}
