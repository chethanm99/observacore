package model

import (
	"time"
)

type Metric struct {
	ID        int
	Name      string
	Value     float64
	Timestamp time.Time
}

type MetricCount struct {
	ReceivedCount  int64
	ProcessedCount int64
	ExportedCount  int64
	RetriedCount   int64
	FilteredCount  int64
	DroppedCount   int64

	RetrySuccess  int64
	RetryAttempts int64

	FailedExports int64
	TotalBatches  int64

	ProcessingLatencyNs int64
	ExportLatencyNs     int64
	EndToEndLatencyNs   int64
}
