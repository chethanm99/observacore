package model

type RetryItem struct {
	Batch   []Metric
	Attempt int
}
