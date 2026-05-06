package batcher

import (
	"observacore/internal/model"
	"sync"
)

type Batcher interface {
	Add(metric model.Metric) ([]model.Metric, bool)
	Flush() ([]model.Metric, bool)
}

type BatchBySize struct {
	Size  int
	batch []model.Metric
	mu    sync.Mutex
}

func NewBatchBySize(size int) *BatchBySize {
	return &BatchBySize{
		Size:  size,
		batch: make([]model.Metric, 0, size),
	}
}

func (b *BatchBySize) Add(m model.Metric) ([]model.Metric, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.batch = append(b.batch, m)

	if len(b.batch) >= b.Size {
		out := make([]model.Metric, len(b.batch))
		copy(out, b.batch)
		b.batch = b.batch[:0]
		return out, true
	}
	return nil, false
}

func (b *BatchBySize) Flush() ([]model.Metric, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.batch) == 0 {
		return nil, false
	}
	out := make([]model.Metric, len(b.batch))
	copy(out, b.batch)
	b.batch = b.batch[:0]
	return out, true
}
