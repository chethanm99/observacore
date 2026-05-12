package batcher

import (
	"observacore/internal/model"
	"sync"
	"time"
)

type BatchBySize struct {
	Size          int
	batch         []model.Metric
	mu            sync.Mutex
	FlushInterval time.Duration
}

func NewBatchBySize(size int, flushInterval time.Duration) *BatchBySize {
	return &BatchBySize{
		Size:          size,
		FlushInterval: flushInterval,
		batch:         make([]model.Metric, 0, size),
	}
}

func (b *BatchBySize) Add(m model.Metric) ([]model.Metric, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.batch = append(b.batch, m)

	if len(b.batch) >= b.Size {
		return b.FlushLocked()
	}
	return nil, false
}

func (b *BatchBySize) Flush() ([]model.Metric, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.FlushLocked()
}

func (b *BatchBySize) FlushLocked() ([]model.Metric, bool) {
	if len(b.batch) == 0 {
		return nil, false
	}

	out := make([]model.Metric, len(b.batch))
	copy(out, b.batch)

	for i := range b.batch {
		b.batch[i] = model.Metric{}
	}
	b.batch = b.batch[:0]
	return out, true
}

func (b *BatchBySize) GetInterval() time.Duration {
	return b.FlushInterval
}
