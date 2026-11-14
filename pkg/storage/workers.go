package storage

import (
	"context"
	"fmt"
)

// GetWorker returns the worker contact record.
func (s *Store) GetWorker(ctx context.Context, id string) (Worker, error) {
	if id == "" {
		return Worker{}, fmt.Errorf("worker identifier: %w", ErrInvalid)
	}
	return readRecord(ctx, s, func(tx *Tx) (Worker, error) {
		return tx.Workers.Get(id)
	})
}

// ListWorkers returns a page of workers in identifier order.
func (s *Store) ListWorkers(ctx context.Context, page PageRequest) (Page[Worker], error) {
	return readPage[Worker](ctx, s, nil, tuple("workers"), page, nil, nil)
}
