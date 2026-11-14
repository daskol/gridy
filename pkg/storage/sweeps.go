package storage

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// CreateSweep stores a new sweep with its execution policy.
func (s *Store) CreateSweep(ctx context.Context, spec SweepSpec) (Sweep, error) {
	spec, err := spec.Normalize()
	if err != nil {
		return Sweep{}, err
	}

	sweep := Sweep{SweepSpec: spec, State: SweepPending}
	err = s.update(ctx, func(tx *Tx, now time.Time) error {
		if tx.Sweeps.Exists(spec.ID) {
			return fmt.Errorf("sweep %q: %w", spec.ID, ErrAlreadyExists)
		}
		sweep.CreatedAt = now
		sweep.UpdatedAt = now
		return tx.Sweeps.Put(spec.ID, sweep)
	})
	if err != nil {
		return Sweep{}, err
	}
	return sweep, nil
}

// GetSweep returns a sweep by identifier.
func (s *Store) GetSweep(ctx context.Context, id string) (Sweep, error) {
	if !validID(id) {
		return Sweep{}, fmt.Errorf("sweep identifier: %w", ErrInvalid)
	}
	return readRecord(ctx, s, func(tx *Tx) (Sweep, error) {
		return tx.Sweeps.Get(id)
	})
}

// ListSweeps returns a page of sweeps in identifier order.
func (s *Store) ListSweeps(ctx context.Context, page PageRequest) (Page[Sweep], error) {
	return readPage[Sweep](ctx, s, nil, tuple("sweeps"), page, nil, nil)
}

// SearchSweeps matches sweep identifiers and codes without case sensitivity.
func (s *Store) SearchSweeps(
	ctx context.Context, query string, page PageRequest,
) (Page[Sweep], error) {
	query = strings.ToLower(query)
	scope := tuple("search_sweeps", query)
	return readPage(ctx, s, nil, scope, page, func(sweep Sweep) bool {
		return strings.Contains(strings.ToLower(sweep.ID), query) ||
			strings.Contains(strings.ToLower(sweep.Code), query)
	}, nil)
}

// SealSweep marks trial generation as complete.
func (s *Store) SealSweep(ctx context.Context, sweepID string) error {
	if !validID(sweepID) {
		return fmt.Errorf("sweep identifier: %w", ErrInvalid)
	}
	return s.update(ctx, func(tx *Tx, now time.Time) error {
		sweep, err := tx.Sweeps.Get(sweepID)
		switch {
		case err != nil:
			return err
		case sweep.Sealed:
			return nil
		default:
			sweep.Sealed = true
			updateSweepState(&sweep, now)
			return tx.Sweeps.Put(sweepID, sweep)
		}
	})
}

// updateSweepState finalizes sealed sweeps when all trials finish. It checks
// FailedCount and updates UpdatedAt.
func updateSweepState(sweep *Sweep, now time.Time) {
	if sweep.Sealed && sweep.TerminalCount == sweep.TrialCount {
		if sweep.FailedCount > 0 {
			sweep.State = SweepFailed
		} else {
			sweep.State = SweepCompleted
		}
	}
	sweep.UpdatedAt = now
}
