package storage

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"
)

// AddTrials atomically adds parameter combinations to an open sweep.
func (s *Store) AddTrials(ctx context.Context, sweepID string, specs []TrialSpec) error {
	if !validID(sweepID) {
		return fmt.Errorf("sweep identifier: %w", ErrInvalid)
	}

	prepared := make([]TrialSpec, 0, len(specs))
	positions := make(map[string]int, len(specs))
	for _, spec := range specs {
		if err := ctx.Err(); err != nil {
			return err
		}

		spec, err := spec.Normalize()
		if err != nil {
			return err
		}

		if index, exists := positions[spec.ID]; exists {
			if !sameJSON(prepared[index].Params, spec.Params) {
				return fmt.Errorf("trial %q parameters: %w", spec.ID, ErrConflict)
			}
			continue
		}
		positions[spec.ID] = len(prepared)
		prepared = append(prepared, spec)
	}

	return s.update(ctx, func(tx *Tx, now time.Time) error {
		sweep, err := tx.Sweeps.Get(sweepID)
		if err != nil {
			return err
		}
		return addTrials(tx, sweep, prepared, now)
	})
}

// GetTrial returns a trial within a sweep.
func (s *Store) GetTrial(ctx context.Context, key TrialKey) (Trial, error) {
	if err := key.Validate(); err != nil {
		return Trial{}, err
	}
	return readRecord(ctx, s, func(tx *Tx) (Trial, error) {
		return tx.Trials.Get(key)
	})
}

// ListTrials returns a page of trials in identifier order.
func (s *Store) ListTrials(
	ctx context.Context, sweepID string, page PageRequest,
) (Page[Trial], error) {
	if !validID(sweepID) {
		return Page[Trial]{}, fmt.Errorf("sweep identifier: %w", ErrInvalid)
	}
	prefix := tuple(sweepID)
	scope := tuple("trials", sweepID)
	return readPage[Trial](ctx, s, prefix, scope, page, nil, func(tx *Tx) error {
		_, err := tx.Sweeps.Get(sweepID)
		return err
	})
}

// addTrials writes prepared trials and updates their sweep.
func addTrials(tx *Tx, sweep Sweep, specs []TrialSpec, now time.Time) error {
	changed := false
	for _, spec := range specs {
		if err := tx.Context().Err(); err != nil {
			return err
		}

		key := TrialKey{SweepID: sweep.ID, TrialID: spec.ID}
		existing, err := tx.Trials.Get(key)
		switch {
		case err == nil:
			if !sameJSON(existing.Params, spec.Params) {
				return fmt.Errorf("trial %q parameters: %w", spec.ID, ErrConflict)
			}
			continue
		case !errors.Is(err, ErrNotFound):
			return err
		case sweep.Sealed:
			return ErrSealed
		case sweep.TrialCount == math.MaxUint64:
			return fmt.Errorf("trial count overflow: %w", ErrConflict)
		}

		trial := Trial{
			TrialKey:  key,
			Params:    spec.Params,
			State:     TrialPending,
			CreatedAt: now,
			UpdatedAt: now,
		}
		if err := tx.Trials.Put(key, trial); err != nil {
			return err
		}
		if err := tx.Ready.Put(key); err != nil {
			return err
		}
		sweep.TrialCount++
		changed = true
	}

	if !changed {
		return nil
	}
	updateSweepState(&sweep, now)
	return tx.Sweeps.Put(sweep.ID, sweep)
}
