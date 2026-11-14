package storage

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ImportSweeps atomically imports and seals sweeps, preserving existing work.
//
// An existing sweep must have the same search space and trial definitions.
// Object key order and equivalent JSON numbers do not change a definition.
// Array order does. All changes roll back if any import conflicts or fails.
func (s *Store) ImportSweeps(ctx context.Context, imports []SweepImport) error {
	prepared, err := normalizeImports(ctx, imports)
	if err != nil {
		return err
	}

	return s.update(ctx, func(tx *Tx, now time.Time) error {
		for _, item := range prepared {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := importSweep(tx, item, now); err != nil {
				return fmt.Errorf("sweep %q: %w", item.Spec.ID, err)
			}
		}
		return nil
	})
}

func importSweep(tx *Tx, item SweepImport, now time.Time) error {
	sweep, err := prepareImportedSweep(tx, item, now)
	switch {
	case err != nil:
		return err
	case sweep.Sealed:
		return nil
	}
	if err := addTrials(tx, sweep, item.Trials, now); err != nil {
		return err
	}
	return sealSweep(tx, sweep.ID, now)
}

// prepareImportedSweep creates missing sweeps and checks existing definitions.
func prepareImportedSweep(tx *Tx, item SweepImport, now time.Time) (Sweep, error) {
	sweep, err := tx.Sweeps.Get(item.Spec.ID)
	switch {
	case errors.Is(err, ErrNotFound):
		sweep = Sweep{
			SweepSpec: item.Spec,
			State:     SweepPending,
			CreatedAt: now,
			UpdatedAt: now,
		}
		if err := tx.Sweeps.Put(sweep.ID, sweep); err != nil {
			return Sweep{}, err
		}
		return sweep, nil
	case err != nil:
		return Sweep{}, err
	default:
		if err := sweep.validateImport(item); err != nil {
			return Sweep{}, err
		}
		if err := checkImportedTrials(tx, sweep, item.Trials); err != nil {
			return Sweep{}, err
		}
		return sweep, nil
	}
}

// checkImportedTrials ensures imports cannot replace or remove stored trials.
func checkImportedTrials(tx *Tx, sweep Sweep, specs []TrialSpec) error {
	found := uint64(0)
	for _, spec := range specs {
		if err := tx.Context().Err(); err != nil {
			return err
		}
		trial, err := tx.Trials.Get(TrialKey{SweepID: sweep.ID, TrialID: spec.ID})
		switch {
		case errors.Is(err, ErrNotFound):
			if sweep.Sealed {
				return fmt.Errorf("trial %q added: %w", spec.ID, ErrConflict)
			}
		case err != nil:
			return err
		default:
			if err := trial.validateImport(spec); err != nil {
				return err
			}
			found++
		}
	}
	if found != sweep.TrialCount {
		return fmt.Errorf("stored trials missing from import: %w", ErrConflict)
	}
	return nil
}
