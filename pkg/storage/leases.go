package storage

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const acquireExpiryLimit = 100

// Acquire returns an assignment or the current work availability.
func (s *Store) Acquire(ctx context.Context, params AcquireParams) (Acquisition, error) {
	if err := params.Validate(); err != nil {
		return Acquisition{}, err
	}

	res := Acquisition{}
	err := s.update(ctx, func(tx *Tx, now time.Time) error {
		if _, err := tx.Sweeps.Get(params.SweepID); err != nil {
			return err
		}

		if err := TouchWorker(tx, params.WorkerID, now); err != nil {
			return err
		}

		assignment, err := tx.Requests.Get(params)
		switch {
		case err == nil:
			res.State = AcquisitionAssigned
			res.Assignment = &assignment
			return nil
		case !errors.Is(err, ErrNotFound):
			return err
		}
		if _, err := expireLeases(tx, now, acquireExpiryLimit); err != nil {
			return err
		}

		sweep, err := tx.Sweeps.Get(params.SweepID)
		if err != nil {
			return err
		}
		if err := sweep.validateExecution(); err != nil {
			return err
		}

		trial, err := tx.Ready.Next(params.SweepID)
		switch {
		case err != nil:
			return err
		case trial != nil:
			// Nothing.
		case sweep.Sealed && sweep.TerminalCount == sweep.TrialCount:
			res.State = AcquisitionDone
			return nil
		default:
			res.State = AcquisitionWait
			return nil
		}

		assignment, err = assignTrial(tx, *trial, sweep, params.WorkerID, now)
		if err != nil {
			return err
		}
		res = Acquisition{State: AcquisitionAssigned, Assignment: &assignment}
		return tx.Requests.Put(params, assignment)
	})
	if err != nil {
		return Acquisition{}, err
	}
	return res, nil
}

func assignTrial(
	tx *Tx, trial Trial, sweep Sweep, workerID string, now time.Time,
) (Assignment, error) {
	if err := trial.validateAssignment(sweep); err != nil {
		return Assignment{}, err
	}

	trial.AttemptID++
	trial.State = TrialRunning
	trial.Result = nil
	trial.Error = ""
	trial.UpdatedAt = now

	attemptID := AttemptKey{
		TrialKey:  trial.TrialKey,
		AttemptID: trial.AttemptID,
	}

	attempt := Attempt{
		AttemptKey:     attemptID,
		WorkerID:       workerID,
		State:          AttemptRunning,
		StartedAt:      now,
		LeaseExpiresAt: now.Add(sweep.LeaseDuration),
	}

	sweep.State = SweepRunning
	updateSweepState(&sweep, now)

	if err := tx.Ready.Delete(trial.TrialKey); err != nil {
		return Assignment{}, err
	}
	if err := tx.Leases.Put(attempt); err != nil {
		return Assignment{}, err
	}
	if err := saveExecution(tx, trial, attempt, sweep); err != nil {
		return Assignment{}, err
	}
	return Assignment{
		AttemptKey:     attempt.AttemptKey,
		Params:         trial.Params,
		LeaseExpiresAt: attempt.LeaseExpiresAt,
	}, nil
}

// RenewLease extends a live attempt without shortening its lease.
func (s *Store) RenewLease(ctx context.Context, key AttemptKey) (time.Time, error) {
	if err := key.Validate(); err != nil {
		return time.Time{}, err
	}
	deadline := time.Time{}
	err := s.update(ctx, func(tx *Tx, now time.Time) error {
		trial, attempt, err := currentAttempt(tx, key)
		if err != nil {
			return err
		}
		if err := attempt.validateLive(trial, now); err != nil {
			return err
		}
		sweep, err := tx.Sweeps.Get(key.SweepID)
		if err != nil {
			return err
		}
		deadline, err = sweep.leaseDeadline(now)
		if err != nil {
			return err
		}
		if err := tx.Leases.Delete(attempt); err != nil {
			return err
		}
		if deadline.Before(attempt.LeaseExpiresAt) {
			deadline = attempt.LeaseExpiresAt
		}
		attempt.LeaseExpiresAt = deadline
		trial.UpdatedAt = now
		if err := tx.Attempts.Put(key, attempt); err != nil {
			return err
		}
		if err := tx.Trials.Put(key.TrialKey, trial); err != nil {
			return err
		}
		if err := tx.Leases.Put(attempt); err != nil {
			return err
		}
		return TouchWorker(tx, attempt.WorkerID, now)
	})
	if err != nil {
		return time.Time{}, err
	}
	return deadline, nil
}

// ExpireLeases processes at most limit expired attempts.
func (s *Store) ExpireLeases(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, fmt.Errorf("expiry limit: %w", ErrInvalid)
	}
	count := 0
	err := s.update(ctx, func(tx *Tx, now time.Time) error {
		var err error
		count, err = expireLeases(tx, now, limit)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func expireLeases(tx *Tx, now time.Time, limit int) (int, error) {
	expired, err := tx.Leases.Expired(now, limit)
	if err != nil {
		return 0, err
	}
	for _, attempt := range expired {
		trial, current, err := currentAttempt(tx, attempt.AttemptKey)
		if err != nil {
			return 0, err
		}
		if err := current.validateExpiry(trial); err != nil {
			return 0, err
		}
		sweep, err := tx.Sweeps.Get(attempt.SweepID)
		if err != nil {
			return 0, err
		}
		attempt.State = AttemptExpired
		attempt.Error = "lease expired"
		attempt.FinishedAt = now
		if err := finishTrial(tx, trial, attempt, sweep, now); err != nil {
			return 0, err
		}
	}
	return len(expired), nil
}
