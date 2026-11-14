package storage

import (
	"context"
	"encoding/json"
	"time"
)

// Complete stores a successful result and accepts identical retries.
func (s *Store) Complete(ctx context.Context, key AttemptKey, result json.RawMessage) error {
	update := Attempt{
		AttemptKey: key,
		State:      AttemptCompleted,
		Result:     result,
	}
	update, err := update.NormalizeResult()
	if err != nil {
		return err
	}
	return s.finish(ctx, update)
}

// Fail records an error and retries within the sweep policy.
func (s *Store) Fail(ctx context.Context, key AttemptKey, message string) error {
	update := Attempt{
		AttemptKey: key,
		State:      AttemptFailed,
		Error:      message,
	}
	update, err := update.NormalizeResult()
	if err != nil {
		return err
	}
	return s.finish(ctx, update)
}

// GetAttempt returns one execution of a trial.
func (s *Store) GetAttempt(ctx context.Context, key AttemptKey) (Attempt, error) {
	if err := key.Validate(); err != nil {
		return Attempt{}, err
	}
	return readRecord(ctx, s, func(tx *Tx) (Attempt, error) {
		return tx.Attempts.Get(key)
	})
}

// ListAttempts returns attempts in increasing attempt order.
func (s *Store) ListAttempts(ctx context.Context, key TrialKey, page PageRequest) (Page[Attempt], error) {
	if err := key.Validate(); err != nil {
		return Page[Attempt]{}, err
	}
	prefix := key.Bytes()
	scope := tuple("attempts", key.SweepID, key.TrialID)
	return readPage[Attempt](ctx, s, prefix, scope, page, nil, func(tx *Tx) error {
		_, err := tx.Trials.Get(key)
		return err
	})
}

func (s *Store) finish(ctx context.Context, result Attempt) error {
	return s.update(ctx, func(tx *Tx, now time.Time) error {
		trial, attempt, err := currentAttempt(tx, result.AttemptKey)
		switch {
		case err != nil:
			return err
		case attempt.State == result.State:
			if !attempt.matchesResult(result) {
				return ErrConflict
			}
			return TouchWorker(tx, attempt.WorkerID, now)
		}

		if err := attempt.validateLive(trial, now); err != nil {
			return err
		}

		sweep, err := tx.Sweeps.Get(result.SweepID)
		if err != nil {
			return err
		}

		attempt.State = result.State
		attempt.Result = result.Result
		attempt.Error = result.Error
		attempt.FinishedAt = now

		if err := finishTrial(tx, trial, attempt, sweep, now); err != nil {
			return err
		}
		return TouchWorker(tx, attempt.WorkerID, now)
	})
}

func currentAttempt(tx *Tx, key AttemptKey) (Trial, Attempt, error) {
	trial, err := tx.Trials.Get(key.TrialKey)
	if err != nil {
		return Trial{}, Attempt{}, err
	}

	if err := trial.validateAttempt(key.AttemptID); err != nil {
		return Trial{}, Attempt{}, err
	}

	attempt, err := tx.Attempts.Get(key)
	return trial, attempt, err
}

func finishTrial(
	tx *Tx, trial Trial, attempt Attempt, sweep Sweep, now time.Time,
) error {
	err := trial.finish(attempt, &sweep, now)
	switch {
	case err != nil:
		return err
	case trial.State == TrialPending:
		if err := tx.Ready.Put(trial.TrialKey); err != nil {
			return err
		}
	}

	updateSweepState(&sweep, now)
	if err := tx.Leases.Delete(attempt); err != nil {
		return err
	}
	return saveExecution(tx, trial, attempt, sweep)
}

func saveExecution(tx *Tx, trial Trial, attempt Attempt, sweep Sweep) error {
	if err := tx.Trials.Put(trial.TrialKey, trial); err != nil {
		return err
	}
	if err := tx.Attempts.Put(attempt.AttemptKey, attempt); err != nil {
		return err
	}
	return tx.Sweeps.Put(sweep.ID, sweep)
}
