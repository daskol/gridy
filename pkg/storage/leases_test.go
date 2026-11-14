package storage_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/daskol/gridy/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestQueueExpiryBatchesDoNotSkipAttempts(t *testing.T) {
	t.Parallel()

	s := NewSuite(t, 1, 5)
	ctx := s.Context()

	for index := range 5 {
		s.Acquire(fmt.Sprint(index))
	}
	s.Advance(time.Minute)
	for index, want := range []int{2, 2, 1, 0} {
		got, err := s.store.ExpireLeases(ctx, 2)
		require.NoError(s, err)
		require.Equal(s, want, got, "expiry batch %d", index)
	}

	sweep, err := s.store.GetSweep(ctx, "sweep")
	require.NoError(s, err)
	require.Equal(s, storage.SweepFailed, sweep.State)
	require.EqualValues(s, 5, sweep.TerminalCount)
	require.EqualValues(s, 5, sweep.FailedCount)
	for index := range 5 {
		attempt, err := s.store.GetAttempt(ctx, storage.AttemptKey{
			SweepID:   "sweep",
			TrialID:   fmt.Sprint(index),
			AttemptID: 1,
		})
		require.NoError(s, err)
		require.Equal(s, storage.AttemptExpired, attempt.State, "ix: %d", index)
	}

	outcome, err := s.store.Acquire(ctx, storage.AcquireParams{
		SweepID:   "sweep",
		WorkerID:  "worker",
		RequestID: "done",
	})
	require.NoError(s, err)
	require.Equal(s, storage.AcquisitionDone, outcome.State)
}

func TestQueueRenewalReplacesLeaseIndex(t *testing.T) {
	t.Parallel()

	s := NewSuite(t, 2, 1)
	ctx := s.Context()

	original := s.Acquire("first")
	s.Advance(30 * time.Second)
	deadline, err := s.store.RenewLease(ctx, original.AttemptKey)
	require.NoError(s, err)
	require.Equal(s, s.Now().Add(time.Minute), deadline)

	repeated := s.Acquire("first")
	require.Equal(s, original.LeaseExpiresAt, repeated.LeaseExpiresAt)

	s.SetTime(original.LeaseExpiresAt)
	count, err := s.store.ExpireLeases(ctx, 10)
	require.NoError(s, err)
	require.Zero(s, count, "expiry at old deadline")

	s.SetTime(deadline)
	count, err = s.store.ExpireLeases(ctx, 10)
	require.NoError(s, err)
	require.Equal(s, 1, count, "expiry at renewed deadline")

	replacement := s.Acquire("replacement")
	require.EqualValues(s, 2, replacement.AttemptID)

	_, err = s.store.RenewLease(ctx, original.AttemptKey)
	require.ErrorIs(s, err, storage.ErrStaleAttempt)
}

func TestQueueAcquirePreservesExpiryCounters(t *testing.T) {
	t.Parallel()

	s := NewSuite(t, 1, 2)
	ctx := s.Context()

	s.Acquire("first")
	s.Advance(time.Minute)
	next := s.Acquire("second")
	require.Equal(s, "1", next.TrialID)

	sweep, err := s.store.GetSweep(ctx, "sweep")
	require.NoError(s, err)
	require.EqualValues(s, 1, sweep.TerminalCount)
	require.EqualValues(s, 1, sweep.FailedCount)
	require.Equal(s, storage.SweepRunning, sweep.State)

	err = s.store.Complete(ctx, next.AttemptKey, json.RawMessage(`{"loss": 0}`))
	require.NoError(s, err)

	sweep, err = s.store.GetSweep(ctx, "sweep")
	require.NoError(s, err)
	require.EqualValues(s, 2, sweep.TerminalCount)
	require.EqualValues(s, 1, sweep.FailedCount)
	require.Equal(s, storage.SweepFailed, sweep.State)
}
