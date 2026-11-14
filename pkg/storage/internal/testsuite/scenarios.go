package testsuite

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/daskol/gridy/pkg/storage"
	"github.com/stretchr/testify/require"
)

func (s *suite) TestAcquireComplete() {
	s.prepareSweep("sweep", 2, "trial")
	err := s.s.SealSweep(s.Context(), "sweep")
	require.NoError(s, err)

	assignment := s.acquire("sweep", "worker", "request")
	result := json.RawMessage(`{"loss":1}`)
	err = s.s.Complete(s.Context(), assignment.AttemptKey, result)
	require.NoError(s, err)

	trial, err := s.s.GetTrial(s.Context(), assignment.TrialKey)
	require.NoError(s, err)
	require.Equal(s, storage.TrialCompleted, trial.State)
	require.Equal(s, assignment.AttemptID, trial.AttemptID)
	require.JSONEq(s, string(assignment.Params), string(trial.Params))
	require.JSONEq(s, string(result), string(trial.Result))
	require.Empty(s, trial.Error)

	history := s.checkHistory(assignment.TrialKey, storage.AttemptCompleted)
	require.Equal(s, "worker", history[0].WorkerID)
	require.JSONEq(s, string(result), string(history[0].Result))
	s.checkSweep("sweep", storage.SweepCompleted, 1, 1, 0)
	s.checkDone("sweep")
}

func (s *suite) TestAcquireLeaseComplete() {
	s.prepareSweep("sweep", 2, "trial")
	err := s.s.SealSweep(s.Context(), "sweep")
	require.NoError(s, err)

	assignment := s.acquire("sweep", "worker", "request")
	s.clock.Advance(6 * time.Second)

	deadline, err := s.s.RenewLease(s.Context(), assignment.AttemptKey)
	require.NoError(s, err)
	require.WithinDuration(s, s.clock.Now().Add(10*time.Second), deadline, 0)
	require.True(s, deadline.After(assignment.LeaseExpiresAt))

	s.clock.Advance(4 * time.Second)
	count, err := s.s.ExpireLeases(s.Context(), 10)
	require.NoError(s, err)
	require.Zero(s, count)

	attempt, err := s.s.GetAttempt(s.Context(), assignment.AttemptKey)
	require.NoError(s, err)
	require.Equal(s, storage.AttemptRunning, attempt.State)
	require.WithinDuration(s, deadline, attempt.LeaseExpiresAt, 0)

	result := json.RawMessage(`{"loss":0.5}`)
	err = s.s.Complete(s.Context(), assignment.AttemptKey, result)
	require.NoError(s, err)

	s.clock.Advance(6 * time.Second)
	count, err = s.s.ExpireLeases(s.Context(), 10)
	require.NoError(s, err)
	require.Zero(s, count)

	trial, err := s.s.GetTrial(s.Context(), assignment.TrialKey)
	require.NoError(s, err)
	require.Equal(s, storage.TrialCompleted, trial.State)
	require.JSONEq(s, string(result), string(trial.Result))

	history := s.checkHistory(assignment.TrialKey, storage.AttemptCompleted)
	require.WithinDuration(s, deadline, history[0].LeaseExpiresAt, 0)
	require.JSONEq(s, string(result), string(history[0].Result))
	s.checkSweep("sweep", storage.SweepCompleted, 1, 1, 0)
	s.checkDone("sweep")
}

func (s *suite) TestAcquireExpire() {
	s.prepareSweep("sweep", 2, "trial")
	err := s.s.SealSweep(s.Context(), "sweep")
	require.NoError(s, err)

	first := s.acquire("sweep", "lost-worker", "first")
	s.clock.Advance(10 * time.Second)
	err = s.s.Complete(s.Context(), first.AttemptKey, json.RawMessage(`{}`))
	require.ErrorIs(s, err, storage.ErrLeaseExpired)
	err = s.s.Fail(s.Context(), first.AttemptKey, "late failure")
	require.ErrorIs(s, err, storage.ErrLeaseExpired)

	_, err = s.s.RenewLease(s.Context(), first.AttemptKey)
	require.ErrorIs(s, err, storage.ErrLeaseExpired)

	count, err := s.s.ExpireLeases(s.Context(), 10)
	require.NoError(s, err)
	require.Equal(s, 1, count)

	history := s.checkHistory(first.TrialKey, storage.AttemptExpired)
	require.Equal(s, "lost-worker", history[0].WorkerID)

	trial, err := s.s.GetTrial(s.Context(), first.TrialKey)
	require.NoError(s, err)
	require.Equal(s, storage.TrialPending, trial.State)
	s.checkSweep("sweep", storage.SweepRunning, 1, 0, 0)

	replacement := s.acquire("sweep", "replacement-worker", "second")
	require.Equal(s, first.TrialKey, replacement.TrialKey)
	require.Equal(s, first.AttemptID+1, replacement.AttemptID)
	require.JSONEq(s, string(first.Params), string(replacement.Params))

	err = s.s.Complete(s.Context(), first.AttemptKey, json.RawMessage(`{}`))
	require.ErrorIs(s, err, storage.ErrStaleAttempt)

	err = s.s.Fail(s.Context(), first.AttemptKey, "late failure")
	require.ErrorIs(s, err, storage.ErrStaleAttempt)

	_, err = s.s.RenewLease(s.Context(), first.AttemptKey)
	require.ErrorIs(s, err, storage.ErrStaleAttempt)

	result := json.RawMessage(`{"loss":0.25}`)
	err = s.s.Complete(s.Context(), replacement.AttemptKey, result)
	require.NoError(s, err)

	err = s.s.Complete(s.Context(), first.AttemptKey, json.RawMessage(`{}`))
	require.ErrorIs(s, err, storage.ErrStaleAttempt)

	trial, err = s.s.GetTrial(s.Context(), replacement.TrialKey)
	require.NoError(s, err)
	require.Equal(s, storage.TrialCompleted, trial.State)
	require.Equal(s, replacement.AttemptID, trial.AttemptID)
	require.JSONEq(s, string(result), string(trial.Result))

	history = s.checkHistory(first.TrialKey, storage.AttemptExpired, storage.AttemptCompleted)
	require.Equal(s, "replacement-worker", history[1].WorkerID)
	require.Empty(s, history[0].Result)
	require.JSONEq(s, string(result), string(history[1].Result))
	s.checkSweep("sweep", storage.SweepCompleted, 1, 1, 0)
	s.checkDone("sweep")
}

func (s *suite) TestAcquireReclaimsExpired() {
	s.prepareSweep("sweep", 2, "trial")

	err := s.s.SealSweep(s.Context(), "sweep")
	require.NoError(s, err)

	first := s.acquire("sweep", "lost-worker", "first")
	s.clock.Advance(10 * time.Second)
	require.WithinDuration(s, first.LeaseExpiresAt, s.clock.Now(), 0)

	replacement := s.acquire("sweep", "replacement-worker", "second")
	require.Equal(s, first.TrialKey, replacement.TrialKey)
	require.Equal(s, first.AttemptID+1, replacement.AttemptID)
	require.JSONEq(s, string(first.Params), string(replacement.Params))

	err = s.s.Complete(s.Context(), first.AttemptKey, json.RawMessage(`{}`))
	require.ErrorIs(s, err, storage.ErrStaleAttempt)

	history := s.checkHistory(first.TrialKey, storage.AttemptExpired, storage.AttemptRunning)
	require.Equal(s, "lost-worker", history[0].WorkerID)
	require.Equal(s, "replacement-worker", history[1].WorkerID)
	s.checkSweep("sweep", storage.SweepRunning, 1, 0, 0)

	result := json.RawMessage(`{"loss":0.25}`)
	err = s.s.Complete(s.Context(), replacement.AttemptKey, result)
	require.NoError(s, err)

	trial, err := s.s.GetTrial(s.Context(), replacement.TrialKey)
	require.NoError(s, err)
	require.Equal(s, storage.TrialCompleted, trial.State)
	require.Equal(s, replacement.AttemptID, trial.AttemptID)
	require.JSONEq(s, string(result), string(trial.Result))

	history = s.checkHistory(first.TrialKey, storage.AttemptExpired, storage.AttemptCompleted)
	require.Empty(s, history[0].Result)
	require.JSONEq(s, string(result), string(history[1].Result))
	s.checkSweep("sweep", storage.SweepCompleted, 1, 1, 0)
	s.checkDone("sweep")
}

func (s *suite) TestFailureRetries() {
	s.prepareSweep("recover", 2, "trial")

	err := s.s.SealSweep(s.Context(), "recover")
	require.NoError(s, err)

	first := s.acquire("recover", "first-worker", "first")
	err = s.s.Fail(s.Context(), first.AttemptKey, "objective crashed")
	require.NoError(s, err)

	s.clock.Advance(20 * time.Second)
	err = s.s.Fail(s.Context(), first.AttemptKey, "objective crashed")
	require.NoError(s, err)

	err = s.s.Fail(s.Context(), first.AttemptKey, "different error")
	require.ErrorIs(s, err, storage.ErrConflict)

	err = s.s.Complete(s.Context(), first.AttemptKey, json.RawMessage(`{}`))
	require.ErrorIs(s, err, storage.ErrConflict)

	s.checkSweep("recover", storage.SweepRunning, 1, 0, 0)

	second := s.acquire("recover", "second-worker", "second")
	require.Equal(s, first.TrialKey, second.TrialKey)
	require.Equal(s, first.AttemptID+1, second.AttemptID)

	err = s.s.Fail(s.Context(), first.AttemptKey, "objective crashed")
	require.ErrorIs(s, err, storage.ErrStaleAttempt)

	result := json.RawMessage(`{"loss":0}`)
	err = s.s.Complete(s.Context(), second.AttemptKey, result)
	require.NoError(s, err)

	trial, err := s.s.GetTrial(s.Context(), first.TrialKey)
	require.NoError(s, err)
	require.Equal(s, storage.TrialCompleted, trial.State)
	require.Empty(s, trial.Error)
	require.JSONEq(s, string(result), string(trial.Result))

	history := s.checkHistory(first.TrialKey, storage.AttemptFailed, storage.AttemptCompleted)
	require.Equal(s, "first-worker", history[0].WorkerID)
	require.Equal(s, "objective crashed", history[0].Error)
	require.Equal(s, "second-worker", history[1].WorkerID)
	s.checkSweep("recover", storage.SweepCompleted, 1, 1, 0)
	s.checkDone("recover")

	s.prepareSweep("exhaust", 2, "trial")
	err = s.s.SealSweep(s.Context(), "exhaust")
	require.NoError(s, err)

	first = s.acquire("exhaust", "first-worker", "first")
	err = s.s.Fail(s.Context(), first.AttemptKey, "first failure")
	require.NoError(s, err)

	second = s.acquire("exhaust", "second-worker", "second")
	err = s.s.Fail(s.Context(), second.AttemptKey, "second failure")
	require.NoError(s, err)

	err = s.s.Fail(s.Context(), first.AttemptKey, "first failure")
	require.ErrorIs(s, err, storage.ErrStaleAttempt)

	s.clock.Advance(20 * time.Second)
	err = s.s.Fail(s.Context(), second.AttemptKey, "second failure")
	require.NoError(s, err)

	trial, err = s.s.GetTrial(s.Context(), first.TrialKey)
	require.NoError(s, err)
	require.Equal(s, storage.TrialFailed, trial.State)
	require.Equal(s, second.AttemptID, trial.AttemptID)
	require.Equal(s, "second failure", trial.Error)
	require.Empty(s, trial.Result)

	history = s.checkHistory(first.TrialKey, storage.AttemptFailed, storage.AttemptFailed)
	require.Equal(s, "first failure", history[0].Error)
	require.Equal(s, "second failure", history[1].Error)

	s.checkSweep("exhaust", storage.SweepFailed, 1, 1, 1)
	s.checkDone("exhaust")
}

func (s *suite) TestRequestRetries() {
	s.prepareSweep("sweep", 2, "trial")

	err := s.s.SealSweep(s.Context(), "sweep")
	require.NoError(s, err)

	first := s.acquire("sweep", "worker", "request")
	second := s.acquire("sweep", "worker", "request")
	s.checkAssignment(first, second)
	s.clock.Advance(time.Second)

	deadline, err := s.s.RenewLease(s.Context(), first.AttemptKey)
	require.NoError(s, err)
	require.True(s, deadline.After(first.LeaseExpiresAt))
	s.checkAssignment(first, s.acquire("sweep", "worker", "request"))

	result := json.RawMessage(`{"loss":1,"nested":{"b":[true,null,1e2],"a":2}}`)
	err = s.s.Complete(s.Context(), first.AttemptKey, result)
	require.NoError(s, err)

	s.checkAssignment(first, s.acquire("sweep", "worker", "request"))
	s.clock.Advance(time.Hour)
	equivalent := json.RawMessage(`{"nested":{"a":2.0,"b":[true,null,100]},"loss":1.0}`)
	err = s.s.Complete(s.Context(), first.AttemptKey, equivalent)
	require.NoError(s, err)

	err = s.s.Complete(s.Context(), first.AttemptKey, json.RawMessage(`{"loss":2}`))
	require.ErrorIs(s, err, storage.ErrConflict)

	err = s.s.Fail(s.Context(), first.AttemptKey, "failed")
	require.ErrorIs(s, err, storage.ErrConflict)

	second = s.acquire("sweep", "worker", "request")
	s.checkAssignment(first, second)

	trial, err := s.s.GetTrial(s.Context(), first.TrialKey)
	require.NoError(s, err)
	require.Equal(s, storage.TrialCompleted, trial.State)
	require.JSONEq(s, string(result), string(trial.Result))

	history := s.checkHistory(first.TrialKey, storage.AttemptCompleted)
	require.JSONEq(s, string(result), string(history[0].Result))
	require.WithinDuration(s, deadline, history[0].LeaseExpiresAt, 0)

	s.checkSweep("sweep", storage.SweepCompleted, 1, 1, 0)
	s.checkDone("sweep")
}

func (s *suite) TestConcurrentWorkers() {
	const trialCount = 8
	const workerCount = 2 * trialCount

	ids := make([]string, trialCount)
	for i := range ids {
		ids[i] = fmt.Sprintf("trial-%02d", i)
	}

	s.prepareSweep("sweep", 2, ids...)
	err := s.s.SealSweep(s.Context(), "sweep")
	require.NoError(s, err)

	type outcome struct {
		worker      string
		acquisition storage.Acquisition
		err         error
	}

	outcomes := make(chan outcome, workerCount)
	workers := sync.WaitGroup{}
	for i := range workerCount {
		workers.Go(func() {
			worker := fmt.Sprintf("worker-%02d", i)
			acquisition, err := s.s.Acquire(s.Context(), storage.AcquireParams{
				SweepID: "sweep", WorkerID: worker, RequestID: "request",
			})
			outcomes <- outcome{worker: worker, acquisition: acquisition, err: err}
		})
	}
	workers.Wait()
	close(outcomes)

	assignments := make(map[storage.TrialKey]storage.Assignment, trialCount)
	owners := make(map[storage.TrialKey]string, trialCount)
	waits := 0
	for result := range outcomes {
		require.NoError(s, result.err)
		switch result.acquisition.State {
		case storage.AcquisitionAssigned:
			require.NotNil(s, result.acquisition.Assignment)

			assignment := *result.acquisition.Assignment
			require.NotContains(s, assignments, assignment.TrialKey)
			require.EqualValues(s, 1, assignment.AttemptID)

			assignments[assignment.TrialKey] = assignment
			owners[assignment.TrialKey] = result.worker
		case storage.AcquisitionWait:
			require.Nil(s, result.acquisition.Assignment)
			waits++
		default:
			s.Fatalf("unexpected acquisition state %q", result.acquisition.State)
		}
	}
	require.Len(s, assignments, trialCount)
	require.Equal(s, workerCount-trialCount, waits)

	failures := make(chan error, trialCount)
	for _, assignment := range assignments {
		workers.Go(func() {
			msg := json.RawMessage(`{"loss":0}`)
			failures <- s.s.Complete(s.Context(), assignment.AttemptKey, msg)
		})
	}
	workers.Wait()

	close(failures)
	for err := range failures {
		require.NoError(s, err)
	}

	for key := range assignments {
		trial, err := s.s.GetTrial(s.Context(), key)
		require.NoError(s, err)
		require.Equal(s, storage.TrialCompleted, trial.State)

		history := s.checkHistory(key, storage.AttemptCompleted)
		require.Equal(s, owners[key], history[0].WorkerID)
	}

	s.checkSweep("sweep", storage.SweepCompleted, trialCount, trialCount, 0)
	s.checkDone("sweep")
}

func (s *suite) TestSweepGeneration() {
	s.prepareSweep("sweep", 2)

	wait, err := s.s.Acquire(s.Context(), storage.AcquireParams{
		SweepID: "sweep", WorkerID: "observer", RequestID: "before-generation",
	})
	require.NoError(s, err)
	require.Equal(s, storage.AcquisitionWait, wait.State)
	require.Nil(s, wait.Assignment)

	err = s.s.AddTrials(s.Context(), "sweep", []storage.TrialSpec{
		{ID: "a"}, {ID: "b"},
	})
	require.NoError(s, err)

	started := s.clock.Now()
	first := s.acquire("sweep", "worker-a", "first")
	second := s.acquire("sweep", "worker-b", "second")
	require.NotEqual(s, first.TrialKey, second.TrialKey)

	err = s.s.SealSweep(s.Context(), "sweep")
	require.NoError(s, err)

	wait, err = s.s.Acquire(s.Context(), storage.AcquireParams{
		SweepID: "sweep", WorkerID: "observer", RequestID: "active",
	})
	require.NoError(s, err)
	require.Equal(s, storage.AcquisitionWait, wait.State)
	require.Nil(s, wait.Assignment)

	s.clock.Advance(2 * time.Second)
	firstResult := json.RawMessage(`{"loss":1}`)
	err = s.s.Complete(s.Context(), first.AttemptKey, firstResult)
	require.NoError(s, err)

	worker, err := s.s.GetWorker(s.Context(), "worker-a")
	require.NoError(s, err)
	require.WithinDuration(s, started, worker.FirstSeenAt, 0)
	require.WithinDuration(s, s.clock.Now(), worker.LastSeenAt, 0)
	s.checkSweep("sweep", storage.SweepRunning, 2, 1, 0)

	secondResult := json.RawMessage(`{"loss":2}`)
	err = s.s.Complete(s.Context(), second.AttemptKey, secondResult)
	require.NoError(s, err)

	for _, completed := range []struct {
		assignment storage.Assignment
		result     json.RawMessage
	}{
		{first, firstResult},
		{second, secondResult},
	} {
		trial, err := s.s.GetTrial(s.Context(), completed.assignment.TrialKey)
		require.NoError(s, err)
		require.Equal(s, storage.TrialCompleted, trial.State)
		require.JSONEq(s, string(completed.result), string(trial.Result))

		history := s.checkHistory(trial.TrialKey, storage.AttemptCompleted)
		require.JSONEq(s, string(completed.result), string(history[0].Result))
	}

	s.checkSweep("sweep", storage.SweepCompleted, 2, 2, 0)
	s.checkDone("sweep")
}

func (s *suite) checkAssignment(expected, actual storage.Assignment) {
	s.Helper()
	require.Equal(s, expected.AttemptKey, actual.AttemptKey)
	require.JSONEq(s, string(expected.Params), string(actual.Params))
	require.WithinDuration(s, expected.LeaseExpiresAt, actual.LeaseExpiresAt, 0)
}

func (s *suite) checkSweep(id string, state storage.SweepState, trials, terminal, failed uint64) {
	s.Helper()
	sweep, err := s.s.GetSweep(s.Context(), id)
	require.NoError(s, err)
	require.Equal(s, state, sweep.State)
	require.Equal(s, trials, sweep.TrialCount)
	require.Equal(s, terminal, sweep.TerminalCount)
	require.Equal(s, failed, sweep.FailedCount)
}

func (s *suite) checkHistory(
	key storage.TrialKey, states ...storage.AttemptState,
) []storage.Attempt {
	s.Helper()

	history, err := s.s.ListAttempts(s.Context(), key, storage.PageRequest{})
	require.NoError(s, err)
	require.Len(s, history.Items, len(states))

	for i, state := range states {
		require.Equal(s, key, history.Items[i].TrialKey)
		require.Equal(s, int64(i+1), history.Items[i].AttemptID)
		require.Equal(s, state, history.Items[i].State)
	}
	return history.Items
}

func (s *suite) checkDone(sweep string) {
	s.Helper()

	done, err := s.s.Acquire(s.Context(), storage.AcquireParams{
		SweepID: sweep, WorkerID: "observer", RequestID: "done",
	})

	require.NoError(s, err)
	require.Equal(s, storage.AcquisitionDone, done.State)
	require.Nil(s, done.Assignment)
}
