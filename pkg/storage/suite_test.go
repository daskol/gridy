package storage_test

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/daskol/gridy/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Suite owns a queue test store and clock.
type Suite struct {
	*testing.T

	store *storage.Store
	mu    sync.Mutex
	now   time.Time
}

func NewSuite(t *testing.T, maxAttempts, trialCount int) *Suite {
	t.Helper()
	s := &Suite{T: t, now: time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)}
	s.init(maxAttempts, trialCount)
	return s
}

func (s *Suite) init(maxAttempts, trialCount int) {
	storeOpts := &storage.Options{Now: s.Now}
	store, err := storage.Open(filepath.Join(s.TempDir(), "queue.db"), storeOpts)
	require.NoError(s, err)
	s.store = store
	s.Cleanup(func() { require.NoError(s, store.Close()) })

	ctx := s.Context()
	_, err = store.CreateSweep(ctx, storage.SweepSpec{
		ID:            "sweep",
		LeaseDuration: time.Minute,
		MaxAttempts:   maxAttempts,
	})
	require.NoError(s, err)

	trials := make([]storage.TrialSpec, trialCount)
	for index := range trials {
		trials[index] = storage.TrialSpec{
			ID:     fmt.Sprint(index),
			Params: json.RawMessage(`{}`),
		}
	}
	require.NoError(s, store.AddTrials(ctx, "sweep", trials))
	require.NoError(s, store.SealSweep(ctx, "sweep"))
}

// Now returns the test clock time.
func (s *Suite) Now() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.now
}

// Advance moves the test clock forward.
func (s *Suite) Advance(delta time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.now = s.now.Add(delta)
}

// SetTime sets the test clock time.
func (s *Suite) SetTime(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.now = now
}

// Acquire requires an assignment for the request.
func (s *Suite) Acquire(requestID string) storage.Assignment {
	s.Helper()
	outcome, err := s.store.Acquire(s.Context(), storage.AcquireParams{
		SweepID: "sweep", WorkerID: "worker", RequestID: requestID,
	})
	require.NoError(s, err)
	require.Equal(s, storage.AcquisitionAssigned, outcome.State)
	require.NotNil(s, outcome.Assignment)
	return *outcome.Assignment
}
