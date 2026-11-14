// Package testsuite checks Storage implementations.
package testsuite

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/daskol/gridy/pkg/storage"
	"github.com/stretchr/testify/require"
)

type NowFn func() time.Time

// Factory opens a fresh empty store with the supplied clock.
type Factory func(t *testing.T, now NowFn) (storage.Storage, error)

type suite struct {
	*testing.T

	s     storage.Storage
	clock *clock
}

// Run checks each scenario with its own store and closes it afterward.
func Run(t *testing.T, factory Factory) {
	t.Helper()
	require.NotNil(t, factory)
	for method := range reflect.TypeFor[*suite]().Methods() {
		if !strings.HasPrefix(method.Name, "Test") {
			continue
		}

		if method.Type.NumIn() != 1 || method.Type.NumOut() != 0 {
			t.Fatalf("%s must take no arguments and return no values", method.Name)
		}

		t.Run(strings.TrimPrefix(method.Name, "Test"), func(t *testing.T) {
			t.Parallel()

			clock := &clock{now: time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)}
			store, err := factory(t, clock.Now)
			require.NoError(t, err)
			require.NotNil(t, store)
			t.Cleanup(func() { require.NoError(t, store.Close()) })

			test := &suite{T: t, s: store, clock: clock}
			reflect.ValueOf(test).MethodByName(method.Name).Call(nil)
		})
	}
}

type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Advance(delta time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(delta)
}

func (s *suite) prepareSweep(id string, maxAttempts int, trials ...string) {
	s.Helper()

	_, err := s.s.CreateSweep(s.Context(), storage.SweepSpec{
		ID: id, LeaseDuration: 10 * time.Second, MaxAttempts: maxAttempts,
	})
	require.NoError(s, err)

	specs := make([]storage.TrialSpec, len(trials))
	for index, trial := range trials {
		specs[index] = storage.TrialSpec{
			ID: trial, Params: json.RawMessage(fmt.Sprintf(`{"x":%d}`, index)),
		}
	}

	err = s.s.AddTrials(s.Context(), id, specs)
	require.NoError(s, err)
}

func (s *suite) acquire(sweep, worker, request string) storage.Assignment {
	s.Helper()

	result, err := s.s.Acquire(s.Context(), storage.AcquireParams{
		SweepID: sweep, WorkerID: worker, RequestID: request,
	})
	require.NoError(s, err)
	require.Equal(s, storage.AcquisitionAssigned, result.State)
	require.NotNil(s, result.Assignment)
	return *result.Assignment
}
