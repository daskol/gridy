// Package storage persists sweeps and their execution history.
package storage

import (
	"context"
	"encoding/json"
	"time"
)

// Storage manages sweeps and atomic trial execution.
type Storage interface {
	// CreateSweep stores a new sweep.
	CreateSweep(ctx context.Context, spec SweepSpec) (Sweep, error)

	// ImportSweeps imports sweeps.
	ImportSweeps(ctx context.Context, imports []SweepImport) error

	// AddTrials adds a batch and accepts identical retries.
	AddTrials(ctx context.Context, sweepID string, specs []TrialSpec) error

	// SealSweep marks trial generation as complete.
	SealSweep(ctx context.Context, sweepID string) error

	// GetSweep returns a sweep.
	GetSweep(ctx context.Context, id string) (Sweep, error)

	// ListSweeps returns a page of sweeps.
	ListSweeps(ctx context.Context, page PageRequest) (Page[Sweep], error)

	// SearchSweeps matches identifiers and codes without case sensitivity.
	SearchSweeps(ctx context.Context, query string, page PageRequest) (Page[Sweep], error)

	// GetWorker returns worker contact times.
	GetWorker(ctx context.Context, id string) (Worker, error)

	// ListWorkers returns a page of workers.
	ListWorkers(ctx context.Context, page PageRequest) (Page[Worker], error)

	// GetTrial returns a trial within a sweep.
	GetTrial(ctx context.Context, key TrialKey) (Trial, error)

	// ListTrials returns a page of trials.
	ListTrials(ctx context.Context, sweepID string, page PageRequest) (Page[Trial], error)

	// GetAttempt returns one execution of a trial.
	GetAttempt(ctx context.Context, key AttemptKey) (Attempt, error)

	// ListAttempts returns attempts in execution order.
	ListAttempts(ctx context.Context, key TrialKey, page PageRequest) (Page[Attempt], error)

	// Acquire returns an assignment, waiting, or completion.
	Acquire(ctx context.Context, params AcquireParams) (Acquisition, error)

	// RenewLease extends an active lease.
	RenewLease(ctx context.Context, key AttemptKey) (time.Time, error)

	// Complete stores a result and accepts identical retries.
	Complete(ctx context.Context, key AttemptKey, result json.RawMessage) error

	// Fail records an error and applies the retry policy.
	Fail(ctx context.Context, key AttemptKey, message string) error

	// ExpireLeases processes at most limit expired attempts.
	ExpireLeases(ctx context.Context, limit int) (int, error)

	// Close releases storage resources.
	Close() error
}
