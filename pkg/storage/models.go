package storage

import (
	"encoding/json"
	"time"
)

type SweepState string

const (
	SweepPending   SweepState = "pending"
	SweepRunning   SweepState = "running"
	SweepCompleted SweepState = "completed"
	SweepFailed    SweepState = "failed"
)

type TrialState string

const (
	TrialPending   TrialState = "pending"
	TrialRunning   TrialState = "running"
	TrialCompleted TrialState = "completed"
	TrialFailed    TrialState = "failed"
)

type AttemptState string

const (
	AttemptRunning   AttemptState = "running"
	AttemptCompleted AttemptState = "completed"
	AttemptFailed    AttemptState = "failed"
	AttemptExpired   AttemptState = "expired"
)

// SweepSpec describes a sweep and its retry policy.
type SweepSpec struct {
	ID            string          `json:"sweep_id"`
	Code          string          `json:"code,omitempty"`
	Space         json.RawMessage `json:"space"`
	LeaseDuration time.Duration   `json:"lease_duration"`
	MaxAttempts   int             `json:"max_attempts"`
}

// Sweep tracks trial generation and completion.
type Sweep struct {
	SweepSpec
	State         SweepState `json:"state"`
	Sealed        bool       `json:"sealed"`
	TrialCount    uint64     `json:"trial_count"`
	TerminalCount uint64     `json:"terminal_count"`
	FailedCount   uint64     `json:"failed_count"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
}

// Worker records coordinator contact times.
type Worker struct {
	ID          string    `json:"worker_id"`
	FirstSeenAt time.Time `json:"first_seen_at"`
	LastSeenAt  time.Time `json:"last_seen_at"`
}

// TrialKey identifies a trial within a sweep.
type TrialKey struct {
	SweepID string `json:"sweep_id"`
	TrialID string `json:"trial_id"`
}

// AttemptKey identifies one execution of a trial.
type AttemptKey struct {
	TrialKey
	AttemptID int64 `json:"attempt_id"`
}

// TrialSpec describes one parameter combination.
type TrialSpec struct {
	ID     string          `json:"trial_id"`
	Params json.RawMessage `json:"params"`
}

// Trial tracks its current attempt and accepted result.
type Trial struct {
	TrialKey
	Params    json.RawMessage `json:"params"`
	State     TrialState      `json:"state"`
	AttemptID int64           `json:"attempt_id"`
	Result    json.RawMessage `json:"result,omitempty"`
	Error     string          `json:"error,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

// Attempt preserves one worker execution.
type Attempt struct {
	AttemptKey
	WorkerID       string          `json:"worker_id"`
	State          AttemptState    `json:"state"`
	StartedAt      time.Time       `json:"started_at"`
	LeaseExpiresAt time.Time       `json:"lease_expires_at"`
	FinishedAt     time.Time       `json:"finished_at"`
	Result         json.RawMessage `json:"result,omitempty"`
	Error          string          `json:"error,omitempty"`
}

// AcquireParams identifies a logical assignment request.
type AcquireParams struct {
	SweepID   string
	WorkerID  string
	RequestID string
}

type AcquisitionState string

const (
	AcquisitionAssigned AcquisitionState = "assigned"
	AcquisitionWait     AcquisitionState = "wait"
	AcquisitionDone     AcquisitionState = "done"
)

// Assignment is the original response to an acquisition.
type Assignment struct {
	AttemptKey
	Params         json.RawMessage `json:"params"`
	LeaseExpiresAt time.Time       `json:"lease_expires_at"`
}

// Acquisition reports an assignment, waiting, or completion.
type Acquisition struct {
	State      AcquisitionState `json:"state"`
	Assignment *Assignment      `json:"assignment,omitempty"`
}

// PageRequest selects a bounded page.
type PageRequest struct {
	Size  int
	Token string
}

// Page contains records and an optional continuation token.
type Page[T any] struct {
	Items         []T    `json:"items"`
	NextPageToken string `json:"next_page_token,omitempty"`
}
