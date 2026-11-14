package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"time"
	"unicode/utf8"
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

// Bytes returns a new encoded trial key.
func (key TrialKey) Bytes() []byte {
	return tuple(key.SweepID, key.TrialID)
}

// Validate checks trial identifiers.
func (key TrialKey) Validate() error {
	if !validID(key.SweepID) || !validID(key.TrialID) {
		return fmt.Errorf("trial key: %w", ErrInvalid)
	}
	return nil
}

// AttemptKey identifies one execution of a trial.
type AttemptKey struct {
	TrialKey
	AttemptID int64 `json:"attempt_id"`
}

// Bytes returns a new encoded attempt key.
func (key AttemptKey) Bytes() []byte {
	return binary.BigEndian.
		AppendUint64(key.TrialKey.Bytes(), uint64(key.AttemptID))
}

// Validate checks trial and attempt identifiers.
func (key AttemptKey) Validate() error {
	if err := key.TrialKey.Validate(); err != nil {
		return err
	}
	if key.AttemptID <= 0 {
		return fmt.Errorf("attempt identifier: %w", ErrInvalid)
	}
	return nil
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

func (t Trial) validateAssignment(sweep Sweep) error {
	switch {
	case t.State != TrialPending:
		return fmt.Errorf("ready trial state: %w", ErrCorrupt)
	case t.SweepID != sweep.ID:
		return fmt.Errorf("ready trial sweep: %w", ErrCorrupt)
	case t.AttemptID < 0:
		return fmt.Errorf("ready trial attempt: %w", ErrCorrupt)
	case t.AttemptID == math.MaxInt64:
		return fmt.Errorf("attempt id exhausted: %w", ErrConflict)
	case t.AttemptID >= int64(sweep.MaxAttempts):
		return fmt.Errorf("trial retry limit: %w", ErrCorrupt)
	default:
		return nil
	}
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

func (p AcquireParams) Bytes() []byte {
	return tuple(p.SweepID, p.WorkerID, p.RequestID)
}

// Validate checks acquisition identifiers.
func (p AcquireParams) Validate() error {
	switch {
	case !validID(p.SweepID):
		return fmt.Errorf("sweep identifier: %w", ErrInvalid)
	case p.WorkerID == "" || !utf8.ValidString(p.WorkerID):
		return fmt.Errorf("worker identifier: %w", ErrInvalid)
	case p.RequestID == "" || !utf8.ValidString(p.RequestID):
		return fmt.Errorf("request identifier: %w", ErrInvalid)
	default:
		return nil
	}
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

func validID(id string) bool {
	if id == "" {
		return false
	}
	for _, char := range id {
		if (char < 'a' || char > 'z') &&
			(char < 'A' || char > 'Z') &&
			(char < '0' || char > '9') &&
			char != '_' && char != '-' {
			return false
		}
	}
	return true
}

// tuple preserves byte order and separates arbitrary string components.
func tuple(parts ...string) []byte {
	var result []byte
	for _, part := range parts {
		for i := range len(part) {
			if part[i] == 0 {
				result = append(result, 0, 255)
			} else {
				result = append(result, part[i])
			}
		}
		result = append(result, 0, 0)
	}
	return result
}

// tuple1 serializes string ID to bytes.
func tuple1(id string) []byte {
	return tuple(id)
}
