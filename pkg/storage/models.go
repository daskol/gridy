package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"time"
	"unicode/utf8"
)

const (
	defaultLeaseDuration = 5 * time.Minute
	defaultMaxAttempts   = 3
	defaultPageSize      = 100
	maxPageSize          = 1000
)

// Record identifies the table for a stored value.
type Record interface {
	// BucketName returns the table name for the zero value.
	BucketName() string
}

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

// Normalize validates the spec and applies defaults.
func (spec SweepSpec) Normalize() (SweepSpec, error) {
	if !validID(spec.ID) {
		return SweepSpec{}, fmt.Errorf("sweep identifier: %w", ErrInvalid)
	}
	if spec.LeaseDuration < 0 || spec.MaxAttempts < 0 {
		return SweepSpec{}, fmt.Errorf("sweep execution policy: %w", ErrInvalid)
	}
	if spec.LeaseDuration == 0 {
		spec.LeaseDuration = defaultLeaseDuration
	}
	if spec.MaxAttempts == 0 {
		spec.MaxAttempts = defaultMaxAttempts
	}
	space, err := spec.GetSpace()
	if err != nil {
		return SweepSpec{}, err
	}
	spec.Space = space
	return spec, nil
}

// GetSpace returns a normalized copy of the search space.
func (spec SweepSpec) GetSpace() (json.RawMessage, error) {
	return normalizeObject(spec.Space, true)
}

func (spec SweepSpec) validateExecution() error {
	if spec.LeaseDuration <= 0 || spec.MaxAttempts <= 0 {
		return fmt.Errorf("sweep execution settings: %w", ErrCorrupt)
	}
	return nil
}

func (spec SweepSpec) leaseDeadline(now time.Time) (time.Time, error) {
	if spec.LeaseDuration <= 0 {
		return time.Time{}, fmt.Errorf("sweep lease duration: %w", ErrCorrupt)
	}
	return now.Add(spec.LeaseDuration), nil
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

// BucketName returns the sweep table name.
func (Sweep) BucketName() string { return TableSweeps }

// Worker records coordinator contact times.
type Worker struct {
	ID          string    `json:"worker_id"`
	FirstSeenAt time.Time `json:"first_seen_at"`
	LastSeenAt  time.Time `json:"last_seen_at"`
}

// BucketName returns the worker table name.
func (Worker) BucketName() string { return TableWorkers }

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

// Normalize validates the spec and normalizes its parameters.
func (spec TrialSpec) Normalize() (TrialSpec, error) {
	if !validID(spec.ID) {
		return TrialSpec{}, fmt.Errorf("trial identifier: %w", ErrInvalid)
	}
	params, err := normalizeObject(spec.Params, true)
	if err != nil {
		return TrialSpec{}, err
	}
	spec.Params = params
	return spec, nil
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

// BucketName returns the trial table name.
func (Trial) BucketName() string { return TableTrials }

func (t Trial) validateAttempt(attemptID int64) error {
	if t.AttemptID != attemptID {
		return ErrStaleAttempt
	}
	return nil
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

func (t *Trial) finish(attempt Attempt, sweep *Sweep, now time.Time) error {
	if attempt.State != AttemptCompleted && sweep.MaxAttempts <= 0 {
		return fmt.Errorf("sweep retry limit: %w", ErrCorrupt)
	}

	t.Result = attempt.Result
	t.Error = attempt.Error
	t.UpdatedAt = now

	switch {
	case attempt.State == AttemptCompleted:
		t.State = TrialCompleted
		sweep.TerminalCount++
	case attempt.AttemptID >= int64(sweep.MaxAttempts):
		t.State = TrialFailed
		sweep.TerminalCount++
		sweep.FailedCount++
	default:
		t.State = TrialPending
	}
	return nil
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

// BucketName returns the attempt table name.
func (Attempt) BucketName() string { return TableAttempts }

// NormalizeResult validates a completion or failure.
func (a Attempt) NormalizeResult() (Attempt, error) {
	if err := a.AttemptKey.Validate(); err != nil {
		return Attempt{}, err
	}
	switch a.State {
	case AttemptCompleted:
		result, err := normalizeObject(a.Result, false)
		if err != nil {
			return Attempt{}, err
		}
		a.Result = result
	case AttemptFailed:
		if a.Error == "" || !utf8.ValidString(a.Error) {
			return Attempt{}, fmt.Errorf("failure message: %w", ErrInvalid)
		}
	default:
		return Attempt{}, fmt.Errorf("result state: %w", ErrInvalid)
	}
	return a, nil
}

func (a Attempt) matchesResult(result Attempt) bool {
	if a.State != result.State {
		return false
	}
	switch a.State {
	case AttemptCompleted:
		return sameJSON(a.Result, result.Result)
	case AttemptFailed:
		return a.Error == result.Error
	default:
		return false
	}
}

func (a Attempt) validateLive(trial Trial, now time.Time) error {
	switch a.State {
	case AttemptExpired:
		return ErrLeaseExpired
	case AttemptCompleted, AttemptFailed:
		return ErrConflict
	case AttemptRunning:
		switch {
		case trial.State != TrialRunning:
			return fmt.Errorf("attempt state: %w", ErrCorrupt)
		case !now.Before(a.LeaseExpiresAt):
			return ErrLeaseExpired
		default:
			return nil
		}
	default:
		return fmt.Errorf("attempt state: %w", ErrCorrupt)
	}
}

func (a Attempt) validateExpiry(trial Trial) error {
	if trial.State != TrialRunning || a.State != AttemptRunning {
		return fmt.Errorf("expired attempt state: %w", ErrCorrupt)
	}
	return nil
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

// BucketName returns the assignment table name.
func (Assignment) BucketName() string { return TableRequests }

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

// Normalize validates and bounds the page size.
func (p PageRequest) Normalize() (PageRequest, error) {
	switch {
	case p.Size < 0:
		return PageRequest{}, fmt.Errorf("page size: %w", ErrInvalid)
	case p.Size == 0:
		p.Size = defaultPageSize
	case p.Size > maxPageSize:
		p.Size = maxPageSize
	}
	return p, nil
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
