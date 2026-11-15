// Package util translates storage values and errors for the public API.
package util

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/daskol/gridy/pkg/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

// Error translates a storage error into a gRPC status.
func Error(err error) error {
	if err == nil {
		return nil
	}
	code := codes.Internal
	switch {
	case errors.Is(err, context.Canceled):
		code = codes.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		code = codes.DeadlineExceeded
	case errors.Is(err, storage.ErrInvalid):
		code = codes.InvalidArgument
	case errors.Is(err, storage.ErrNotFound):
		code = codes.NotFound
	case errors.Is(err, storage.ErrAlreadyExists):
		code = codes.AlreadyExists
	case errors.Is(err, storage.ErrConflict), errors.Is(err, storage.ErrStaleAttempt):
		code = codes.Aborted
	case errors.Is(err, storage.ErrLeaseExpired), errors.Is(err, storage.ErrSealed):
		code = codes.FailedPrecondition
	case errors.Is(err, storage.ErrClosed):
		code = codes.Unavailable
	}
	if code == codes.Internal {
		return status.Error(code, "storage operation failed")
	}
	return status.Error(code, err.Error())
}

// Object decodes an optional JSON object from storage.
func Object(raw json.RawMessage) (*structpb.Struct, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	value := &structpb.Struct{}
	if err := protojson.Unmarshal(raw, value); err != nil {
		return nil, status.Error(codes.Internal, "invalid stored JSON object")
	}
	return value, nil
}

// SweepID parses a queue parent resource name.
func SweepID(parent string) (string, error) {
	parts := strings.Split(parent, "/")
	if len(parts) != 2 || parts[0] != "sweeps" {
		return "", status.Error(codes.InvalidArgument, "expected sweeps/{sweep_id}")
	}
	if err := (storage.TrialKey{SweepID: parts[1], TrialID: "0"}).Validate(); err != nil {
		return "", Error(err)
	}
	return parts[1], nil
}

// AttemptKey parses a trial resource name and checks its attempt identifier.
func AttemptKey(name string, attemptID int64) (storage.AttemptKey, error) {
	parts := strings.Split(name, "/")
	if len(parts) != 4 || parts[0] != "sweeps" || parts[2] != "trials" {
		desc := "expected sweeps/{sweep_id}/trials/{trial_id}"
		return storage.AttemptKey{}, status.Error(codes.InvalidArgument, desc)
	}
	key := storage.AttemptKey{
		SweepID:   parts[1],
		TrialID:   parts[3],
		AttemptID: attemptID,
	}
	if err := key.Validate(); err != nil {
		return storage.AttemptKey{}, Error(err)
	}
	return key, nil
}
