package storage

import (
	"errors"

	bbolt_errors "go.etcd.io/bbolt/errors"
)

var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")
	ErrConflict      = errors.New("conflict")
	ErrInvalid       = errors.New("invalid argument")
	ErrSealed        = errors.New("sweep is sealed")
	ErrStaleAttempt  = errors.New("stale attempt")
	ErrLeaseExpired  = errors.New("lease expired")
	ErrClosed        = errors.New("store is closed")
	ErrSchema        = errors.New("unsupported storage schema")
	ErrCorrupt       = errors.New("corrupt storage record")
)

func databaseError(err error) error {
	if errors.Is(err, bbolt_errors.ErrDatabaseNotOpen) {
		return ErrClosed
	}
	return err
}
