package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
)

// Options configures the store.
type Options struct {
	// File lock wait. Zero uses one second.
	Timeout time.Duration
	// Clock for timestamps and leases. Nil uses time.Now.
	Now func() time.Time
}

// Store owns one bbolt database.
type Store struct {
	db  *bolt.DB
	now func() time.Time
}

var _ Storage = (*Store)(nil)

// Open opens or creates a database with durable commits.
func Open(path string, options *Options) (*Store, error) {
	if path == "" {
		return nil, fmt.Errorf("database path: %w", ErrInvalid)
	}

	opts := Options{}
	if options != nil {
		opts = *options
	}
	if opts.Timeout < 0 {
		return nil, fmt.Errorf("open timeout: %w", ErrInvalid)
	}
	if opts.Timeout == 0 {
		opts.Timeout = time.Second
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create database directory: %w", err)
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: opts.Timeout})
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	if err := db.Update(initSchema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initialize database: %w", err)
	}

	return &Store{db: db, now: opts.Now}, nil
}

func (s *Store) DB() *bolt.DB {
	return s.db // TODO: Remove accessor and add new constructor.
}

// Close closes the database after active transactions finish.
func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) view(ctx context.Context, fn ViewFn) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	err := s.db.View(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(newTx(ctx, tx)); err != nil {
			return err
		}
		return ctx.Err()
	})
	return databaseError(err)
}

func (s *Store) update(ctx context.Context, fn UpdateFn) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	err := s.db.Update(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(newTx(ctx, tx), s.now().UTC()); err != nil {
			return err
		}
		return ctx.Err()
	})
	return databaseError(err)
}
