package storage

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	bucketMeta     = []byte("meta")
	bucketSweeps   = []byte("sweeps")
	bucketWorkers  = []byte("workers")
	bucketTrials   = []byte("trials")
	bucketAttempts = []byte("attempts")
	bucketRequests = []byte("requests")
	bucketReady    = []byte("ready")
	bucketLeases   = []byte("leases")
)

const (
	schemaVersion        = uint64(1)
	defaultLeaseDuration = 5 * time.Minute
	defaultMaxAttempts   = 3
	defaultPageSize      = 100
	maxPageSize          = 1000
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

// TODO: var _ Storage = (*Store)(nil)

// Open opens or creates a database with durable commits.
func Open(path string, options *Options) (*Store, error) {
	if path == "" {
		return nil, fmt.Errorf("database path: %w", ErrInvalid)
	}

	clock := time.Now
	timeout := time.Second
	if options != nil {
		if options.Timeout < 0 {
			return nil, fmt.Errorf("open timeout: %w", ErrInvalid)
		}
		if options.Timeout > 0 {
			timeout = options.Timeout
		}
		if options.Now != nil {
			clock = options.Now
		}
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create database directory: %w", err)
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: timeout})
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	store := &Store{db: db, now: clock}
	err = store.init(context.Background())
	return store, err
}

func (s *Store) init(ctx context.Context) error {
	err := s.update(ctx, func(tx *bolt.Tx, _ time.Time) error {
		buckets := [][]byte{
			bucketMeta, bucketSweeps, bucketWorkers, bucketTrials,
			bucketAttempts, bucketRequests, bucketReady, bucketLeases,
		}

		if meta := tx.Bucket(bucketMeta); meta != nil {
			ver := meta.Get([]byte("version"))
			if len(ver) != 8 || binary.BigEndian.Uint64(ver) != schemaVersion {
				return ErrSchema
			}

			for _, name := range buckets {
				if tx.Bucket(name) == nil {
					return fmt.Errorf("missing bucket %s: %w", name, ErrCorrupt)
				}
			}
			return nil
		}

		if key, _ := tx.Cursor().First(); key != nil {
			return ErrSchema
		}

		for _, name := range buckets {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return err
			}
		}

		return tx.Bucket(bucketMeta).Put(
			[]byte("version"),
			binary.BigEndian.AppendUint64(nil, schemaVersion),
		)
	})
	if err != nil {
		_ = s.db.Close()
		return fmt.Errorf("initialize database: %w", err)
	}
	return nil
}

// Close closes the database after active transactions finish.
func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) view(ctx context.Context, fn func(*bolt.Tx) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	err := s.db.View(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(tx); err != nil {
			return err
		}
		return ctx.Err()
	})
	return databaseError(err)
}

func (s *Store) update(ctx context.Context, fn func(*bolt.Tx, time.Time) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	err := s.db.Update(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(tx, s.now().UTC()); err != nil {
			return err
		}
		return ctx.Err()
	})
	return databaseError(err)
}
