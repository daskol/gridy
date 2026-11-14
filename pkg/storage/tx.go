package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.etcd.io/bbolt"
)

type UpdateFn func(*Tx, time.Time) error

type ViewFn func(*Tx) error

// Tx provides typed access within one database transaction.
type Tx struct {
	ctx context.Context
	raw *bbolt.Tx

	Ready  readyBucket
	Leases leaseBucket

	Sweeps   recordBucket[string, Sweep]
	Workers  recordBucket[string, Worker]
	Trials   recordBucket[TrialKey, Trial]
	Attempts recordBucket[AttemptKey, Attempt]
	Requests recordBucket[AcquireParams, Assignment]
}

func newTx(ctx context.Context, raw *bbolt.Tx) *Tx {
	return &Tx{
		ctx: ctx,
		raw: raw,

		Ready:  readyBucket{raw},
		Leases: leaseBucket{raw},

		Sweeps:   recordBucket[string, Sweep]{raw, TableSweeps, tuple1},
		Workers:  recordBucket[string, Worker]{raw, TableWorkers, tuple1},
		Trials:   recordBucket[TrialKey, Trial]{raw, TableTrials, TrialKey.Bytes},
		Attempts: recordBucket[AttemptKey, Attempt]{raw, TableAttempts, AttemptKey.Bytes},
		Requests: recordBucket[AcquireParams, Assignment]{raw, TableRequests, AcquireParams.Bytes},
	}
}

func (tx *Tx) Context() context.Context {
	return tx.ctx
}

// recordBucket stores typed records with encoded keys.
type recordBucket[K, V any] struct {
	tx   *bbolt.Tx
	name string
	key  func(K) []byte
}

func (b recordBucket[K, V]) Get(key K) (V, error) {
	return GetRecord[V](b.tx, b.name, b.key(key))
}

func (b recordBucket[K, V]) Put(key K, value V) error {
	return PutRecord(b.tx, b.name, b.key(key), value)
}

func (b recordBucket[K, V]) Exists(key K) bool {
	return b.tx.Bucket([]byte(b.name)).Get(b.key(key)) != nil
}

// readyBucket indexes trials available for assignment.
type readyBucket struct {
	tx *bbolt.Tx
}

func (b readyBucket) Put(key TrialKey) error {
	return b.tx.Bucket([]byte(TableReady)).Put(key.Bytes(), []byte{1})
}

func (b readyBucket) Delete(key TrialKey) error {
	return b.tx.Bucket([]byte(TableReady)).Delete(key.Bytes())
}

// Next returns nil when the sweep has no ready trial.
func (b readyBucket) Next(sweepID string) (*Trial, error) {
	prefix := tuple(sweepID)
	key, _ := b.tx.Bucket([]byte(TableReady)).Cursor().Seek(prefix)
	if !bytes.HasPrefix(key, prefix) {
		return nil, nil
	}
	trial, err := GetRecord[Trial](b.tx, TableTrials, key)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(key, trial.Bytes()) {
		return nil, fmt.Errorf("ready trial key: %w", ErrCorrupt)
	}
	return &trial, nil
}

// leaseBucket indexes attempts by lease expiration.
type leaseBucket struct {
	tx *bbolt.Tx
}

func (b leaseBucket) Put(attempt Attempt) error {
	return b.tx.Bucket([]byte(TableLeases)).
		Put(leaseKey(attempt), attempt.Bytes())
}

func (b leaseBucket) Delete(attempt Attempt) error {
	return b.tx.Bucket([]byte(TableLeases)).Delete(leaseKey(attempt))
}

// Expired collects a bounded batch before any index entries are removed.
func (b leaseBucket) Expired(now time.Time, limit int) ([]Attempt, error) {
	expired := []Attempt{}
	cutoff := deadlineKey(now)
	cursor := b.tx.Bucket([]byte(TableLeases)).Cursor()
	for key, val := cursor.First(); key != nil && len(expired) < limit; key, val = cursor.Next() {
		if len(key) < len(cutoff) {
			return nil, fmt.Errorf("lease index key: %w", ErrCorrupt)
		}
		if bytes.Compare(key[:len(cutoff)], cutoff) > 0 {
			break
		}

		entry, err := GetRecord[Attempt](b.tx, TableAttempts, val)
		if err != nil {
			return nil, err
		}
		if entry.State != AttemptRunning || !bytes.Equal(leaseKey(entry), key) {
			return nil, fmt.Errorf("lease index record: %w", ErrCorrupt)
		}

		expired = append(expired, entry)
	}
	return expired, nil
}

func deadlineKey(deadline time.Time) []byte {
	key := binary.BigEndian.AppendUint64(nil, uint64(deadline.Unix())^(1<<63))
	return binary.BigEndian.AppendUint32(key, uint32(deadline.Nanosecond()))
}

func leaseKey(attempt Attempt) []byte {
	return append(deadlineKey(attempt.LeaseExpiresAt), attempt.Bytes()...)
}

// GetRecord reads a JSON record from a table.
func GetRecord[T any](tx *bbolt.Tx, table string, key []byte) (T, error) {
	record := *new(T)
	bucket := tx.Bucket([]byte(table))
	if bucket == nil {
		return record, fmt.Errorf("table %q: %w", table, ErrNotFound)
	}
	data := bucket.Get(key)
	if data == nil {
		return record, ErrNotFound
	}
	if err := json.Unmarshal(data, &record); err != nil {
		return record, fmt.Errorf("decode %s: %w: %v", table, ErrCorrupt, err)
	}
	return record, nil
}

// PutRecord writes a JSON record to a table.
func PutRecord(tx *bbolt.Tx, table string, key []byte, record any) error {
	bucket := tx.Bucket([]byte(table))
	if bucket == nil {
		return fmt.Errorf("table %q: %w", table, ErrNotFound)
	}
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return bucket.Put(key, data)
}

// TouchWorker updates a worker contact time.
func TouchWorker(tx *Tx, id string, now time.Time) error {
	worker, err := tx.Workers.Get(id)
	if errors.Is(err, ErrNotFound) {
		worker = Worker{ID: id, FirstSeenAt: now}
	} else if err != nil {
		return err
	}
	if now.After(worker.LastSeenAt) {
		worker.LastSeenAt = now
	}
	return tx.Workers.Put(id, worker)
}
