package storage

import (
	"encoding/binary"
	"fmt"

	"go.etcd.io/bbolt"
)

// The coordinator stores all data in one bbolt file with eight root buckets.
// The sweeps, workers, trials and attempts buckets hold entity records. The
// requests bucket holds assignment snapshots. The ready and leases buckets are
// secondary indexes. The meta bucket holds the schema version.
//
// A trial belongs to one sweep. Each attempt belongs to a trial and records
// its worker. Trial.AttemptID identifies the latest attempt. Older attempts
// remain available as execution history.
//
// The following pseudo SQL describes the logical schema. Value types are Go
// records stored as JSON. Store maintains the references and indexes.
//
//  CREATE TABLE sweeps (sweep_id PRIMARY KEY, value Sweep);
//  CREATE TABLE workers (worker_id PRIMARY KEY, value Worker);
//  CREATE TABLE trials (sweep_id, trial_id, value Trial,
//      PRIMARY KEY (sweep_id, trial_id));
//  CREATE TABLE attempts (sweep_id, trial_id, attempt_id, value Attempt,
//      PRIMARY KEY (sweep_id, trial_id, attempt_id));
//  CREATE TABLE requests (sweep_id, worker_id, request_id, value Assignment,
//      PRIMARY KEY (sweep_id, worker_id, request_id));
//
//  CREATE INDEX ready ON trials (sweep_id, trial_id)
//      WHERE value.state = 'pending';
//  CREATE INDEX leases ON attempts
//      (value.lease_expires_at, sweep_id, trial_id, attempt_id)
//      WHERE value.state = 'running';
//
// The layouts below use <trial_key> and <attempt_key> for the complete keys in
// the trials and attempts buckets.
//
//	meta      "version"
//	sweeps    <sweep_id>     <00h> <00h>
//	workers   <worker_id>    <00h> <00h>
//	trials    <sweep_id>     <00h> <00h> <trial_id> <00h> <00h>
//	attempts  <trial_key>    <attempt_id_be64>
//	requests  <sweep_id>     <00h> <00h> <worker_id> <00h> <00h>
//			  <request_id>   <00h> <00h>
//	ready     <trial_key>
//	leases    <seconds_be64> <nanoseconds_be32> <attempt_key>
//
// String fields contain UTF-8 bytes. An embedded <00h> is escaped as <00h>
// <FFh>. The pair <00h> <00h> terminates each string field. All integer fields
// are big-endian. Lease seconds use Unix time with the sign bit flipped so the
// leases bucket is ordered by expiration time.
//
// The meta value is the schema version encoded as an eight-byte integer. Ready
// values contain <01h>. Lease values contain <attempt_key>. All other values
// are the JSON records shown in the logical schema.
//
// Adding a trial writes both its record and its ready entry. Acquire removes
// the ready entry, creates an attempt and its lease, and saves an assignment
// in requests. The request key scopes retries to one worker and sweep.
// Repeated acquisitions return that snapshot even after its lease changes.
//
// RenewLease replaces the lease index entry with the new deadline. Completion,
// failure and expiry remove the lease entry and preserve the attempt record.
// Failed and expired trials return to ready while retries remain. Trial and
// sweep state and counters change in the same transaction as their indexes.
// Worker requests also update contact times in that transaction.
//
// An empty ready index for a sweep can mean that workers are still running or
// trial generation is unfinished. Acquire reports completion only when the
// sweep is sealed and every trial has reached a terminal state. Successful
// writes return after commit.

// Table names identify root buckets.
const (
	TableMeta     = "meta"
	TableSweeps   = "sweeps"
	TableWorkers  = "workers"
	TableTrials   = "trials"
	TableAttempts = "attempts"
	TableRequests = "requests"
	TableReady    = "ready"
	TableLeases   = "leases"
)

// schemaVersion identifies the bucket layout and key encodings.
const schemaVersion = uint64(1)

// initSchema initializes an empty database or validates the stored schema.
func initSchema(tx *bbolt.Tx) error {
	tables := []string{
		TableMeta, TableSweeps, TableWorkers, TableTrials,
		TableAttempts, TableRequests, TableReady, TableLeases,
	}

	if meta := tx.Bucket([]byte(TableMeta)); meta != nil {
		ver := meta.Get([]byte("version"))
		if len(ver) != 8 || binary.BigEndian.Uint64(ver) != schemaVersion {
			return ErrSchema
		}

		for _, name := range tables {
			if tx.Bucket([]byte(name)) == nil {
				return fmt.Errorf("missing bucket %s: %w", name, ErrCorrupt)
			}
		}
		return nil
	}

	// A nonempty database without metadata has an unknown schema.
	if key, _ := tx.Cursor().First(); key != nil {
		return ErrSchema
	}

	for _, name := range tables {
		if _, err := tx.CreateBucketIfNotExists([]byte(name)); err != nil {
			return err
		}
	}

	return tx.Bucket([]byte(TableMeta)).Put(
		[]byte("version"),
		binary.BigEndian.AppendUint64(nil, schemaVersion),
	)
}
