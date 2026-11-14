package storage_test

import (
	"testing"

	"github.com/daskol/gridy/pkg/storage"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func TestRecordRejectsMissingTable(t *testing.T) {
	t.Parallel()

	s := NewSuite(t, 1, 0)
	key := storage.TrialKey{SweepID: "sweep", TrialID: "trial"}
	err := s.store.DB().Update(func(tx *bbolt.Tx) error {
		trial, err := storage.GetRecord[storage.Trial](tx, "missing", key.Bytes())
		require.ErrorIs(s, err, storage.ErrNotFound)
		require.Zero(s, trial)

		trialZero := storage.Trial{TrialKey: key}
		err = storage.PutRecord(tx, "missing", key.Bytes(), trialZero)
		require.ErrorIs(s, err, storage.ErrNotFound)

		bucket := tx.Bucket([]byte("missing"))
		require.Nil(s, bucket)
		return nil
	})
	require.NoError(s, err)
}

func TestAcquireRejectsMismatchedReadyTrial(t *testing.T) {
	t.Parallel()

	s := NewSuite(t, 1, 1)
	key := storage.TrialKey{SweepID: "sweep", TrialID: "0"}
	err := s.store.DB().Update(func(tx *bbolt.Tx) error {
		trial, err := storage.GetRecord[storage.Trial](tx, storage.TableTrials, key.Bytes())
		if err != nil {
			return err
		}
		trial.TrialID = "wrong"
		return storage.PutRecord(tx, storage.TableTrials, key.Bytes(), trial)
	})
	require.NoError(s, err)

	result, err := s.store.Acquire(s.Context(), storage.AcquireParams{
		SweepID: "sweep", WorkerID: "new-worker", RequestID: "request",
	})
	require.ErrorIs(s, err, storage.ErrCorrupt)
	require.Zero(s, result, "failed acquisition returned an assignment")

	_, err = s.store.GetWorker(s.Context(), "new-worker")
	require.ErrorIs(s, err, storage.ErrNotFound, "worker still persisted")
}
