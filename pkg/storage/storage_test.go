package storage_test

import (
	"path/filepath"
	"testing"

	"github.com/daskol/gridy/pkg/storage"
	"github.com/daskol/gridy/pkg/storage/internal/testsuite"
)

func TestStore(t *testing.T) {
	t.Parallel()

	testsuite.Run(t, func(t *testing.T, now testsuite.NowFn) (storage.Storage, error) {
		storePath := filepath.Join(t.TempDir(), "store.db")
		return storage.Open(storePath, &storage.Options{Now: now})
	})
}
