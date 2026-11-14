package searchspace_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/daskol/gridy/internal/searchspace"
	"github.com/stretchr/testify/require"
)

const example = `
[dataset]
window_size = [3, 5, 7, 9, 11]
num_negatives = [3, 5, 7]

[model]
embed_size = [128, 256, 512]

[optimizer]
batch_size = [512, 1024, 2048, 4096]
lr = [5e-3, 1e-3, 5e-4]
`

func writeSpace(t *testing.T, name, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	err := os.WriteFile(path, []byte(contents), 0o600)
	require.NoError(t, err)
	return path
}

func TestLoadFilesEnumeratesNestedGrid(t *testing.T) {
	t.Parallel()

	imports, err := searchspace.LoadFiles(t.Context(), []string{
		writeSpace(t, "experiment-1.toml", example),
	})
	require.NoError(t, err)
	require.Len(t, imports, 1)

	item := imports[0]
	require.Equal(t, "experiment-1", item.Spec.ID)
	require.Equal(t, "experiment-1", item.Spec.Code)

	require.JSONEq(t, `{
		"dataset":{"window_size":[3,5,7,9,11],"num_negatives":[3,5,7]},
		"model":{"embed_size":[128,256,512]},
		"optimizer":{"batch_size":[512,1024,2048,4096],"lr":[0.005,0.001,0.0005]}
	}`, string(item.Spec.Space))
	require.Len(t, item.Trials, 540)

	require.Equal(t, "0", item.Trials[0].ID)
	require.JSONEq(t, `{
		"dataset":{"window_size":3,"num_negatives":3},
		"model":{"embed_size":128},
		"optimizer":{"batch_size":512,"lr":0.005}
	}`, string(item.Trials[0].Params))

	require.Equal(t, "1", item.Trials[1].ID)
	require.JSONEq(t, `{
		"dataset":{"window_size":3,"num_negatives":3},
		"model":{"embed_size":128},
		"optimizer":{"batch_size":512,"lr":0.001}
	}`, string(item.Trials[1].Params))

	require.Equal(t, "539", item.Trials[539].ID)
	require.JSONEq(t, `{
		"dataset":{"window_size":11,"num_negatives":7},
		"model":{"embed_size":512},
		"optimizer":{"batch_size":4096,"lr":0.0005}
	}`, string(item.Trials[539].Params))

	seen := make(map[string]bool, len(item.Trials))
	for index, trial := range item.Trials {
		require.Equal(t, strconv.Itoa(index), trial.ID)
		require.False(t, seen[string(trial.Params)], "duplicate trial %s", trial.ID)
		seen[string(trial.Params)] = true
	}
}

func TestLoadFilesIgnoresTableAndKeyOrder(t *testing.T) {
	t.Parallel()

	first := writeSpace(t, "experiment.toml", `
[model]
enabled = [true, false]
size = [128, 256]
[optimizer]
lr = [5e-3, 1e-3]
`)
	second := writeSpace(t, "experiment.toml", `
# Different table order, whitespace, and numeric spelling.
[optimizer]
lr = [0.005, 0.001]
[model]
size = [128, 256]
enabled = [true, false]
`)
	want, err := searchspace.LoadFiles(t.Context(), []string{first})
	require.NoError(t, err)

	got, err := searchspace.LoadFiles(t.Context(), []string{second})
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestLoadFilesKeepsIndependentSweepsAndScalarTypes(t *testing.T) {
	t.Parallel()

	const first = `value = [true, "true", 9007199254740991, -9007199254740991, 0.5]`
	const second = `[nested.deeper]
name = ["a", "b"]`

	imports, err := searchspace.LoadFiles(t.Context(), []string{
		writeSpace(t, "first.toml", first),
		writeSpace(t, "second.toml", second),
	})

	require.NoError(t, err)
	require.Len(t, imports, 2)
	require.Equal(t, "first", imports[0].Spec.ID)
	require.Len(t, imports[0].Trials, 5)
	require.JSONEq(t, `{"value":true}`, string(imports[0].Trials[0].Params))
	require.JSONEq(t, `{"value":"true"}`, string(imports[0].Trials[1].Params))
	require.Contains(t, string(imports[0].Trials[2].Params), "9007199254740991")
	require.Contains(t, string(imports[0].Trials[3].Params), "-9007199254740991")
	require.Equal(t, "second", imports[1].Spec.ID)
	require.Len(t, imports[1].Trials, 2)
	require.JSONEq(t, `{"nested":{"deeper":{"name":"a"}}}`, string(imports[1].Trials[0].Params))
}

func TestLoadFilesKeepsDistinctScalarCandidates(t *testing.T) {
	t.Parallel()

	const space = `value = [9007199254740990, 9007199254740991, 0.1, 0.10000000000000002, "1", 1, true]`

	path := writeSpace(t, "distinct.toml", space)
	imports, err := searchspace.LoadFiles(t.Context(), []string{path})
	require.NoError(t, err)
	require.Len(t, imports, 1)

	expected := []string{
		`{"value":9007199254740990}`,
		`{"value":9007199254740991}`,
		`{"value":0.1}`,
		`{"value":0.10000000000000002}`,
		`{"value":"1"}`,
		`{"value":1}`,
		`{"value":true}`,
	}
	require.Len(t, imports[0].Trials, len(expected))
	for index, params := range expected {
		require.Equal(t, strconv.Itoa(index), imports[0].Trials[index].ID)
		require.Equal(t, params, string(imports[0].Trials[index].Params))
	}
}

func TestLoadFilesRejectsInvalidGrid(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name, contents, reason string
	}{
		{"empty file", "", "empty"},
		{"empty table", "[dataset]", "dataset"},
		{"empty axis", "[dataset]\nwindow_size = []", "dataset.window_size"},
		{"scalar", "[dataset]\nwindow_size = 3", "dataset.window_size"},
		{"nested array", "values = [[1, 2]]", "values"},
		{"array of tables", "[[model]]\nsize = [128]", "model"},
		{"date", "date = [2026-11-15]", "date"},
		{"timestamp", "date = [2026-11-15T00:00:00Z]", "date"},
		{"not a number", "value = [nan]", "finite"},
		{"infinity", "value = [+inf]", "finite"},
		{"duplicate", "value = [3, 3]", "duplicate"},
		{"duplicate numeric forms", "value = [1, 1.0]", "duplicate"},
		{"duplicate exponent forms", "value = [1000, 1e3]", "duplicate"},
		{"duplicate zeros", "value = [0.0, -0.0]", "duplicate"},
		{"duplicate mixed zeros", "value = [0, -0.0]", "duplicate"},
		{"duplicate positive boundary", "value = [9007199254740991, 9007199254740991.0]", "duplicate"},
		{"duplicate negative boundary", "value = [-9007199254740991, -9007199254740991.0]", "duplicate"},
		{"unsafe positive integer", "value = [9007199254740992]", "integer out of range"},
		{"unsafe negative integer", "value = [-9007199254740992]", "integer out of range"},
		{"invalid syntax", "values = [", "toml"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := writeSpace(t, "invalid.toml", tc.contents)
			imports, err := searchspace.LoadFiles(t.Context(), []string{path})
			require.ErrorContains(t, err, tc.reason)
			require.ErrorContains(t, err, path)
			require.Nil(t, imports)
		})
	}
}

func TestLoadFilesRejectsAmbiguousFilenames(t *testing.T) {
	t.Parallel()

	names := []string{
		"space.txt", "experiment.TOML", ".toml", "experiment.v1.toml",
		"two words.toml", "эксперимент.toml",
	}

	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			path := writeSpace(t, name, "value = [1]")
			imports, err := searchspace.LoadFiles(t.Context(), []string{path})
			require.ErrorContains(t, err, path)
			require.Nil(t, imports)
		})
	}

	first := writeSpace(t, "same.toml", "value = [1]")
	second := writeSpace(t, "same.toml", "value = [2]")
	imports, err := searchspace.LoadFiles(t.Context(), []string{first, second})
	require.ErrorContains(t, err, "duplicate sweep")
	require.ErrorContains(t, err, first)
	require.ErrorContains(t, err, second)
	require.Nil(t, imports)
}

func TestLoadFilesReturnsNoPartialResult(t *testing.T) {
	t.Parallel()

	first := writeSpace(t, "valid.toml", "value = [1]")
	second := writeSpace(t, "invalid.toml", "value = 2")
	imports, err := searchspace.LoadFiles(t.Context(), []string{first, second})
	require.Error(t, err)
	require.Nil(t, imports)

	missing := filepath.Join(t.TempDir(), "missing.toml")
	imports, err = searchspace.LoadFiles(t.Context(), []string{first, missing})
	require.ErrorIs(t, err, os.ErrNotExist)
	require.Nil(t, imports)
}

func TestLoadFilesRejectsCardinalityOverflow(t *testing.T) {
	t.Parallel()

	contents := &strings.Builder{}
	for axis := range strconv.IntSize {
		fmt.Fprintf(contents, "axis%d = [false, true]\n", axis)
	}
	path := writeSpace(t, "overflow.toml", contents.String())
	imports, err := searchspace.LoadFiles(t.Context(), []string{path})
	require.ErrorContains(t, err, "grid size overflow")
	require.Nil(t, imports)
}

func TestLoadFilesHonorsCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	imports, err := searchspace.LoadFiles(ctx, []string{"missing.toml"})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, imports)
}
