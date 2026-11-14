// Package searchspace loads TOML parameter grids and enumerates their trials.
package searchspace

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/daskol/gridy/pkg/storage"
	"github.com/pelletier/go-toml/v2"
)

const maxSafeInteger = 1<<53 - 1

type axis struct {
	path   []string
	values []any
}

// LoadFiles reads independent sweeps named after their TOML files.
//
// Tables retain their nesting. Each leaf must be a nonempty array of distinct
// JSON scalars. Integer candidates must be in the JSON API safe range
// [-(2^53-1), 2^53-1]. Keys are sorted and array order is preserved, making
// trial IDs reproducible. All inputs are validated before any imports are
// returned.
func LoadFiles(ctx context.Context, paths []string) ([]storage.SweepImport, error) {
	imports := make([]storage.SweepImport, 0, len(paths))
	seen := make(map[string]string, len(paths))

	for _, path := range paths {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		id, err := sweepIDFromPath(path)
		if err != nil {
			return nil, fmt.Errorf("search space %q: %w", path, err)
		}

		if previous, exists := seen[id]; exists {
			return nil, fmt.Errorf("duplicate sweep %q: %q and %q", id, previous, path)
		}
		seen[id] = path

		item, err := loadFile(ctx, path, id)
		if err != nil {
			return nil, fmt.Errorf("search space %q: %w", path, err)
		}
		imports = append(imports, item)
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return imports, nil
}

func sweepIDFromPath(path string) (string, error) {
	id, ok := strings.CutSuffix(filepath.Base(path), ".toml")
	if !ok {
		return "", errors.New("expected .toml file")
	}

	if id == "" || strings.ContainsFunc(id, func(char rune) bool {
		return (char < 'a' || char > 'z') &&
			(char < 'A' || char > 'Z') &&
			(char < '0' || char > '9') &&
			char != '_' && char != '-'
	}) {
		return "", fmt.Errorf("invalid filename: %w", storage.ErrInvalid)
	}

	return id, nil
}

func loadFile(ctx context.Context, path, id string) (storage.SweepImport, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return storage.SweepImport{}, err
	}

	space := make(map[string]any)
	if err := toml.Unmarshal(data, &space); err != nil {
		return storage.SweepImport{}, err
	}

	axes, err := collectAxes(ctx, space, nil)
	if err != nil {
		return storage.SweepImport{}, err
	}

	encoded, err := json.Marshal(space)
	if err != nil {
		return storage.SweepImport{}, err
	}

	trials, err := enumerate(ctx, axes)
	if err != nil {
		return storage.SweepImport{}, err
	}

	return storage.SweepImport{
		Spec:   storage.SweepSpec{ID: id, Code: id, Space: encoded},
		Trials: trials,
	}, nil
}

func collectAxes(ctx context.Context, table map[string]any, path []string) ([]axis, error) {
	if len(table) == 0 {
		return nil, fmt.Errorf("empty table %q", strings.Join(path, "."))
	}

	axes := make([]axis, 0, len(table))
	for _, key := range slices.Sorted(maps.Keys(table)) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		childPath := append(slices.Clone(path), key)
		switch value := table[key].(type) {
		case map[string]any:
			children, err := collectAxes(ctx, value, childPath)
			if err != nil {
				return nil, err
			}
			axes = append(axes, children...)
		case []any:
			joinedPath := strings.Join(childPath, ".")
			if err := validateAxis(ctx, joinedPath, value); err != nil {
				return nil, err
			}
			axes = append(axes, axis{path: childPath, values: value})
		default:
			joinedPath := strings.Join(childPath, ".")
			return nil, fmt.Errorf("parameter %q: expected array", joinedPath)
		}
	}

	return axes, nil
}

func validateAxis(ctx context.Context, name string, values []any) error {
	if len(values) == 0 {
		return fmt.Errorf("parameter %q: empty array", name)
	}

	seen := make(map[any]struct{}, len(values))
	for index, value := range values {
		if err := ctx.Err(); err != nil {
			return err
		}

		key, err := candidateKey(value)
		if err != nil {
			return fmt.Errorf("parameter %q[%d]: %w", name, index, err)
		}
		if _, exists := seen[key]; exists {
			return fmt.Errorf("parameter %q[%d]: duplicate value", name, index)
		}
		seen[key] = struct{}{}
	}

	return nil
}

// candidateKey validates a scalar and returns a comparable duplicate key. Safe
// integers and floats share numeric keys; the original values stay intact.
func candidateKey(value any) (any, error) {
	switch value := value.(type) {
	case string, bool:
		return value, nil
	case int64:
		if value < -maxSafeInteger || value > maxSafeInteger {
			return nil, errors.New("integer out of range")
		}
		return float64(value), nil
	case float64:
		if math.IsInf(value, 0) || math.IsNaN(value) {
			return nil, errors.New("nonfinite number")
		}
		return value, nil
	default:
		return nil, errors.New("unsupported value type")
	}
}

func gridSize(axes []axis) (int, error) {
	count := 1
	for _, axis := range axes {
		if count > math.MaxInt/len(axis.values) {
			return 0, errors.New("grid size overflow")
		}
		count *= len(axis.values)
	}
	return count, nil
}

func enumerate(ctx context.Context, axes []axis) ([]storage.TrialSpec, error) {
	count, err := gridSize(axes)
	if err != nil {
		return nil, err
	}

	trials := make([]storage.TrialSpec, 0)
	indices := make([]int, len(axes))

	for index := range count {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		encoded, err := json.Marshal(parametersAt(axes, indices))
		if err != nil {
			return nil, err
		}

		trials = append(trials, storage.TrialSpec{
			ID:     strconv.Itoa(index),
			Params: encoded,
		})

		for i := len(indices) - 1; i >= 0; i-- {
			indices[i]++
			if indices[i] < len(axes[i].values) {
				break
			}
			indices[i] = 0
		}
	}

	return trials, nil
}

func parametersAt(axes []axis, indices []int) map[string]any {
	params := make(map[string]any)
	for i, axis := range axes {
		table := params
		for _, key := range axis.path[:len(axis.path)-1] {
			child, exists := table[key]
			if !exists {
				child = make(map[string]any)
				table[key] = child
			}
			table = child.(map[string]any)
		}

		table[axis.path[len(axis.path)-1]] = axis.values[indices[i]]
	}

	return params
}
