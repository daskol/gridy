package storage_test

import (
	"math"
	"testing"

	"github.com/daskol/gridy/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestKeyBytes(t *testing.T) {
	t.Parallel()

	trial := storage.TrialKey{SweepID: "sweep", TrialID: "trial"}
	expected := "sweep\x00\x00trial\x00\x00"
	require.Equal(t, []byte(expected), trial.Bytes())

	first := storage.AttemptKey{TrialKey: trial, AttemptID: 1}
	expected = "sweep\x00\x00trial\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01"
	require.Equal(t, []byte(expected), first.Bytes())

	next := storage.AttemptKey{TrialKey: trial, AttemptID: 256}
	expected = "sweep\x00\x00trial\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00"
	require.Equal(t, []byte(expected), next.Bytes())
}

func TestTrialKeyValidate(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		id    string
		valid bool
	}{
		{"allowed characters", "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-", true},
		{"empty", "", false},
		{"below digits", "/", false},
		{"above digits", ":", false},
		{"below uppercase", "@", false},
		{"above uppercase", "[", false},
		{"below lowercase", "`", false},
		{"above lowercase", "{", false},
		{"embedded separator", "a/b", false},
		{"nul", "a\x00b", false},
		{"unicode", "é", false},
		{"invalid utf8", "\xff", false},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			keys := []storage.TrialKey{
				{SweepID: test.id, TrialID: "trial"},
				{SweepID: "sweep", TrialID: test.id},
			}
			for _, key := range keys {
				if test.valid {
					require.NoError(t, key.Validate())
				} else {
					require.ErrorIs(t, key.Validate(), storage.ErrInvalid)
				}
			}
		})
	}
}

func TestAttemptKeyValidate(t *testing.T) {
	t.Parallel()

	trialKey := storage.TrialKey{SweepID: "sweep", TrialID: "trial"}
	key := storage.AttemptKey{
		TrialKey:  trialKey,
		AttemptID: 1,
	}
	require.NoError(t, key.Validate())

	key.AttemptID = math.MaxInt64
	require.NoError(t, key.Validate())

	key.AttemptID = 0
	require.ErrorIs(t, key.Validate(), storage.ErrInvalid)

	key.AttemptID = -1
	require.ErrorIs(t, key.Validate(), storage.ErrInvalid)

	key.AttemptID = 1
	key.TrialID = "invalid/trial"
	require.ErrorIs(t, key.Validate(), storage.ErrInvalid)

	key.TrialID = "trial"
	key.SweepID = ""
	require.ErrorIs(t, key.Validate(), storage.ErrInvalid)
}
