package sweep

import (
	"context"

	"github.com/daskol/gridy/internal/service/util"
	sweep_api "github.com/daskol/gridy/pkg/api/sweep/v1"
	"github.com/daskol/gridy/pkg/storage"
)

// GetTrial returns a trial.
func (s *SweepService) GetTrial(ctx context.Context, req *sweep_api.GetTrialRequest) (*sweep_api.Trial, error) {
	trial, err := s.store.GetTrial(ctx, storage.TrialKey{
		SweepID: req.GetSweepId(),
		TrialID: req.GetTrialId(),
	})
	if err != nil {
		return nil, util.Error(err)
	}
	return marshalTrial(trial)
}
