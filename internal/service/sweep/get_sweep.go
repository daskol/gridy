package sweep

import (
	"context"

	"github.com/daskol/gridy/internal/service/storageapi"
	sweep_api "github.com/daskol/gridy/pkg/api/sweep/v1"
)

// GetSweep returns a sweep.
func (s *SweepService) GetSweep(ctx context.Context, req *sweep_api.GetSweepRequest) (*sweep_api.Sweep, error) {
	sweep, err := s.store.GetSweep(ctx, req.GetSweepId())
	if err != nil {
		return nil, storageapi.Error(err)
	}
	return marshalSweep(sweep)
}
