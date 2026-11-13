package sweep

import (
	"context"

	sweep_api "github.com/daskol/gridy/pkg/api/sweep/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetTrial returns a trial.
func (s *SweepService) GetTrial(context.Context, *sweep_api.GetTrialRequest) (*sweep_api.Trial, error) {
	return nil, status.Error(codes.Unimplemented, "method GetTrial not implemented")
}
