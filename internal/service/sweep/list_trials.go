package sweep

import (
	"context"

	sweep_api "github.com/daskol/gridy/pkg/api/sweep/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ListTrials returns a page of trials.
func (s *SweepService) ListTrials(context.Context, *sweep_api.ListTrialsRequest) (*sweep_api.ListTrialsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method ListTrials not implemented")
}
