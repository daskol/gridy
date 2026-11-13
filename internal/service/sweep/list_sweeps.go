package sweep

import (
	"context"

	sweep_api "github.com/daskol/gridy/pkg/api/sweep/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ListSweeps returns a page of sweeps.
func (s *SweepService) ListSweeps(context.Context, *sweep_api.ListSweepsRequest) (*sweep_api.ListSweepsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method ListSweeps not implemented")
}
