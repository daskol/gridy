package sweep

import (
	"context"

	sweep_api "github.com/daskol/gridy/pkg/api/sweep/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SearchSweeps finds sweeps matching a query.
func (s *SweepService) SearchSweeps(context.Context, *sweep_api.SearchSweepsRequest) (*sweep_api.SearchSweepsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method SearchSweeps not implemented")
}
