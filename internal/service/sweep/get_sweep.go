package sweep

import (
	"context"

	sweep_api "github.com/daskol/gridy/pkg/api/sweep/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetSweep returns a sweep.
func (s *SweepService) GetSweep(context.Context, *sweep_api.GetSweepRequest) (*sweep_api.Sweep, error) {
	return nil, status.Error(codes.Unimplemented, "method GetSweep not implemented")
}
