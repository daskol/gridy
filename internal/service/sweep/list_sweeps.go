package sweep

import (
	"context"

	"github.com/daskol/gridy/internal/service/storageapi"
	sweep_api "github.com/daskol/gridy/pkg/api/sweep/v1"
	"github.com/daskol/gridy/pkg/storage"
)

// ListSweeps returns a page of sweeps.
func (s *SweepService) ListSweeps(ctx context.Context, req *sweep_api.ListSweepsRequest) (*sweep_api.ListSweepsResponse, error) {
	page, err := s.store.ListSweeps(ctx, storage.PageRequest{
		Size:  int(req.GetPageSize()),
		Token: req.GetPageToken(),
	})
	if err != nil {
		return nil, storageapi.Error(err)
	}

	items, err := marshalSweeps(page.Items)
	if err != nil {
		return nil, err
	}
	return &sweep_api.ListSweepsResponse{
		Sweeps:        items,
		NextPageToken: page.NextPageToken,
	}, nil
}
