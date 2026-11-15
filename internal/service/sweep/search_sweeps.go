package sweep

import (
	"context"

	"github.com/daskol/gridy/internal/service/util"
	sweep_api "github.com/daskol/gridy/pkg/api/sweep/v1"
	"github.com/daskol/gridy/pkg/storage"
)

// SearchSweeps finds sweeps matching a query.
func (s *SweepService) SearchSweeps(ctx context.Context, req *sweep_api.SearchSweepsRequest) (*sweep_api.SearchSweepsResponse, error) {
	page, err := s.store.SearchSweeps(ctx, req.GetQuery(), storage.PageRequest{
		Size:  int(req.GetPageSize()),
		Token: req.GetPageToken(),
	})
	if err != nil {
		return nil, util.Error(err)
	}

	items, err := marshalSweeps(page.Items)
	if err != nil {
		return nil, err
	}
	return &sweep_api.SearchSweepsResponse{
		Sweeps:        items,
		NextPageToken: page.NextPageToken,
	}, nil
}
