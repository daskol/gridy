package sweep

import (
	"context"

	"github.com/daskol/gridy/internal/service/util"
	sweep_api "github.com/daskol/gridy/pkg/api/sweep/v1"
	"github.com/daskol/gridy/pkg/storage"
)

// ListTrials returns a page of trials.
func (s *SweepService) ListTrials(ctx context.Context, req *sweep_api.ListTrialsRequest) (*sweep_api.ListTrialsResponse, error) {
	page, err := s.store.ListTrials(ctx, req.GetSweepId(), storage.PageRequest{
		Size:  int(req.GetPageSize()),
		Token: req.GetPageToken(),
	})
	if err != nil {
		return nil, util.Error(err)
	}

	items := make([]*sweep_api.Trial, 0, len(page.Items))
	for _, trial := range page.Items {
		item, err := marshalTrial(trial)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return &sweep_api.ListTrialsResponse{
		Trials:        items,
		NextPageToken: page.NextPageToken,
	}, nil
}
