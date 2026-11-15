package queue

import (
	"context"

	"github.com/daskol/gridy/internal/service/util"
	"github.com/daskol/gridy/pkg/api/queue/v1"
)

// Fail records an attempt error and applies the sweep retry policy.
func (s *QueueService) Fail(ctx context.Context, req *queue.FailRequest) (*queue.FailResponse, error) {
	key, err := util.AttemptKey(req.GetName(), req.GetAttemptId())
	if err != nil {
		return nil, err
	}
	if err := s.store.Fail(ctx, key, req.GetError()); err != nil {
		return nil, util.Error(err)
	}
	return &queue.FailResponse{}, nil
}
