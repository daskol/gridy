package queue

import (
	"context"

	"github.com/daskol/gridy/internal/service/util"
	"github.com/daskol/gridy/pkg/api/queue/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// Complete durably records the result of an active attempt.
func (s *QueueService) Complete(ctx context.Context, req *queue.CompleteRequest) (*queue.CompleteResponse, error) {
	key, err := util.AttemptKey(req.GetName(), req.GetAttemptId())
	if err != nil {
		return nil, err
	}
	if req.GetResult() == nil {
		return nil, status.Error(codes.InvalidArgument, "result is required")
	}
	result, err := protojson.Marshal(req.GetResult())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid result JSON")
	}
	if err := s.store.Complete(ctx, key, result); err != nil {
		return nil, util.Error(err)
	}
	return &queue.CompleteResponse{}, nil
}
