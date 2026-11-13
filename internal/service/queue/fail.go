package queue

import (
	"context"

	"github.com/daskol/gridy/pkg/api/queue/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Fail is not implemented yet.
func (s *QueueService) Fail(context.Context, *queue.FailRequest) (*queue.FailResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method Fail not implemented")
}
