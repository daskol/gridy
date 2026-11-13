package queue

import (
	"context"

	"github.com/daskol/gridy/pkg/api/queue/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Acquire is not implemented yet.
func (s *QueueService) Acquire(context.Context, *queue.AcquireRequest) (*queue.AcquireResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method Acquire not implemented")
}
