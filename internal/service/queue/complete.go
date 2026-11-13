package queue

import (
	"context"

	"github.com/daskol/gridy/pkg/api/queue/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Complete is not implemented yet.
func (s *QueueService) Complete(context.Context, *queue.CompleteRequest) (*queue.CompleteResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method Complete not implemented")
}
