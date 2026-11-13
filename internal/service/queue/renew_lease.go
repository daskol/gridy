package queue

import (
	"context"

	"github.com/daskol/gridy/pkg/api/queue/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RenewLease is not implemented yet.
func (s *QueueService) RenewLease(context.Context, *queue.RenewLeaseRequest) (*queue.RenewLeaseResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method RenewLease not implemented")
}
