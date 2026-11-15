package queue

import (
	"context"

	"github.com/daskol/gridy/internal/service/storageapi"
	"github.com/daskol/gridy/pkg/api/queue/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// RenewLease extends the lease of an active attempt.
func (s *QueueService) RenewLease(ctx context.Context, req *queue.RenewLeaseRequest) (*queue.RenewLeaseResponse, error) {
	key, err := storageapi.AttemptKey(req.GetName(), req.GetAttemptId())
	if err != nil {
		return nil, err
	}
	expiration, err := s.store.RenewLease(ctx, key)
	if err != nil {
		return nil, storageapi.Error(err)
	}
	return &queue.RenewLeaseResponse{
		LeaseExpiresAt: timestamppb.New(expiration),
	}, nil
}
