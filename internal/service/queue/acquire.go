package queue

import (
	"context"
	"fmt"

	"github.com/daskol/gridy/internal/service/storageapi"
	"github.com/daskol/gridy/pkg/api/queue/v1"
	"github.com/daskol/gridy/pkg/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Acquire assigns a trial or reports whether the worker should wait or stop.
func (s *QueueService) Acquire(ctx context.Context, req *queue.AcquireRequest) (*queue.AcquireResponse, error) {
	id, err := storageapi.SweepID(req.GetParent())
	if err != nil {
		return nil, err
	}

	acquisition, err := s.store.Acquire(ctx, storage.AcquireParams{
		SweepID: id, WorkerID: req.GetWorkerId(), RequestID: req.GetRequestId(),
	})
	if err != nil {
		return nil, storageapi.Error(err)
	}

	switch acquisition.State {
	case storage.AcquisitionWait:
		return &queue.AcquireResponse{
			Outcome: &queue.AcquireResponse_Wait{Wait: &emptypb.Empty{}},
		}, nil
	case storage.AcquisitionDone:
		return &queue.AcquireResponse{
			Outcome: &queue.AcquireResponse_Done{Done: &emptypb.Empty{}},
		}, nil
	case storage.AcquisitionAssigned:
		assignment := acquisition.Assignment
		if assignment == nil {
			return nil, status.Error(codes.Internal, "missing stored assignment")
		}
		params, err := storageapi.Object(assignment.Params)
		if err != nil {
			return nil, err
		}
		content := &queue.Assignment{
			Name:      fmt.Sprintf("sweeps/%s/trials/%s", assignment.SweepID, assignment.TrialID),
			AttemptId: assignment.AttemptID, Params: params,
			LeaseExpiresAt: timestamppb.New(assignment.LeaseExpiresAt),
		}
		return &queue.AcquireResponse{
			Outcome: &queue.AcquireResponse_Assignment{Assignment: content},
		}, nil
	default:
		return nil, status.Error(codes.Internal, "invalid stored acquisition outcome")
	}
}
