// Package queue implements the worker queue service.
package queue

import (
	"context"

	"github.com/daskol/gridy/pkg/api/queue/v1"
	"github.com/daskol/gridy/pkg/storage"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
)

// QueueService handles worker requests.
type QueueService struct {
	queue.UnimplementedQueueServiceServer
	store storage.Storage
}

// New creates a queue service.
func New(store storage.Storage) *QueueService {
	return &QueueService{store: store}
}

// Register registers the gRPC service.
func (s *QueueService) Register(sr grpc.ServiceRegistrar) {
	queue.RegisterQueueServiceServer(sr, s)
}

// RegisterHTTP registers the HTTP handlers.
func (s *QueueService) RegisterHTTP(ctx context.Context, mux *runtime.ServeMux) error {
	return queue.RegisterQueueServiceHandlerServer(ctx, mux, s)
}
