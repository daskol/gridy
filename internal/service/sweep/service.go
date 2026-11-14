// Package sweep implements the sweep service.
package sweep

import (
	"context"

	sweep_api "github.com/daskol/gridy/pkg/api/sweep/v1"
	"github.com/daskol/gridy/pkg/api/sweep/v1/sweepmcp"
	"github.com/daskol/gridy/pkg/storage"
	gw_runtime "github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	mcp_runtime "github.com/redpanda-data/protoc-gen-go-mcp/pkg/runtime"
	"google.golang.org/grpc"
)

// SweepService serves sweep queries.
type SweepService struct {
	sweep_api.UnimplementedSweepServiceServer
	store storage.Storage
}

// New creates a sweep service.
func New(store storage.Storage) *SweepService {
	return &SweepService{store: store}
}

// Register registers the gRPC service.
func (s *SweepService) Register(sr grpc.ServiceRegistrar) {
	sweep_api.RegisterSweepServiceServer(sr, s)
}

// RegisterHTTP registers the HTTP handlers.
func (s *SweepService) RegisterHTTP(ctx context.Context, mux *gw_runtime.ServeMux) error {
	return sweep_api.RegisterSweepServiceHandlerServer(ctx, mux, s)
}

// RegisterMCP registers the MCP tools.
func (s *SweepService) RegisterMCP(sr mcp_runtime.MCPServer) {
	sweepmcp.RegisterSweepServiceHandler(sr, s)
}
