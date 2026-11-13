// Package service runs the gRPC, HTTP and MCP servers.
package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	mcpruntime "github.com/redpanda-data/protoc-gen-go-mcp/pkg/runtime"
	"github.com/redpanda-data/protoc-gen-go-mcp/pkg/runtime/gosdk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

const shutdownTimeout = 10 * time.Second

// Config sets the listener addresses.
type Config struct {
	GRPCListen string
	HTTPListen string
}

// GRPCService registers gRPC handlers.
type GRPCService interface {
	Register(grpc.ServiceRegistrar)
}

// HTTPService registers HTTP handlers.
type HTTPService interface {
	RegisterHTTP(context.Context, *runtime.ServeMux) error
}

// MCPService registers MCP tools.
type MCPService interface {
	RegisterMCP(mcpruntime.MCPServer)
}

// App serves registered services over shared transports.
type App struct {
	config       Config
	grpcServer   *grpc.Server
	healthServer *health.Server
	httpServer   *http.Server
	httpMux      *http.ServeMux
	gateway      *runtime.ServeMux
	mcpServer    *mcp.Server
	mcpRegistrar mcpruntime.MCPServer
}

// New prepares the servers without opening listeners.
func New(config Config) *App {
	logger := slog.Default()
	mcpServer, mcpRegistrar := gosdk.NewServer("gridy", "dev")
	mcpServer.AddReceivingMiddleware(mcpAccessLogger(logger))
	a := &App{
		config: config,
		grpcServer: grpc.NewServer(
			grpc.ChainUnaryInterceptor(grpcUnaryLogger(logger)),
			grpc.ChainStreamInterceptor(grpcStreamLogger(logger)),
		),
		healthServer: health.NewServer(),
		httpMux:      http.NewServeMux(),
		gateway:      newGatewayMux(),
		mcpServer:    mcpServer,
		mcpRegistrar: mcpRegistrar,
	}
	healthpb.RegisterHealthServer(a.grpcServer, a.healthServer)

	a.httpMux.Handle("/", a.gateway)
	a.httpMux.Handle("/mcp", mcp.NewStreamableHTTPHandler(
		func(*http.Request) *mcp.Server { return a.mcpServer },
		&mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true},
	))
	a.httpMux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("Ok.\n"))
	})
	a.httpServer = &http.Server{
		Handler:           httpAccessLogger(logger, a.httpMux),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       time.Minute,
	}
	return a
}

// RegisterGRPC adds services to the gRPC server.
func (a *App) RegisterGRPC(services ...GRPCService) {
	for _, s := range services {
		s.Register(a.grpcServer)
	}
}

// RegisterHTTP adds services to the HTTP gateway.
func (a *App) RegisterHTTP(ctx context.Context, services ...HTTPService) error {
	for _, s := range services {
		if err := s.RegisterHTTP(ctx, a.gateway); err != nil {
			return fmt.Errorf("register HTTP handlers: %w", err)
		}
	}
	return nil
}

// RegisterMCP adds tools to the MCP server.
func (a *App) RegisterMCP(services ...MCPService) {
	for _, s := range services {
		s.RegisterMCP(a.mcpRegistrar)
	}
}

// Run serves requests until cancellation or a server error.
func (a *App) Run(ctx context.Context) error {
	if ctx.Err() != nil {
		return nil
	}

	// Bind both endpoints before serving either one. A failed second bind must
	// release the first listener including any Unix socket it created.
	grpcListener, err := listen(ctx, a.config.GRPCListen)
	if err != nil {
		return fmt.Errorf("listen for gRPC on %q: %w", a.config.GRPCListen, err)
	}
	defer grpcListener.Close()

	httpListener, err := listen(ctx, a.config.HTTPListen)
	if err != nil {
		return fmt.Errorf("listen for HTTP on %q: %w", a.config.HTTPListen, err)
	}
	defer httpListener.Close()

	defer a.grpcServer.Stop()

	a.healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	for name := range a.grpcServer.GetServiceInfo() {
		a.healthServer.SetServingStatus(name, healthpb.HealthCheckResponse_SERVING)
	}

	// Keep in-flight HTTP requests alive while Shutdown drains them. Cancel
	// their base context only after draining or exhausting the shutdown
	// budget.
	httpContext, cancelHTTP := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelHTTP()

	a.httpServer.BaseContext = func(net.Listener) context.Context { return httpContext }
	defer a.httpServer.Close()

	results := make(chan error, 2)
	go func() {
		err := a.grpcServer.Serve(grpcListener)
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			err = fmt.Errorf("serve gRPC: %w", err)
		} else {
			err = nil
		}
		results <- err
	}()

	go func() {
		err := a.httpServer.Serve(httpListener)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			err = fmt.Errorf("serve HTTP: %w", err)
		} else {
			err = nil
		}
		results <- err
	}()

	slog.InfoContext(
		ctx, "gridy is listening",
		"grpc", grpcListener.Addr().String(),
		"http", httpListener.Addr().String())

	return a.wait(ctx, results, cancelHTTP)
}

func (a *App) wait(
	ctx context.Context,
	results <-chan error,
	cancelHTTP context.CancelFunc,
) error {
	// Wait until context is done or a server exits.
	var serveErr error
	pending := 2
	select {
	case <-ctx.Done():
	case serveErr = <-results:
		pending--
	}

	a.healthServer.Shutdown()
	shutdownContext := context.WithoutCancel(ctx)
	shutdownContext, cancelShutdown := context.WithTimeout(shutdownContext, shutdownTimeout)
	defer cancelShutdown()

	grpcStopped := make(chan struct{})
	go func() {
		a.grpcServer.GracefulStop()
		close(grpcStopped)
	}()

	if err := a.httpServer.Shutdown(shutdownContext); err != nil {
		serveErr = errors.Join(serveErr, fmt.Errorf("shutdown: http: %w", err))
		cancelHTTP()
		_ = a.httpServer.Close()
	}

	select {
	case <-grpcStopped:
	case <-shutdownContext.Done():
		a.grpcServer.Stop()
		<-grpcStopped
		shutdownErr := fmt.Errorf("shutdown: grpc: %w", shutdownContext.Err())
		serveErr = errors.Join(serveErr, shutdownErr)
	}

	for range pending {
		serveErr = errors.Join(serveErr, <-results)
	}
	return serveErr
}

func newGatewayMux() *runtime.ServeMux {
	httpMarshaler := &runtime.HTTPBodyMarshaler{
		Marshaler: &runtime.JSONPb{
			UseProtoNames:   true,
			EmitUnpopulated: true,
			DiscardUnknown:  true,
		},
	}
	return runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, httpMarshaler),
	)
}

func listen(ctx context.Context, address string) (net.Listener, error) {
	network := "tcp"
	if path, ok := strings.CutPrefix(address, "unix://"); ok {
		if !filepath.IsAbs(path) {
			return nil, fmt.Errorf("serve: unix-socket must be absolute path: %q", path)
		}
		network, address = "unix", path
	} else if address == "" {
		return nil, errors.New("listen address must not be empty")
	}
	var config net.ListenConfig
	return config.Listen(ctx, network, address)
}
