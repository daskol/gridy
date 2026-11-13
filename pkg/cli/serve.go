package cli

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

	"github.com/daskol/gridy/pkg/api/queue/v1"
	"github.com/daskol/gridy/pkg/api/sweep/v1"
	"github.com/daskol/gridy/pkg/api/sweep/v1/sweepmcp"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/redpanda-data/protoc-gen-go-mcp/pkg/runtime/gosdk"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	defaultDSN        = `TODO`
	defaultGRPCListen = "127.0.0.1:8081"
	defaultHTTPListen = "127.0.0.1:8080"
	shutdownTimeout   = 10 * time.Second
)

type ServeConfig struct {
	DSN        string
	GRPCListen string
	HTTPListen string
}

func NewServeCommand() *cobra.Command {
	config := &ServeConfig{}
	cmd := cobra.Command{
		Use:   "serve",
		Short: "Run the gRPC, HTTP/JSON, and MCP servers.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return Serve(cmd.Context(), config)
		},
	}

	fs := cmd.Flags()
	fs.StringVar(&config.DSN, "dsn", defaultDSN, "Database source name")
	fs.StringVar(&config.GRPCListen, "grpc-listen", defaultGRPCListen, "gRPC listen address")
	fs.StringVar(&config.HTTPListen, "http-listen", defaultHTTPListen, "HTTP listen address")
	return &cmd
}

func Serve(ctx context.Context, c *ServeConfig) error {
	if c == nil {
		return errors.New("serve configuration is required")
	}
	if ctx.Err() != nil {
		return nil
	}

	// Bind both endpoints before serving either one. A failed second bind must
	// release the first listener including any Unix socket it created.
	grpcListener, err := listen(ctx, c.GRPCListen)
	if err != nil {
		return fmt.Errorf("listen for gRPC on %q: %w", c.GRPCListen, err)
	}
	defer grpcListener.Close()

	httpListener, err := listen(ctx, c.HTTPListen)
	if err != nil {
		return fmt.Errorf("listen for HTTP on %q: %w", c.HTTPListen, err)
	}
	defer httpListener.Close()

	// Until the coordinator is implemented, generated handlers return
	// codes.Unimplemented consistently across all three transports.
	queueService := queue.UnimplementedQueueServiceServer{}
	sweepService := sweep.UnimplementedSweepServiceServer{}
	grpcServer := grpc.NewServer()
	queue.RegisterQueueServiceServer(grpcServer, queueService)
	sweep.RegisterSweepServiceServer(grpcServer, sweepService)
	defer grpcServer.Stop()

	healthServer := health.NewServer()
	for _, service := range []string{"", queue.QueueService_ServiceDesc.ServiceName, sweep.SweepService_ServiceDesc.ServiceName} {
		healthServer.SetServingStatus(service, healthpb.HealthCheckResponse_SERVING)
	}
	healthpb.RegisterHealthServer(grpcServer, healthServer)

	// Keep in-flight HTTP requests alive while Shutdown drains them. Cancel
	// their base context only after draining or exhausting the shutdown budget.
	httpContext, cancelHTTP := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelHTTP()

	gateway := newGatewayMux()
	if err := queue.RegisterQueueServiceHandlerServer(httpContext, gateway, queueService); err != nil {
		return fmt.Errorf("register queue HTTP handlers: %w", err)
	}
	if err := sweep.RegisterSweepServiceHandlerServer(httpContext, gateway, sweepService); err != nil {
		return fmt.Errorf("register sweep HTTP handlers: %w", err)
	}

	mcpServer, mcpAdapter := gosdk.NewServer("gridy", "dev")
	sweepmcp.RegisterSweepServiceHandler(mcpAdapter, sweepService)

	mux := http.NewServeMux()
	mux.Handle("/", gateway)
	mux.Handle("/mcp", mcp.NewStreamableHTTPHandler(
		func(*http.Request) *mcp.Server { return mcpServer },
		&mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true},
	))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})

	httpServer := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       time.Minute,
		BaseContext:       func(net.Listener) context.Context { return httpContext },
	}
	defer httpServer.Close()

	results := make(chan error, 2)
	go func() {
		err := grpcServer.Serve(grpcListener)
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			err = fmt.Errorf("serve gRPC: %w", err)
		} else {
			err = nil
		}
		results <- err
	}()

	go func() {
		err := httpServer.Serve(httpListener)
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

	var serveErr error
	pending := 2
	select {
	case <-ctx.Done():
	case serveErr = <-results:
		pending--
	}

	healthServer.Shutdown()
	shutdownContext, cancelShutdown := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancelShutdown()

	grpcStopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(grpcStopped)
	}()

	if err := httpServer.Shutdown(shutdownContext); err != nil {
		serveErr = errors.Join(serveErr, fmt.Errorf("shut down HTTP: %w", err))
		cancelHTTP()
		_ = httpServer.Close()
	}

	select {
	case <-grpcStopped:
	case <-shutdownContext.Done():
		grpcServer.Stop()
		<-grpcStopped
		serveErr = errors.Join(serveErr, fmt.Errorf("shut down gRPC: %w", shutdownContext.Err()))
	}
	for range pending {
		serveErr = errors.Join(serveErr, <-results)
	}
	return serveErr
}

func newGatewayMux() *runtime.ServeMux {
	return runtime.NewServeMux(runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.HTTPBodyMarshaler{
		Marshaler: &runtime.JSONPb{
			MarshalOptions: protojson.MarshalOptions{
				UseProtoNames:   true,
				EmitUnpopulated: true,
			},
			UnmarshalOptions: protojson.UnmarshalOptions{
				DiscardUnknown: true,
			},
		},
	}))
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
