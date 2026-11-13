package cli

import (
	"context"
	"errors"

	"github.com/daskol/gridy/internal/service"
	queueservice "github.com/daskol/gridy/internal/service/queue"
	sweepservice "github.com/daskol/gridy/internal/service/sweep"
	"github.com/spf13/cobra"
)

const (
	defaultDSN        = `TODO`
	defaultGRPCListen = "127.0.0.1:8081"
	defaultHTTPListen = "127.0.0.1:8080"
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

	queueService := queueservice.New()
	sweepService := sweepservice.New()

	app := service.New(service.Config{
		GRPCListen: c.GRPCListen,
		HTTPListen: c.HTTPListen,
	})
	app.RegisterGRPC(queueService, sweepService)
	if err := app.RegisterHTTP(ctx, queueService, sweepService); err != nil {
		return err
	}
	app.RegisterMCP(sweepService)
	return app.Run(ctx)
}
