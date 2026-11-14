package cli

import (
	"context"
	"errors"
	"fmt"

	"github.com/daskol/gridy/internal/searchspace"
	"github.com/daskol/gridy/internal/service"
	queueservice "github.com/daskol/gridy/internal/service/queue"
	sweepservice "github.com/daskol/gridy/internal/service/sweep"
	"github.com/daskol/gridy/pkg/storage"
	"github.com/spf13/cobra"
)

const (
	defaultDSN        = "gridy.db"
	defaultGRPCListen = "127.0.0.1:8081"
	defaultHTTPListen = "127.0.0.1:8080"
)

type ServeConfig struct {
	DSN              string
	GRPCListen       string
	HTTPListen       string
	SearchSpaceFiles []string
}

func NewServeCommand() *cobra.Command {
	config := &ServeConfig{}
	cmd := cobra.Command{
		Use:   "serve search-space.toml [search-space.toml...]",
		Short: "Run the gRPC, HTTP/JSON, and MCP servers.",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			config.SearchSpaceFiles = args
			return Serve(cmd.Context(), config)
		},
	}

	fs := cmd.Flags()
	fs.StringVar(&config.DSN, "dsn", defaultDSN, "Path to the bbolt database file")
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

	if len(c.SearchSpaceFiles) == 0 {
		return errors.New("at least one search-space TOML file is required")
	}

	imports, err := searchspace.LoadFiles(ctx, c.SearchSpaceFiles)
	if err != nil {
		return err
	}
	store, err := storage.Open(c.DSN, nil)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer store.Close()

	if err := store.ImportSweeps(ctx, imports); err != nil {
		return fmt.Errorf("load search spaces: %w", err)
	}

	queueService := queueservice.New(store)
	sweepService := sweepservice.New(store)

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
