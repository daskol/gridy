package service

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/felixge/httpsnoop"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

func httpAccessLogger(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log := logger.With("transport", "http", "method", r.Method, "path", r.URL.Path)
		log.InfoContext(r.Context(), "request started")
		metrics := httpsnoop.CaptureMetrics(next, w, r)
		log.InfoContext(
			r.Context(), "request finished",
			"status", metrics.Code, "duration", metrics.Duration)
	})
}

func grpcUnaryLogger(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		finish := logGRPCRequest(ctx, logger, info.FullMethod)
		resp, err := next(ctx, req)
		finish(err)
		return resp, err
	}
}

func grpcStreamLogger(logger *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, next grpc.StreamHandler) error {
		finish := logGRPCRequest(stream.Context(), logger, info.FullMethod)
		err := next(srv, stream)
		finish(err)
		return err
	}
}

func logGRPCRequest(ctx context.Context, logger *slog.Logger, method string) func(error) {
	log := logger.With("transport", "grpc", "method", method)
	started := time.Now()
	log.InfoContext(ctx, "request started")
	return func(err error) {
		grpcStatus, ok := status.FromError(err)
		if !ok {
			grpcStatus = status.FromContextError(err)
		}

		attrs := []any{
			"code", grpcStatus.Code().String(),
			"duration", time.Since(started),
		}
		if err != nil {
			attrs = append(attrs, "error", err)
		}

		log.InfoContext(ctx, "request finished", attrs...)
	}
}

func mcpAccessLogger(logger *slog.Logger) mcp.Middleware {
	return func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			log := logger.With("transport", "mcp", "method", method)
			if req != nil {
				params, ok := req.GetParams().(*mcp.CallToolParamsRaw)
				if ok && params != nil {
					log = log.With("tool", params.Name)
				}
			}

			started := time.Now()
			log.InfoContext(ctx, "request started")

			result, err := next(ctx, method, req)
			failed := err != nil
			toolResult, ok := result.(*mcp.CallToolResult)
			if ok && toolResult != nil {
				failed = failed || toolResult.IsError
			}

			attrs := []any{"is_error", failed, "duration", time.Since(started)}
			if err != nil {
				attrs = append(attrs, "error", err)
			}

			log.InfoContext(ctx, "request finished", attrs...)
			return result, err
		}
	}
}
