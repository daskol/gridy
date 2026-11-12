// Entrypoint to a gridy coordination server that manages state of distributed
// sweeping.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/daskol/gridy/pkg/cli"
)

func main() {
	ctx := context.Background()
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	cmd := cli.NewCommand(os.Stdout, os.Stderr)
	err := cmd.ExecuteContext(ctx)
	switch {
	case err == nil:
		fallthrough
	case errors.Is(err, context.Canceled):
		// Ok.
	default:
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
