package cli

import (
	"io"

	"github.com/spf13/cobra"
)

func NewCommand(stdout, stderr io.Writer) *cobra.Command {
	cmd := cobra.Command{
		Use:           "gridy",
		Short:         "A sweep coordinator and execution service.",
		Args:          cobra.NoArgs,
		SilenceErrors: true,
		SilenceUsage:  true,
	}

	cmd.SetOut(stdout)
	cmd.SetErr(stderr)
	cmd.AddCommand(NewServeCommand())
	return &cmd
}
