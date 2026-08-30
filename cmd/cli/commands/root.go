package commands

import (
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func NewRoot(globalFlags *pflag.FlagSet) *cobra.Command {
	rootCmd := &cobra.Command{
		Use:          "stratuscli",
		Short:        "CLI for the stratus stream",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}

	rootCmd.PersistentFlags().AddFlagSet(globalFlags)

	return rootCmd
}
