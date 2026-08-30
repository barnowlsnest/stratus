package commands

import (
	"github.com/barnowlsnest/go-logslib/v2/pkg/logger"
	"github.com/barnowlsnest/go-logslib/v2/pkg/sharedlog"
	"github.com/spf13/cobra"

	"github.com/barnowlsnest/stratus/pkg/stratusv1"
)

func NewInfo(client *stratusv1.Client) *cobra.Command {
	return &cobra.Command{
		Use:     "info",
		Short:   "Info about the stream",
		Aliases: []string{"i"},
		Args:    cobra.NoArgs,
		RunE:    runInfoFunc(client),
	}
}

func runInfoFunc(client *stratusv1.Client) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		if err := ctx.Err(); err != nil {
			return err
		}

		res, err := client.GetStreamInfo(ctx)
		if err != nil {
			return err
		}

		sharedlog.Info("stream info",
			logger.Uint64Field("start", res.Range.Start),
			logger.Uint64Field("end", res.Range.End),
			logger.Uint64Field("in_memory", res.CachedRecords),
			logger.Uint64Field("on_disk", res.FSRecords),
		)

		return nil
	}
}
