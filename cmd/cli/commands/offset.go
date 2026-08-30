package commands

import (
	"time"

	"github.com/barnowlsnest/go-logslib/v2/pkg/logger"
	"github.com/barnowlsnest/go-logslib/v2/pkg/sharedlog"
	"github.com/barnowlsnest/stratus/pkg/stratusv1"
	"github.com/spf13/cobra"
)

func NewOffset(client *stratusv1.Client) (*cobra.Command, error) {
	tailCmd := &cobra.Command{
		Use:     "offset",
		Short:   "Tail the stream",
		Aliases: []string{},
		Args:    cobra.NoArgs,
		RunE:    runOffsetFunc(client),
	}

	tailCmd.Flags().Uint64P(flagStartID, "s", 0, "id of the first record to read")
	tailCmd.Flags().Uint64P(flagMaxRecords, "m", 0, "maximum number of records to read before exiting")
	tailCmd.Flags().Duration(flagReadTimeout, 5*time.Second, "maximum duration to wait for new records before exiting")
	if err := tailCmd.MarkFlagRequired(flagStartID); err != nil {
		return nil, err
	}
	if err := tailCmd.MarkFlagRequired(flagMaxRecords); err != nil {
		return nil, err
	}

	return tailCmd, nil
}

func runOffsetFunc(client *stratusv1.Client) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		if err := ctx.Err(); err != nil {
			return err
		}

		startID, err := cmd.Flags().GetUint64(flagStartID)
		if err != nil {
			return err
		}

		maxRecords, err := cmd.Flags().GetUint64(flagMaxRecords)
		if err != nil {
			return err
		}

		t, err := cmd.Flags().GetDuration(flagReadTimeout)
		if err != nil {
			return err
		}

		output, err := client.ReadOffset(ctx, startID, maxRecords, t)
		if err != nil {
			return err
		}

		done := make(chan struct{})
		go func() {
			defer close(done)
			for r := range output {
				sharedlog.Info("record",
					logger.Uint64Field("id", r.ID),
					logger.StringField("data", string(r.RawData)),
				)
			}
		}()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			if err := ctx.Err(); err != nil {
				return err
			}

			return nil
		}
	}
}
