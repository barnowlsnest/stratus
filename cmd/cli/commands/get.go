package commands

import (
	"time"

	"github.com/barnowlsnest/go-logslib/v2/pkg/logger"
	"github.com/barnowlsnest/go-logslib/v2/pkg/sharedlog"
	"github.com/spf13/cobra"

	"github.com/barnowlsnest/stratus/pkg/stratusv1"
)

func NewGet(client *stratusv1.Client) (*cobra.Command, error) {
	getCmd := &cobra.Command{
		Use:     "get",
		Short:   "Get a record from the stream",
		Aliases: []string{"g", "read", "range"},
		Args:    cobra.NoArgs,
		RunE:    runGetFunc(client),
	}

	getCmd.Flags().Uint64P(flagStartID, "s", 0, "id of the first record to read")
	getCmd.Flags().Uint64P(flagEndID, "e", 0, "id of the last record to read")
	getCmd.Flags().Duration(flagReadTimeout, 5*time.Second, "timeout for reading new records")

	if err := getCmd.MarkFlagRequired(flagStartID); err != nil {
		return nil, err
	}
	if err := getCmd.MarkFlagRequired(flagEndID); err != nil {
		return nil, err
	}

	return getCmd, nil
}

func runGetFunc(client *stratusv1.Client) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		if err := ctx.Err(); err != nil {
			return err
		}

		startID, err := cmd.Flags().GetUint64(flagStartID)
		if err != nil {
			return err
		}

		endID, err := cmd.Flags().GetUint64(flagEndID)
		if err != nil {
			return err
		}

		t, err := cmd.Flags().GetDuration(flagReadTimeout)
		if err != nil {
			return err
		}

		records, err := client.ReadRange(ctx, startID, endID, t)
		if err != nil {
			return err
		}

		for _, record := range records {
			sharedlog.Info("record",
				logger.Uint64Field("id", record.ID),
				logger.StringField("data", string(record.RawData)),
			)
		}

		return nil
	}
}
