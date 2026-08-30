package commands

import (
	"github.com/barnowlsnest/go-logslib/v2/pkg/logger"
	"github.com/barnowlsnest/go-logslib/v2/pkg/sharedlog"
	"github.com/barnowlsnest/stratus/pkg/stratusv1"
	"github.com/spf13/cobra"
)

func NewDelete(client *stratusv1.Client) (*cobra.Command, error) {
	deleteCmd := &cobra.Command{
		Use:     "delete",
		Short:   "Delete a record from the stream",
		Aliases: []string{"d", "del"},
		Args:    cobra.NoArgs,
		RunE:    runDeleteFunc(client),
	}

	deleteCmd.Flags().Uint64P(flagEndID, "e", 0, "id of the record to truncate the stream to")
	if err := deleteCmd.MarkFlagRequired(flagEndID); err != nil {
		return nil, err
	}

	return deleteCmd, nil
}

func runDeleteFunc(client *stratusv1.Client) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		if err := ctx.Err(); err != nil {
			return err
		}

		endID, err := cmd.Flags().GetUint64(flagEndID)
		if err != nil {
			return err
		}

		res, err := client.Delete(ctx, endID)
		if err != nil {
			return err
		}

		sharedlog.Info("deleted",
			logger.Uint64Field("start", res.DeletedRecords.Start),
			logger.Uint64Field("end", res.DeletedRecords.End),
		)

		return nil
	}
}
