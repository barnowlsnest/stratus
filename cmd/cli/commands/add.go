package commands

import (
	"github.com/barnowlsnest/go-logslib/v2/pkg/logger"
	"github.com/barnowlsnest/go-logslib/v2/pkg/sharedlog"
	"github.com/spf13/cobra"

	"github.com/barnowlsnest/stratus/pkg/stratusv1"
)

func NewAdd(client *stratusv1.Client) (*cobra.Command, error) {
	addCmd := &cobra.Command{
		Use:     "add",
		Short:   "Append a record to the stream",
		Aliases: []string{"a", "append"},
		Args:    cobra.NoArgs,
		RunE:    runAddFunc(client),
	}

	addCmd.Flags().Uint64P(flagKey, "k", 0, "dedup key")
	addCmd.Flags().StringP(flagData, "d", "", "data to add")

	if err := addCmd.MarkFlagRequired(flagKey); err != nil {
		return nil, err
	}
	if err := addCmd.MarkFlagRequired(flagData); err != nil {
		return nil, err
	}

	return addCmd, nil
}

func runAddFunc(client *stratusv1.Client) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		if err := ctx.Err(); err != nil {
			return err
		}

		key, err := cmd.Flags().GetUint64(flagKey)
		if err != nil {
			return err
		}

		data, err := cmd.Flags().GetString(flagData)
		if err != nil {
			return err
		}

		records := []stratusv1.InputRecord{{DedupKey: key, RawData: []byte(data)}}

		res, err := client.Add(ctx, records)
		if err != nil {
			return err
		}

		sharedlog.Info("added record to the stream",
			logger.Uint64Field("id", res.AddedRecords.End),
		)

		return nil
	}
}
