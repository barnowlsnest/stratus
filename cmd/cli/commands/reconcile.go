package commands

import (
	"github.com/barnowlsnest/go-logslib/v2/pkg/logger"
	"github.com/barnowlsnest/go-logslib/v2/pkg/sharedlog"
	"github.com/spf13/cobra"

	"github.com/barnowlsnest/stratus/pkg/stratusv1"
)

func NewReconcileCache(client *stratusv1.Client) *cobra.Command {
	return &cobra.Command{
		Use:     "reconcile",
		Short:   "Reconcile the cache with the stream",
		Aliases: []string{"re", "rec"},
		Args:    cobra.NoArgs,
		RunE:    runReconcileFunc(client),
	}
}

func runReconcileFunc(client *stratusv1.Client) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		if err := ctx.Err(); err != nil {
			return err
		}

		res, err := client.ReconcileCache(ctx)
		if err != nil {
			return err
		}

		sharedlog.Info("im-memory cache reconciled",
			logger.Uint64Field("start", res.Start),
			logger.Uint64Field("end", res.End),
		)

		return nil
	}
}
