package commands

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/barnowlsnest/go-logslib/v2/pkg/logger"
	"github.com/barnowlsnest/go-logslib/v2/pkg/sharedlog"
	"github.com/barnowlsnest/stratus/pkg/stratusv1"
	"github.com/spf13/cobra"
)

type AddRequestDTO struct {
	Records []InputRecordDTO `json:"input"`
}

type InputRecordDTO struct {
	DedupKey uint64          `json:"dedup_key"`
	RawData  json.RawMessage `json:"raw_data"`
}

func NewAddFile(client *stratusv1.Client) (*cobra.Command, error) {
	addFileCmd := &cobra.Command{
		Use:     "addfile",
		Short:   "Add a record to the stream from a file",
		Aliases: []string{"af"},
		Args:    cobra.NoArgs,
		RunE:    runAddFileFunc(client),
	}

	addFileCmd.Flags().StringP(flagInputFile, "f", "", "file to read data from")
	if err := addFileCmd.MarkFlagRequired(flagInputFile); err != nil {
		return nil, err
	}

	if err := addFileCmd.MarkFlagFilename(flagInputFile, "json", "csv"); err != nil {
		return nil, err
	}

	return addFileCmd, nil
}

func runAddFileFunc(client *stratusv1.Client) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		if err := ctx.Err(); err != nil {
			return err
		}

		path, err := cmd.Flags().GetString(flagInputFile)
		if err != nil {
			return err
		}

		path = filepath.Clean(path)
		path, err = filepath.Abs(path)
		if err != nil {
			return err
		}

		f, err := os.OpenFile(path, os.O_RDONLY, 0644)
		if err != nil {
			return fmt.Errorf("failed to open file %s: %w", path, err)
		}

		defer func() { _ = f.Close() }()

		data, err := io.ReadAll(f)
		if err != nil {
			return fmt.Errorf("failed to read file %s: %w", path, err)
		}

		var input AddRequestDTO
		if err := json.Unmarshal(data, &input); err != nil {
			return fmt.Errorf("failed to unmarshal file %s: %w", path, err)
		}

		req := make([]stratusv1.InputRecord, len(input.Records))
		for i, r := range input.Records {
			req[i] = stratusv1.InputRecord{
				DedupKey: r.DedupKey,
				RawData:  r.RawData,
			}
		}

		res, err := client.Add(ctx, req)
		if err != nil {
			return err
		}

		sharedlog.Info("added records to the stream from the file "+path,
			logger.Uint64Field("duplicates", res.DuplicateRecords),
			logger.Uint64Field("start", res.AddedRecords.Start),
			logger.Uint64Field("end", res.AddedRecords.End),
		)

		return nil
	}
}
