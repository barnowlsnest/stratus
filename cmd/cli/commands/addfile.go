package commands

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/barnowlsnest/go-logslib/v2/pkg/logger"
	"github.com/barnowlsnest/go-logslib/v2/pkg/sharedlog"
	"github.com/barnowlsnest/stratus/pkg/stratusv1"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

type AddRequestDTO struct {
	Records []InputRecordDTO `json:"input" yaml:"input"`
}

type InputRecordDTO struct {
	DedupKey uint64          `json:"dedup_key" yaml:"dedup_key"`
	RawData  json.RawMessage `json:"raw_data" yaml:"raw_data"`
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

		json.NewDecoder(f)

		defer func() { _ = f.Close() }()

		var input *AddRequestDTO
		ext := filepath.Ext(path)
		switch ext {
		case ".json":
			input, err = fromJSON(f)
			if err != nil {
				return fmt.Errorf("failed to parse JSON file %s: %w", path, err)
			}
		case "yaml", ".yml":
			input, err = fromYAML(f)
			if err != nil {
				return fmt.Errorf("failed to parse YAML file %s: %w", path, err)
			}
		case ".csv":
			input, err = fromCSV(f)
			if err != nil {
				return fmt.Errorf("failed to parse CSV file %s: %w", path, err)
			}
		default:
			return fmt.Errorf("unsupported file format: %s", ext)
		}

		res, err := client.Add(ctx, toInputRecords(input))
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

func fromJSON(f *os.File) (*AddRequestDTO, error) {
	var dto AddRequestDTO
	d := json.NewDecoder(f)
	if err := d.Decode(&dto); err != nil {
		return nil, fmt.Errorf("failed to decode json file %s: %w", f.Name(), err)
	}

	return &dto, nil
}

func fromYAML(f *os.File) (*AddRequestDTO, error) {
	var dto AddRequestDTO
	d := yaml.NewDecoder(f)
	if err := d.Decode(&dto); err != nil {
		return nil, fmt.Errorf("failed to decode yaml file %s: %w", f.Name(), err)
	}

	return &dto, nil
}

func fromCSV(f *os.File) (*AddRequestDTO, error) {
	r := csv.NewReader(f)
	records, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read csv file %s: %w", f.Name(), err)
	}

	dto := &AddRequestDTO{
		Records: make([]InputRecordDTO, len(records)),
	}
	for i, record := range records {
		if len(record) < 2 {
			return nil, fmt.Errorf("invalid csv record format: %v", record)
		}

		keyCell, dataCell := record[0], record[1]
		key, err := strconv.ParseUint(keyCell, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid dedup key value in csv record: %v", record)
		}

		dto.Records[i] = InputRecordDTO{
			DedupKey: key,
			RawData:  json.RawMessage(dataCell),
		}
	}

	return dto, nil
}

func toInputRecords(input *AddRequestDTO) []stratusv1.InputRecord {
	records := make([]stratusv1.InputRecord, len(input.Records))
	for i, r := range input.Records {
		records[i] = stratusv1.InputRecord{
			DedupKey: r.DedupKey,
			RawData:  r.RawData,
		}
	}

	return records
}
