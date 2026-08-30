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
	DedupKey uint64  `json:"dedup_key" yaml:"dedup_key"`
	RawData  RawData `json:"raw_data" yaml:"raw_data"`
}

// RawData holds an opaque record payload. It accepts either a string holding a
// JSON document or an inline JSON/YAML structure, and always carries the JSON
// encoding of whatever was given.
type RawData []byte

func (d RawData) MarshalJSON() ([]byte, error) {
	if len(d) == 0 {
		return []byte("null"), nil
	}

	return d, nil
}

func (d *RawData) UnmarshalJSON(b []byte) error {
	// A JSON string is unwrapped so that `"raw_data": "{}"` and
	// `"raw_data": {}` end up with the same payload.
	if len(b) > 0 && b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}

		*d = RawData(s)

		return nil
	}

	*d = append((*d)[:0], b...)

	return nil
}

func (d *RawData) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind == yaml.ScalarNode && node.Tag == "!!str" {
		*d = RawData(node.Value)

		return nil
	}

	var v any
	if err := node.Decode(&v); err != nil {
		return err
	}

	b, err := json.Marshal(v)
	if err != nil {
		return err
	}

	*d = b

	return nil
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

	if err := addFileCmd.MarkFlagFilename(flagInputFile, "json", "yaml", "yml", "csv"); err != nil {
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

		var input *AddRequestDTO
		ext := filepath.Ext(path)
		switch ext {
		case ".json":
			input, err = fromJSON(f)
			if err != nil {
				return fmt.Errorf("failed to parse JSON file %s: %w", path, err)
			}
		case ".yaml", ".yml":
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
			RawData:  RawData(dataCell),
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
