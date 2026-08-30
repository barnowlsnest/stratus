package config

import (
	"time"

	"github.com/barnowlsnest/go-configlib/v2/pkg/configs"
)

const (
	// DefaultWALBatchSize is the number of records the wal groups into a single fsync batch.
	DefaultWALBatchSize = 16
	// DefaultWALMaxSegmentSize is the size a wal segment may reach before it is rotated.
	DefaultWALMaxSegmentSize = 64 << 20 // 64MB
	// DefaultWALMaxRecordSize is the largest single record the wal accepts.
	DefaultWALMaxRecordSize = 8 << 20 // 8MB
)

type Config struct {
	LogLevel          string        `name:"log_level" default:"info" usage:"log level for the application"`
	WALDir            string        `name:"wal_dir" usage:"wal segments folder"`
	Host              string        `name:"host" default:"127.0.0.1" usage:"host to listen on"`
	DedupWindow       time.Duration `name:"dedup_window" default:"1m" usage:"deduplication window"`
	WALMaxSegmentSize int64         `name:"wal_max_segment_size" usage:"maximum wal segment size in bytes before rotation"`
	MaxBatchReadSize  int           `name:"max_batch_read_size" default:"1024" usage:"maximum number of records to read in a single batch"`
	CacheSize         int           `name:"cache_size" default:"4096" usage:"preloader LRU capacity"`
	Port              int           `name:"port" default:"8000" usage:"port to listen on"`
	WALBatchSize      int           `name:"wal_batch_size" usage:"number of wal records per fsync batch"`
	WALMaxRecordSize  int           `name:"wal_max_record_size" usage:"maximum wal record size in bytes"`
}

func Load() (*Config, error) {
	var cfg Config
	_, err := configs.Resolve(&cfg, "")
	if err != nil {
		return nil, err
	}

	if cfg.WALBatchSize <= 0 {
		cfg.WALBatchSize = DefaultWALBatchSize
	}

	if cfg.WALMaxSegmentSize <= 0 {
		cfg.WALMaxSegmentSize = DefaultWALMaxSegmentSize
	}

	if cfg.WALMaxRecordSize <= 0 {
		cfg.WALMaxRecordSize = DefaultWALMaxRecordSize
	}

	return &cfg, nil
}
