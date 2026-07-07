package config

import (
	"time"
	
	"github.com/barnowlsnest/go-configlib/v2/pkg/configs"
)

type Config struct {
	LogLevel         string        `name:"log_level" default:"info" usage:"log level for the application"`
	WALDir           string        `name:"wal_dir" usage:"wal segments folder"`
	Host             string        `name:"host" default:"0.0.0.0" usage:"host to listen on"`
	DedupWindow      time.Duration `name:"dedup_window" default:"1m" usage:"deduplication window"`
	MaxBatchReadSize int           `name:"max_batch_read_size" default:"1024" usage:"maximum number of records to read in a single batch"`
	CacheSize        int           `name:"cache_size" default:"4096" usage:"preloader LRU capacity"`
	Port             int           `name:"port" default:"8000" usage:"port to listen on"`
}

func Load() (*Config, error) {
	var cfg Config
	_, err := configs.Resolve(&cfg, "")
	if err != nil {
		return nil, err
	}
	
	return &cfg, nil
}
