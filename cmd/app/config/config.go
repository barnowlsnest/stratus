package config

import (
	"errors"
	"time"

	"github.com/barnowlsnest/go-configlib/v2/pkg/configs"
)

var ErrIncompleteTLS = errors.New("config: tls_cert_file and tls_key_file must be set together")

const (
	// DefaultWALBatchSize is the number of records the wal groups into a single fsync batch.
	DefaultWALBatchSize = 16
	// DefaultWALMaxSegmentSize is the size a wal segment may reach before it is rotated.
	DefaultWALMaxSegmentSize = 64 << 20 // 64MB
	// DefaultWALMaxRecordSize is the largest single record the wal accepts.
	DefaultWALMaxRecordSize = 8 << 20 // 8MB
	// DefaultSubscriberBuffer is how many records the WAL follower can queue for
	// the cache-fill loop, which caches all of them before waking tails once.
	DefaultSubscriberBuffer = 1024
	// DefaultMaxConcurrentStreams caps simultaneous gRPC streams (ReadOffset tails).
	DefaultMaxConcurrentStreams = 4096
	// DefaultGRPCKeepaliveTime is how long a connection may be idle before the
	// server pings; without this, NAT/LBs silently drop long-lived ReadOffset streams.
	DefaultGRPCKeepaliveTime = 2 * time.Minute
	// DefaultGRPCKeepaliveTimeout is how long the server waits for a ping ack.
	DefaultGRPCKeepaliveTimeout = 20 * time.Second
	// DefaultGRPCKeepaliveMinTime is the minimum client ping interval the server accepts.
	DefaultGRPCKeepaliveMinTime = 5 * time.Second
)

type Config struct {
	LogLevel          string        `name:"log_level" default:"info" usage:"log level for the application"`
	WALDir            string        `name:"wal_dir" usage:"wal segments folder"`
	Host              string        `name:"host" default:"127.0.0.1" usage:"host to listen on"`
	DedupWindow       time.Duration `name:"dedup_window" default:"1m" usage:"deduplication window"`
	WALMaxSegmentSize int64         `name:"wal_max_segment_size" usage:"maximum wal segment size in bytes before rotation"`
	MaxBatchReadSize  int           `name:"max_batch_read_size" default:"1024" usage:"maximum number of records in one ReadOffset message"`
	MaxReadBytes      int           `name:"max_read_bytes" default:"32000" usage:"maximum encoded bytes in one ReadOffset message"`
	CacheSize         int           `name:"cache_size" default:"4096" usage:"preloader LRU capacity"`
	SubscriberBuffer  int           `name:"subscriber_buffer" default:"1024" usage:"wal follower queue size, and newest records kept for tails"`
	Port              int           `name:"port" default:"8000" usage:"port to listen on"`
	WALBatchSize      int           `name:"wal_batch_size" usage:"number of wal records per fsync batch"`
	WALMaxRecordSize  int           `name:"wal_max_record_size" usage:"maximum wal record size in bytes"`
	AuthToken         string        `name:"auth_token" flag:"-" usage:"bearer token clients must present"`
	TLSCertFile       string        `name:"tls_cert_file" usage:"PEM certificate file"`
	TLSKeyFile        string        `name:"tls_key_file" usage:"PEM private key file"`
	PprofAddr         string        `name:"pprof_addr" usage:"pprof HTTP listen address, e.g. 127.0.0.1:6060; empty disables it"`
	PprofBlockRate    int           `name:"pprof_block_rate" usage:"runtime.SetBlockProfileRate value; 0 disables block profiling"`
	PprofMutexFrac    int           `name:"pprof_mutex_fraction" usage:"runtime.SetMutexProfileFraction value; 0 disables mutex profiling"`
	// MaxConcurrentStreams caps simultaneous gRPC streams (ReadOffset tails).
	MaxConcurrentStreams int `name:"max_concurrent_streams" usage:"max simultaneous gRPC streams"`
	// GRPCKeepaliveTime is idle time before the server pings long-lived streams.
	GRPCKeepaliveTime    time.Duration `name:"grpc_keepalive_time" usage:"idle time before server keepalive ping"`
	GRPCKeepaliveTimeout time.Duration `name:"grpc_keepalive_timeout" usage:"wait for keepalive ping ack"`
	GRPCKeepaliveMinTime time.Duration `name:"grpc_keepalive_min_time" usage:"minimum client ping interval accepted"`
}

func (c *Config) TLSEnabled() bool {
	return c.TLSCertFile != "" && c.TLSKeyFile != ""
}

func (c *Config) Validate() error {
	if (c.TLSCertFile == "") != (c.TLSKeyFile == "") {
		return ErrIncompleteTLS
	}

	return nil
}

func Load() (*Config, error) {
	var cfg Config
	_, err := configs.Resolve(&cfg, "")
	if err != nil {
		return nil, err
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	cfg.applyDefaults()

	return &cfg, nil
}

// applyDefaults replaces unset or non-positive sizes with their defaults.
func (c *Config) applyDefaults() {
	if c.WALBatchSize <= 0 {
		c.WALBatchSize = DefaultWALBatchSize
	}

	if c.WALMaxSegmentSize <= 0 {
		c.WALMaxSegmentSize = DefaultWALMaxSegmentSize
	}

	if c.WALMaxRecordSize <= 0 {
		c.WALMaxRecordSize = DefaultWALMaxRecordSize
	}

	if c.SubscriberBuffer <= 0 {
		c.SubscriberBuffer = DefaultSubscriberBuffer
	}

	if c.MaxConcurrentStreams <= 0 {
		c.MaxConcurrentStreams = DefaultMaxConcurrentStreams
	}

	if c.GRPCKeepaliveTime == 0 {
		c.GRPCKeepaliveTime = DefaultGRPCKeepaliveTime
	}

	if c.GRPCKeepaliveTimeout <= 0 {
		c.GRPCKeepaliveTimeout = DefaultGRPCKeepaliveTimeout
	}

	if c.GRPCKeepaliveMinTime <= 0 {
		c.GRPCKeepaliveMinTime = DefaultGRPCKeepaliveMinTime
	}
}
