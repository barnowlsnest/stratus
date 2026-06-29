package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"
	
	"github.com/barnowlsnest/go-configlib/v2/pkg/configs"
	"github.com/barnowlsnest/go-wallib/pkg/wal"
	"github.com/barnowlsnest/stratus/internal/dedup"
	"github.com/barnowlsnest/stratus/internal/ingester"
	"github.com/barnowlsnest/stratus/internal/preloader"
	"github.com/barnowlsnest/stratus/internal/storage"
	"github.com/barnowlsnest/stratus/internal/stream"
	"golang.org/x/sync/errgroup"
)

type StratusConfig struct {
	LogLevel         string        `name:"log_level" default:"info" usage:"log level for the application"`
	WALDir           string        `name:"wal_dir" usage:"wal segments folder"`
	DedupTTL         time.Duration `name:"dedup_ttl" default:"1m" usage:"deduplication window"`
	MaxBatchReadSize int           `name:"max_batch_read_size" default:"1024" usage:"maximum number of records to read in a single batch"`
	Port             int           `name:"port" usage:"port to listen on"`
	Host             string        `name:"host" default:"0.0.0.0" usage:"host to listen on"`
}

func main() {
	var cfg StratusConfig
	_, err := configs.Resolve(&cfg, "stratus")
	if err != nil {
		log.Fatalf("failed to resolve config: %v", err)
	}
	
	w, r, err := wal.Open(cfg.WALDir,
		wal.WithBatchSize(8),
	)
	if err != nil {
		log.Fatalf("failed to open WAL: %v", err)
	}
	
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	
	d := dedup.New(cfg.DedupTTL)
	store, err := storage.New(
		storage.WithWAL(w),
		storage.WithDeduplicator(d),
		storage.WithMaxReadBatchSize(cfg.MaxBatchReadSize),
	)
	if err != nil {
		_ = w.Close()
		log.Fatalf("failed to create storage: %v", err)
	}
	
	in, err := ingester.New(ingester.WithStorage(store))
	if err != nil {
		_ = w.Close()
		log.Fatalf("failed to create ingester: %v", err)
	}
	
	pre, err := preloader.New(
		preloader.WithStorage(store),
	)
	if err != nil {
		_ = w.Close()
		log.Fatalf("failed to create preloader: %v", err)
	}
	defer func() { pre.Stop() }()
	
	var preloadGroup errgroup.Group
	preloadGroup.Go(func() error {
		return pre.Start(ctx, r.FirstLSN, r.LastLSN)
	})
	preloadGroup.Go(func() error {
		return pre.WaitStarted(ctx, 5*time.Second)
	})
	
	if err := preloadGroup.Wait(); err != nil {
		_ = w.Close()
		log.Fatalf("failed to start preloader: %v", err)
	}
	
	cache, err := pre.Cache()
	if err != nil {
		_ = w.Close()
		log.Fatalf("failed to create cache: %v", err)
	}
	
	str, err := stream.New(
		stream.WithCache(cache),
		stream.WithIngester(in),
	)
	if err != nil {
		_ = w.Close()
		log.Fatalf("failed to create stream: %v", err)
	}
	
}
