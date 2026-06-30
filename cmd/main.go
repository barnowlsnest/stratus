package main

import (
	"context"
	"fmt"
	"log"
	"os/signal"
	"syscall"
	"time"
	
	"github.com/barnowlsnest/go-configlib/v2/pkg/configs"
	"github.com/barnowlsnest/go-datalib/v5/pkg/lru"
	"github.com/barnowlsnest/go-logslib/v2/pkg/sharedlog"
	"github.com/barnowlsnest/go-wallib/pkg/wal"
	"github.com/barnowlsnest/stratus/cmd/server"
	"github.com/barnowlsnest/stratus/internal/dedup"
	"github.com/barnowlsnest/stratus/internal/ingester"
	"github.com/barnowlsnest/stratus/internal/preloader"
	"github.com/barnowlsnest/stratus/internal/storage"
	"github.com/barnowlsnest/stratus/internal/stream"
	"golang.org/x/sync/errgroup"
)

const preCacheStartTimeout = 5 * time.Second

type StratusConfig struct {
	LogLevel         string        `name:"log_level" default:"info" usage:"log level for the application"`
	WALDir           string        `name:"wal_dir" usage:"wal segments folder"`
	DedupTTL         time.Duration `name:"dedup_ttl" default:"1m" usage:"deduplication window"`
	MaxBatchReadSize int           `name:"max_batch_read_size" default:"1024" usage:"maximum number of records to read in a single batch"`
	CacheSize        int           `name:"cache_size" default:"4096" usage:"preloader LRU capacity"`
	Port             int           `name:"port" usage:"port to listen on"`
	Host             string        `name:"host" default:"0.0.0.0" usage:"host to listen on"`
}

func main() {
	var cfg StratusConfig
	_, err := configs.Resolve(&cfg, "")
	if err != nil {
		log.Fatalf("failed to resolve config: %v", err)
	}
	
	w, r, err := wal.Open(cfg.WALDir,
		wal.WithBatchSize(8),
	)
	if err != nil {
		log.Fatalf("failed to open WAL: %v", err)
	}
	
	sharedlog.Info("wal opened",
		sharedlog.F("bytesTruncated", r.BytesTruncated),
		sharedlog.F("segmentsRemoved", r.SegmentsRemoved),
	)
	
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
		preloader.WithCache(lru.New[storage.Record](cfg.CacheSize)),
	)
	if err != nil {
		_ = w.Close()
		log.Fatalf("failed to create preloader: %v", err)
	}
	
	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return pre.Start(gCtx, r.FirstLSN, r.LastLSN)
	})
	
	if err := pre.WaitStarted(gCtx, preCacheStartTimeout); err != nil {
		stop()
		_ = w.Close()
		log.Fatalf("preloader did not start: %v", err)
	}
	
	cache, err := pre.Cache()
	if err != nil {
		stop()
		_ = w.Close()
		log.Fatalf("failed to create cache: %v", err)
	}
	
	str, err := stream.New(
		stream.WithIngester(in),
		stream.WithCache(cache),
	)
	if err != nil {
		stop()
		_ = w.Close()
		log.Fatalf("failed to create stream: %v", err)
	}
	
	srv, err := server.New(
		server.WithStream(str),
		server.WithAddr(fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)),
	)
	if err != nil {
		stop()
		_ = w.Close()
		log.Fatalf("failed to create server: %v", err)
	}
	
	g.Go(func() error {
		return srv.Run(gCtx)
	})
	
	if err := g.Wait(); err != nil {
		sharedlog.Error(err, sharedlog.F("reason", "unexpected failure"))
	}
	
	sharedlog.Info("shutting down server")
	
	pre.Stop()
	_ = w.Close()
}
