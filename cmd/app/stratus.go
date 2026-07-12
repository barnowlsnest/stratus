package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/barnowlsnest/go-datalib/v5/pkg/lru"
	"github.com/barnowlsnest/go-logslib/v2/pkg/logger"
	"github.com/barnowlsnest/go-wallib/pkg/wal"
	stratusv1 "github.com/barnowlsnest/stratus/api/grpc/stratus/v1"
	"github.com/barnowlsnest/stratus/cmd/app/config"
	"github.com/barnowlsnest/stratus/cmd/app/server"
	"github.com/barnowlsnest/stratus/internal/dedup"
	"github.com/barnowlsnest/stratus/internal/storage"
	"github.com/barnowlsnest/stratus/internal/stream"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

const (
	stopTimeout  = 5 * time.Second
	startTimeout = 30 * time.Second
)

func main() {
	appCfg, err := config.Load()
	if err != nil {
		log.Fatal("failed to load config: ", err)
	}

	mainCtx := context.Background()
	sysCtx, sysCancel := signal.NotifyContext(mainCtx, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer sysCancel()

	appLogger, err := newLogger(appCfg)
	if err != nil {
		log.Fatal("failed to create logger: ", err)
	}

	w, err := newWAL(appCfg, appLogger)
	if err != nil {
		log.Fatal("failed to create WAL: ", err)
	}
	defer func() { _ = w.Close() }()

	walStorage, err := newStorage(w, dedup.New(appCfg.DedupWindow))
	if err != nil {
		log.Fatal("failed to create storage: ", err)
	}

	walStream, err := newStream(walStorage, lru.New[storage.Record](appCfg.CacheSize))
	if err != nil {
		log.Fatal("failed to create stream: ", err)
	}

	addr := net.JoinHostPort(appCfg.Host, strconv.Itoa(appCfg.Port))
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal("failed to listen: ", err)
	}

	grpcServer := grpc.NewServer()
	stratusv1.RegisterStreamServiceServer(grpcServer, server.New(walStream))

	var g errgroup.Group
	g.Go(func() error {
		<-sysCtx.Done()
		appLogger.Info(fmt.Sprintf("shutting down: %s", context.Cause(sysCtx).Error()))
		grpcServer.GracefulStop()
		return sysCtx.Err()
	})
	g.Go(func() error {
		return walStream.Start(sysCtx)
	})

	if err := walStream.WaitForStart(sysCtx, startTimeout); err != nil {
		log.Fatal("failed to start stream: ", err)
	}

	appLogger.Info("stream is ready")

	g.Go(func() error {
		appLogger.Info("grpc: serving", logger.Field{Key: "addr", Value: addr})
		defer sysCancel()
		return grpcServer.Serve(lis)
	})

	if err := g.Wait(); err != nil {
		appLogger.Error(fmt.Sprintf("app error: %s", err.Error()))
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), stopTimeout)
	defer stopCancel()

	appLogger.Info("stoping the stream: ", logger.Field{Key: "timeout", Value: stopTimeout.String()})
	walStream.Stop(stopCtx)
}

func newLogger(cfg *config.Config) (*logger.Logger, error) {
	level, err := logger.LogLevelFromString(cfg.LogLevel)
	if err != nil {
		return nil, err
	}

	appLogger := logger.New(logger.Config{
		Level:      level,
		Format:     logger.TextFormat,
		BufferSize: 0,
		UseUTC:     true,
	})

	return appLogger, nil
}

func newWAL(appCfg *config.Config, appLogger *logger.Logger) (*wal.WAL, error) {
	w, re, err := wal.Open(appCfg.WALDir,
		wal.WithBatchSize(8),
		wal.WithLogger(appLogger),
	)
	if err != nil {
		return nil, err
	}

	appLogger.Info("wal: opened",
		logger.Field{Key: "wal_dir", Value: appCfg.WALDir},
		logger.Field{Key: "entries_recovered", Value: strconv.FormatInt(int64(re.EntriesRecovered), 10)},
		logger.Field{Key: "segments_removed", Value: strconv.FormatInt(int64(re.SegmentsRemoved), 10)},
		logger.Field{Key: "first_lsn", Value: strconv.FormatInt(int64(re.LastLSN), 10)},
		logger.Field{Key: "last_lsn", Value: strconv.FormatInt(int64(re.LastLSN), 10)},
	)

	return w, nil
}

func newStorage(w *wal.WAL, d *dedup.Deduplicator) (*storage.Storage, error) {
	return storage.New(
		storage.WithDeduplicator(d),
		storage.WithWAL(w),
	)
}

func newStream(s *storage.Storage, cache *lru.LRU[storage.Record]) (*stream.Stream, error) {
	return stream.New(
		stream.WithStorage(s),
		stream.WithCache(cache),
		stream.WithSubscriberBuffer(1),
	)
}
