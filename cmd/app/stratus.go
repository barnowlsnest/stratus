package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"github.com/barnowlsnest/go-datalib/v5/pkg/lru"
	"github.com/barnowlsnest/go-logslib/v2/pkg/logger"
	"github.com/barnowlsnest/go-wallib/pkg/wal"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	stratusv1 "github.com/barnowlsnest/stratus/api/grpc/stratus/v1"
	"github.com/barnowlsnest/stratus/cmd/app/config"
	"github.com/barnowlsnest/stratus/cmd/app/server"
	"github.com/barnowlsnest/stratus/internal/dedup"
	"github.com/barnowlsnest/stratus/internal/storage"
	"github.com/barnowlsnest/stratus/internal/stream"
)

const (
	stopTimeout  = 5 * time.Second
	startTimeout = 30 * time.Second

	addrField = "addr"

	pprofReadHeaderTimeout = 5 * time.Second
	// pprofWriteTimeout must outlast the longest ?seconds= a CPU profile or trace asks for.
	pprofWriteTimeout = 2 * time.Minute
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	appCfg, err := config.Load()
	if err != nil {
		return err
	}

	mainCtx := context.Background()
	sysCtx, sysCancel := signal.NotifyContext(mainCtx, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer sysCancel()

	appLogger, err := newLogger(appCfg)
	if err != nil {
		return err
	}

	w, err := newWAL(appCfg, appLogger)
	if err != nil {
		return err
	}

	defer func() { _ = w.Close() }()

	walStorage, err := newStorage(w, dedup.New(appCfg.DedupWindow), appCfg.MaxBatchReadSize)
	if err != nil {
		return err
	}

	walStream, err := newStream(walStorage, appCfg)
	if err != nil {
		return err
	}

	addr := net.JoinHostPort(appCfg.Host, strconv.Itoa(appCfg.Port))
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	grpcServer, err := newGRPCServer(appCfg, appLogger)
	if err != nil {
		_ = lis.Close()
		return err
	}

	stratusv1.RegisterStreamServiceServer(grpcServer, server.New(walStream))

	pprofServer := startPprof(appCfg, appLogger)

	var g errgroup.Group
	g.Go(func() error {
		<-sysCtx.Done()
		appLogger.Info(fmt.Sprintf("shutting down: %s", context.Cause(sysCtx).Error()))
		stopPprof(pprofServer, appLogger)
		grpcServer.GracefulStop()
		return sysCtx.Err()
	})
	g.Go(func() error {
		return walStream.Start(sysCtx)
	})

	if err := walStream.WaitForStart(sysCtx, startTimeout); err != nil {
		return err
	}

	appLogger.Info("stream is ready")

	g.Go(func() error {
		appLogger.Info("grpc: serving", logger.Field{Key: addrField, Value: addr})
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

	return nil
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

func newGRPCServer(cfg *config.Config, appLogger *logger.Logger) (*grpc.Server, error) {
	var opts []grpc.ServerOption

	if cfg.TLSEnabled() {
		creds, err := credentials.NewServerTLSFromFile(cfg.TLSCertFile, cfg.TLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load tls credentials: %w", err)
		}

		opts = append(opts, grpc.Creds(creds))
	} else {
		appLogger.Warn("grpc: TLS disabled, traffic (including the auth token) is plaintext")
	}

	if cfg.AuthToken != "" {
		opts = append(opts,
			grpc.UnaryInterceptor(server.UnaryAuthInterceptor(cfg.AuthToken)),
			grpc.StreamInterceptor(server.StreamAuthInterceptor(cfg.AuthToken)),
		)
	} else {
		appLogger.Warn("grpc: authentication disabled, set AUTH_TOKEN to require it")
	}

	return grpc.NewServer(opts...), nil
}

func newWAL(appCfg *config.Config, appLogger *logger.Logger) (*wal.WAL, error) {
	w, re, err := wal.Open(appCfg.WALDir,
		wal.WithBatchSize(appCfg.WALBatchSize),
		wal.WithMaxSegmentSize(appCfg.WALMaxSegmentSize),
		wal.WithMaxRecordSize(appCfg.WALMaxRecordSize),
		wal.WithLogger(appLogger),
	)
	if err != nil {
		return nil, err
	}

	appLogger.Info("wal: opened",
		logger.StringField("wal_dir", appCfg.WALDir),
		logger.Uint64Field("entries_recovered", re.EntriesRecovered),
		logger.IntField("segments_removed", re.SegmentsRemoved),
		logger.Uint64Field("first_lsn", re.FirstLSN),
		logger.Uint64Field("last_lsn", re.LastLSN),
	)

	return w, nil
}

// newStorage builds the storage; a maxReadBatch of zero or less keeps the
// storage's default.
func newStorage(w *wal.WAL, d *dedup.Deduplicator, maxReadBatch int) (*storage.Storage, error) {
	return storage.New(
		storage.WithDeduplicator(d),
		storage.WithWAL(w),
		storage.WithMaxReadBatchSize(uint64(max(maxReadBatch, 0))),
	)
}

func newStream(s *storage.Storage, appCfg *config.Config) (*stream.Stream, error) {
	return stream.New(
		stream.WithStorage(s),
		stream.WithCache(lru.New[storage.Record](appCfg.CacheSize)),
		stream.WithCacheSize(appCfg.CacheSize),
		stream.WithSubscriberBuffer(appCfg.SubscriberBuffer),
		stream.WithMaxReadBytes(appCfg.MaxReadBytes),
	)
}

// startPprof serves the pprof handlers on cfg.PprofAddr and returns the server,
// or nil when pprof is disabled or cannot listen. Its failures are only logged,
// so streaming never stops because of the profiling endpoint.
func startPprof(cfg *config.Config, appLogger *logger.Logger) *http.Server {
	if cfg.PprofAddr == "" {
		return nil
	}

	runtime.SetBlockProfileRate(cfg.PprofBlockRate)
	runtime.SetMutexProfileFraction(cfg.PprofMutexFrac)

	lis, err := net.Listen("tcp", cfg.PprofAddr)
	if err != nil {
		appLogger.Error(fmt.Sprintf("pprof: listen: %s", err.Error()))
		return nil
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: pprofReadHeaderTimeout,
		WriteTimeout:      pprofWriteTimeout,
	}

	go func() {
		appLogger.Info("pprof: serving", logger.Field{Key: addrField, Value: cfg.PprofAddr})
		if err := srv.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
			appLogger.Error(fmt.Sprintf("pprof: serve: %s", err.Error()))
		}
	}()

	return srv
}

// stopPprof shuts the pprof server down within stopTimeout; a nil server is a no-op.
func stopPprof(srv *http.Server, appLogger *logger.Logger) {
	if srv == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), stopTimeout)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		appLogger.Error(fmt.Sprintf("pprof: shutdown: %s", err.Error()))
	}
}
