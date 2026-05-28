package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/boeing/go-ledger-x/internal/handler"
	"github.com/boeing/go-ledger-x/internal/kafka"
	"github.com/boeing/go-ledger-x/internal/repository"
	"github.com/boeing/go-ledger-x/internal/worker"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbURL         := mustEnv("DATABASE_URL")
	kafkaBrokers  := strings.Split(mustEnv("KAFKA_BROKERS"), ",")
	listenAddr    := envOr("LISTEN_ADDR", ":8081")
	workerCount   := 10
	channelBuffer := 1000

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		slog.Error("connect postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	repo     := repository.NewWalletRepository(pool)
	producer := kafka.NewProducer(kafkaBrokers)
	defer producer.Close()

	consumer := kafka.NewConsumer(kafkaBrokers, "ledger-service", repo, producer)
	consumer.Start(ctx)
	defer consumer.Close()

	wp := worker.NewPool(workerCount, channelBuffer, repo, producer)
	wp.Start(ctx)

	srv := &http.Server{
		Addr:    listenAddr,
		Handler: handler.New(wp, repo).Routes(),
	}

	go func() {
		slog.Info("ledger service listening", "addr", listenAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http server", "err", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.Info("shutdown signal received")
	cancel() // stop consumer loop

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutCancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		slog.Error("http shutdown", "err", err)
	}

	wp.Shutdown() // drain in-flight worker jobs

	slog.Info("shutdown complete")
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		slog.Error("required env var missing", "key", key)
		os.Exit(1)
	}
	return v
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
