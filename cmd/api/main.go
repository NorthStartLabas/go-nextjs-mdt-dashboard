package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"extraction-pipeline/internal/api"
	"extraction-pipeline/internal/config"
	"extraction-pipeline/internal/db"
	"extraction-pipeline/internal/orchestrator"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := config.LoadConfig()
	if err != nil {
		logger.Error("failed to load configuration", "error", err)
		os.Exit(1)
	}

	sqliteClient, err := db.NewSQLiteClient(cfg.SQLitePath)
	if err != nil {
		logger.Error("failed to initialize sqlite", "error", err)
		os.Exit(1)
	}
	defer sqliteClient.Close()

	if err := sqliteClient.FailStalePipelineRuns(context.Background(), time.Now().UTC()); err != nil {
		logger.Warn("failed to clean stale pipeline runs", "error", err)
	}

	workDir, err := os.Getwd()
	if err != nil {
		logger.Error("failed to determine working directory", "error", err)
		os.Exit(1)
	}
	runner := &orchestrator.CommandRunner{WorkDir: workDir, Log: logger}
	manager := orchestrator.NewManager(sqliteClient, runner, logger)

	configFiles := map[string]string{
		"breaks":   "breaks_config.json",
		"floor":    "floor_mapping.json",
		"operator": "operator_mapping.json",
	}
	server := api.NewServer(sqliteClient, manager, logger, configFiles)

	httpServer := &http.Server{
		Addr:    ":8080",
		Handler: server.Handler(),
	}

	go func() {
		logger.Info("api server listening", "addr", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("api server failed", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("failed to gracefully shutdown api server", "error", err)
	}
	logger.Info("api server stopped")
}
