package main

import (
	"context"
	"extraction-pipeline/internal/config"
	"extraction-pipeline/internal/db"
	"extraction-pipeline/internal/logic"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

func main() {
	// Initialize Structured Logging
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	slog.Info("starting extraction pipeline...")

	// 1. Create Root Context
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
	defer cancel()

	// 2. Load Configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		slog.Error("failed to load configuration", "error", err)
		os.Exit(1)
	}

	// 3. Initialize SQLite
	slog.Info("initializing SQLite...", "path", cfg.SQLitePath)
	sqliteClient, err := db.NewSQLiteClient(cfg.SQLitePath)
	if err != nil {
		slog.Error("failed to initialize SQLite", "error", err)
		os.Exit(1)
	}
	defer sqliteClient.Close()

	// 4. Sync Routes
	routeProc := logic.NewRouteProcessor(sqliteClient)
	if err := routeProc.SyncRoutesFromCSV(ctx, cfg.RoutesCSVPath); err != nil {
		slog.Error("error syncing routes", "error", err)
		os.Exit(1)
	}

	// 5. Initialize Snowflake
	slog.Info("connecting to Snowflake...")
	snowflakeClient, err := db.NewSnowflakeClient(cfg.SnowflakeDSN)
	if err != nil {
		slog.Error("failed to connect to Snowflake", "error", err)
		os.Exit(1)
	}
	defer snowflakeClient.Close()

	// 6. Pre-Clear Data (Sequential to avoid SQLite locking issues)
	today := time.Now().Format("2006-01-02")
	slog.Info("pre-clearing data for today...", "date", today)
	if err := sqliteClient.ClearPickingDate(ctx, today); err != nil {
		slog.Error("failed to clear picking data", "error", err)
		os.Exit(1)
	}
	if err := sqliteClient.ClearPackingDate(ctx, today); err != nil {
		slog.Error("failed to clear packing data", "error", err)
		os.Exit(1)
	}

	// 7. Run Extractions in Parallel
	slog.Info("starting concurrent data extractions...")
	var wg sync.WaitGroup
	errChan := make(chan error, 2)

	// Picking Extraction
	wg.Add(1)
	go func() {
		defer wg.Done()
		proc := logic.NewPickingProcessor(snowflakeClient, sqliteClient, cfg.FloorMap, cfg.OperatorMap)
		if err := proc.RunPicking(ctx); err != nil {
			errChan <- fmt.Errorf("picking extraction failed: %w", err)
		}
	}()

	// Packing Extraction
	wg.Add(1)
	go func() {
		defer wg.Done()
		proc := logic.NewPackingProcessor(snowflakeClient, sqliteClient, cfg.OperatorMap, cfg.FloorMap)
		if err := proc.RunPackingExtraction(ctx); err != nil {
			errChan <- fmt.Errorf("packing extraction failed: %w", err)
		}
	}()

	// Wait for completion
	wg.Wait()
	close(errChan)

	// Check for errors
	hasError := false
	for err := range errChan {
		slog.Error("extraction error occurred", "error", err)
		hasError = true
	}

	if hasError {
		slog.Error("pipeline finished with errors")
		os.Exit(1)
	}

	slog.Info("extractions completed successfully")

	// 8. Calculate Productivity
	prodProc := logic.NewProductivityProcessor(sqliteClient, cfg.BreaksConfig)

	slog.Info("triggering productivity calculations...", "date", today)
	if err := prodProc.CalculateHourlyProductivity(ctx, today); err != nil {
		slog.Warn("hourly picking productivity failed", "error", err)
	}

	if err := prodProc.CalculateDailyProductivity(ctx, today); err != nil {
		slog.Warn("daily picking productivity failed", "error", err)
	}

	// Packing Productivity
	if err := prodProc.CalculateHourlyPackingProductivity(ctx, today); err != nil {
		slog.Warn("hourly packing productivity failed", "error", err)
	}

	if err := prodProc.CalculateDailyPackingProductivity(ctx, today); err != nil {
		slog.Warn("daily packing productivity failed", "error", err)
	}

	slog.Info("pipeline and analytics complete")
}
