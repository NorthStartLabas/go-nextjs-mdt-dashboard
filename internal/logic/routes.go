package logic

import (
	"context"
	"encoding/csv"
	"fmt"
	"log/slog"
	"os"
)

// RouteProcessor manages the synchronization of route mappings
type RouteProcessor struct {
	sqlite RouteRepository
}

// NewRouteProcessor initializes a new RouteProcessor with injected dependencies
func NewRouteProcessor(sqlite RouteRepository) *RouteProcessor {
	return &RouteProcessor{
		sqlite: sqlite,
	}
}

// SyncRoutesFromCSV reads the CSV file and updates the SQLite database
func (p *RouteProcessor) SyncRoutesFromCSV(ctx context.Context, path string) error {
	slog.Info("syncing route mappings from CSV...", "path", path)

	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open csv file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	// Read header
	_, err = reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read csv header: %w", err)
	}

	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to read csv records: %w", err)
	}

	if err := p.sqlite.UpsertRoutes(ctx, records); err != nil {
		return fmt.Errorf("failed to upsert routes to database: %w", err)
	}

	slog.Info("successfully synced routes", "count", len(records), "path", path)
	return nil
}
