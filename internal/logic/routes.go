package logic

import (
	"encoding/csv"
	"fmt"
	"os"
)

// RouteProcessor handles logic related to routes
type RouteProcessor struct {
	sqlite interface {
		UpsertRoutes(routes [][]string) error
	}
}

// NewRouteProcessor initializes the route processor
func NewRouteProcessor(sqlite interface {
	UpsertRoutes(routes [][]string) error
}) *RouteProcessor {
	return &RouteProcessor{sqlite: sqlite}
}

// SyncRoutesFromCSV reads the CSV file and updates the SQLite database
func (p *RouteProcessor) SyncRoutesFromCSV(path string) error {
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

	err = p.sqlite.UpsertRoutes(records)
	if err != nil {
		return fmt.Errorf("failed to upsert routes to database: %w", err)
	}

	fmt.Printf("Successfully synced %d routes from %s\n", len(records), path)
	return nil
}
