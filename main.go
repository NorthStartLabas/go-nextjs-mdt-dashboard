package main

import (
	"extraction-pipeline/internal/config"
	"extraction-pipeline/internal/db"
	"extraction-pipeline/internal/logic"
	"fmt"
	"log"
	"sync"
)

func main() {
	fmt.Println("Starting Extraction Pipeline...")

	// 1. Load Configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Critical error loading config: %v", err)
	}

	// 2. Initialize SQLite
	fmt.Println("Initializing SQLite...")
	sqliteClient, err := db.NewSQLiteClient(cfg.SQLitePath)
	if err != nil {
		log.Fatalf("Critical error initializing SQLite: %v", err)
	}
	defer sqliteClient.Close()

	// 3. Sync Routes
	fmt.Println("Syncing routes from CSV...")
	routeProc := logic.NewRouteProcessor(sqliteClient)
	if err := routeProc.SyncRoutesFromCSV(cfg.RoutesCSVPath); err != nil {
		log.Fatalf("Error syncing routes: %v", err)
	}

	// 4. Initialize Snowflake
	fmt.Println("Connecting to Snowflake (this may open a browser window)...")
	snowflakeClient, err := db.NewSnowflakeClient(cfg.SnowflakeDSN)
	if err != nil {
		log.Fatalf("Critical error connecting to Snowflake: %v", err)
	}
	defer snowflakeClient.Close()

	// 5. Run Extractions in Parallel
	fmt.Println("\nStarting concurrent data extractions...")
	var wg sync.WaitGroup
	errChan := make(chan error, 2)

	// Picking Extraction
	wg.Add(1)
	go func() {
		defer wg.Done()
		proc := logic.NewExtractionProcessor(snowflakeClient, sqliteClient, cfg.FloorMap)
		if err := proc.RunExtraction(); err != nil {
			errChan <- fmt.Errorf("picking extraction failed: %w", err)
		}
	}()

	// Packing Extraction
	wg.Add(1)
	go func() {
		defer wg.Done()
		proc := logic.NewPackingProcessor(snowflakeClient, sqliteClient)
		if err := proc.RunPackingExtraction(); err != nil {
			errChan <- fmt.Errorf("packing extraction failed: %w", err)
		}
	}()

	// Wait for completion
	wg.Wait()
	close(errChan)

	// Check for errors
	hasError := false
	for err := range errChan {
		fmt.Printf("ERROR: %v\n", err)
		hasError = true
	}

	if hasError {
		log.Fatal("Pipeline finished with errors.")
	}

	fmt.Println("\nSuccess! Pipeline initialized, routes synced, and all concurrent extractions completed.")
}
