package main

import (
	"extraction-pipeline/internal/config"
	"extraction-pipeline/internal/db"
	"extraction-pipeline/internal/logic"
	"fmt"
	"log"
	"sync"
	"time"
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
		proc := logic.NewPickingProcessor(snowflakeClient, sqliteClient, cfg.FloorMap, cfg.OperatorMap)
		if err := proc.RunPicking(); err != nil {
			errChan <- fmt.Errorf("picking extraction failed: %w", err)
		}
	}()

	// Packing Extraction
	wg.Add(1)
	go func() {
		defer wg.Done()
		proc := logic.NewPackingProcessor(snowflakeClient, sqliteClient, cfg.OperatorMap)
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

	// 6. Calculate Productivity
	fmt.Println("\nTriggering productivity calculations...")
	today := time.Now().Format("2006-01-02")
	prodProc := logic.NewProductivityProcessor(sqliteClient, cfg.BreaksConfig)
	if err := prodProc.CalculateHourlyProductivity(today); err != nil {
		fmt.Printf("WARNING: Hourly productivity calculation failed: %v\n", err)
	}

	if err := prodProc.CalculateDailyProductivity(today); err != nil {
		fmt.Printf("WARNING: Daily productivity calculation failed: %v\n", err)
	}

	fmt.Println("\nPipeline and Analytics complete.")
}
