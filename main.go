package main

import (
	"extraction-pipeline/internal/config"
	"extraction-pipeline/internal/db"
	"extraction-pipeline/internal/logic"
	"fmt"
	"log"
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

	// 5. Run Extraction
	fmt.Println("\nStarting data extraction...")
	extractionProc := logic.NewExtractionProcessor(snowflakeClient, sqliteClient)
	if err := extractionProc.RunExtraction(); err != nil {
		log.Fatalf("Critical error during extraction: %v", err)
	}

	fmt.Println("\nSuccess! Pipeline initialized, routes synced, and data extraction completed.")
}
