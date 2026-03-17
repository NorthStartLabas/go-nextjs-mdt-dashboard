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

	// 4. Initialize Snowflake and Test
	fmt.Println("Connecting to Snowflake (this may open a browser window)...")
	snowflakeClient, err := db.NewSnowflakeClient(cfg.SnowflakeDSN)
	if err != nil {
		log.Fatalf("Critical error connecting to Snowflake: %v", err)
	}
	defer snowflakeClient.Close()

	fmt.Println("Running Snowflake table tests...")
	if err := snowflakeClient.TestQuery(); err != nil {
		log.Fatalf("Error testing Snowflake tables: %v", err)
	}

	fmt.Println("\nSuccess! Pipeline initialized, routes synced, and Snowflake connection verified.")
}
