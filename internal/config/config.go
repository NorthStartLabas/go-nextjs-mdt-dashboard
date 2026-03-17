package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// Config holds the application configuration
type Config struct {
	SnowflakeDSN string
	SQLitePath    string
	RoutesCSVPath string
	FloorMap      map[string]string
	OperatorMap   map[string]string
	BreaksConfig  map[string]map[string]float64 // LGNUM -> Hour -> BreakDuration
}

// LoadConfig reads configuration from files and environment
func LoadConfig() (*Config, error) {
	// Read Snowflake connection string from file
	dsnBytes, err := os.ReadFile("snowflake_go_driver_connection_string.txt")
	if err != nil {
		return nil, fmt.Errorf("failed to read snowflake connection string file: %w", err)
	}

	dsn := strings.TrimSpace(string(dsnBytes))
	dsn = strings.Trim(dsn, "\"")

	// Read floor mapping
	floorMapBytes, err := os.ReadFile("floor_mapping.json")
	if err != nil {
		return nil, fmt.Errorf("failed to read floor mapping file: %w", err)
	}

	var floorMap map[string]string
	if err := json.Unmarshal(floorMapBytes, &floorMap); err != nil {
		return nil, fmt.Errorf("failed to parse floor mapping: %w", err)
	}

	// Read operator mapping
	opMapBytes, err := os.ReadFile("operator_mapping.json")
	if err != nil {
		return nil, fmt.Errorf("failed to read operator mapping file: %w", err)
	}

	var opMap map[string]string
	if err := json.Unmarshal(opMapBytes, &opMap); err != nil {
		return nil, fmt.Errorf("failed to parse operator mapping: %w", err)
	}

	// Read breaks configuration
	breaksBytes, err := os.ReadFile("breaks_config.json")
	if err != nil {
		return nil, fmt.Errorf("failed to read breaks config file: %w", err)
	}

	var breaksConfig map[string]map[string]float64
	if err := json.Unmarshal(breaksBytes, &breaksConfig); err != nil {
		return nil, fmt.Errorf("failed to parse breaks config: %w", err)
	}

	return &Config{
		SnowflakeDSN: dsn,
		SQLitePath:    "extraction.db",
		RoutesCSVPath: "routes.csv",
		FloorMap:      floorMap,
		OperatorMap:   opMap,
		BreaksConfig:  breaksConfig,
	}, nil
}
