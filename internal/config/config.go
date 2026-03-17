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
	// The driver expects "account/user?auth..." or a full URL.
	// The provided string looks like: "BUTERL2@MEDTRONIC.COM@MDTPLC-AWSUSE1P1/PROD_CDH_DB/SDS_MAIN?warehouse=PROD_ANALYTICS_WH&authenticator=externalbrowser"
	// We might need to prefix with snowflake:// if the driver requires it, or handle it as is.
    // gosnowflake usually accepts: <user>:<password>@<account>/<database>/<schema>?warehouse=<warehouse>
    // The user's string is a bit different but looks like a valid Snowflake DSN for externalbrowser.

	// Read floor mapping
	floorMapBytes, err := os.ReadFile("floor_mapping.json")
	if err != nil {
		return nil, fmt.Errorf("failed to read floor mapping file: %w", err)
	}

	var floorMap map[string]string
	if err := json.Unmarshal(floorMapBytes, &floorMap); err != nil {
		return nil, fmt.Errorf("failed to parse floor mapping: %w", err)
	}

	return &Config{
		SnowflakeDSN: dsn,
		SQLitePath:    "extraction.db",
		RoutesCSVPath: "routes.csv",
		FloorMap:      floorMap,
	}, nil
}
