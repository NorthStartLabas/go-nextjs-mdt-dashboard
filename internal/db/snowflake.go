package db

import (
	"database/sql"
	"fmt"
	_ "github.com/snowflakedb/gosnowflake"
)

// SnowflakeClient handles interaction with Snowflake
type SnowflakeClient struct {
	db *sql.DB
}

// NewSnowflakeClient initializes a connection to Snowflake
func NewSnowflakeClient(dsn string) (*SnowflakeClient, error) {
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open snowflake connection: %w", err)
	}

	// Verify connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping snowflake: %w", err)
	}

	return &SnowflakeClient{db: db}, nil
}

// Close closes the connection
func (c *SnowflakeClient) Close() error {
	return c.db.Close()
}

// TestQuery runs a simple query to confirm access to tables
func (c *SnowflakeClient) TestQuery() error {
	tables := []string{
		"PROD_CDH_DB.SDS_MAIN.SDS_CP_LTAP",
		"PROD_CDH_DB.SDS_MAIN.SDS_CP_ZORF_HU_TO_LINK",
		"PROD_CDH_DB.SDS_MAIN.SDS_CP_ZORF_HUTO_LNKHIS",
	}

	for _, table := range tables {
		var count int
		query := fmt.Sprintf("SELECT COUNT(*) FROM %s LIMIT 10", table)
		err := c.db.QueryRow(query).Scan(&count)
		if err != nil {
			return fmt.Errorf("failed to query table %s: %w", table, err)
		}
		fmt.Printf("Successfully accessed %s (Count: %d)\n", table, count)
	}

	return nil
}
