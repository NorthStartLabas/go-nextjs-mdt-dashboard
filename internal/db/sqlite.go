package db

import (
	"database/sql"
	"fmt"
	_ "modernc.org/sqlite"
)

// SQLiteClient handles interaction with the local SQLite database
type SQLiteClient struct {
	db *sql.DB
}

// NewSQLiteClient initializes the SQLite database
func NewSQLiteClient(path string) (*SQLiteClient, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database: %w", err)
	}

	client := &SQLiteClient{db: db}
	if err := client.InitSchema(); err != nil {
		db.Close()
		return nil, err
	}

	return client, nil
}

// Close closes the database connection
func (c *SQLiteClient) Close() error {
	return c.db.Close()
}

// InitSchema creates the necessary tables if they don't exist
func (c *SQLiteClient) InitSchema() error {
	query := `
	CREATE TABLE IF NOT EXISTS routes (
		route TEXT PRIMARY KEY,
		flow TEXT
	);`
	_, err := c.db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create routes table: %w", err)
	}
	return nil
}

// UpsertRoutes clears the routes table and inserts new data
func (c *SQLiteClient) UpsertRoutes(routes [][]string) error {
	tx, err := c.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback()

	// Delete existing data
	_, err = tx.Exec("DELETE FROM routes")
	if err != nil {
		return fmt.Errorf("failed to clear routes table: %w", err)
	}

	// Insert new data
	stmt, err := tx.Prepare("INSERT INTO routes (route, flow) VALUES (?, ?)")
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	for _, row := range routes {
		if len(row) < 2 {
			continue
		}
		_, err = stmt.Exec(row[0], row[1])
		if err != nil {
			return fmt.Errorf("failed to insert route %v: %w", row[0], err)
		}
	}

	return tx.Commit()
}
