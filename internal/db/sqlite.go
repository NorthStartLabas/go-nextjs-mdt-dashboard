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
	queries := []string{
		`CREATE TABLE IF NOT EXISTS routes (
			route TEXT PRIMARY KEY,
			flow TEXT
		);`,
		`CREATE TABLE IF NOT EXISTS raw_picking (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			vlpla TEXT,
			qdatu TEXT,
			nista REAL,
			qname TEXT,
			kober TEXT,
			qzeit TEXT,
			nlpla TEXT,
			vbeln TEXT,
			vltyp TEXT,
			lgnum TEXT,
			brgew REAL,
			lgort TEXT,
			volum REAL,
			route TEXT,
			lprio TEXT,
			flow TEXT
		);`,
	}
	for _, q := range queries {
		_, err := c.db.Exec(q)
		if err != nil {
			return fmt.Errorf("failed to execute schema query: %w", err)
		}
	}
	return nil
}

// GetFlowMap returns a map of route to flow for lookup
func (c *SQLiteClient) GetFlowMap() (map[string]string, error) {
	rows, err := c.db.Query("SELECT route, flow FROM routes")
	if err != nil {
		return nil, fmt.Errorf("failed to query routes: %w", err)
	}
	defer rows.Close()

	flowMap := make(map[string]string)
	for rows.Next() {
		var route, flow string
		if err := rows.Scan(&route, &flow); err != nil {
			return nil, err
		}
		flowMap[route] = flow
	}
	return flowMap, nil
}

// RawPickingRecord matches the schema for bulk insertion
type RawPickingRecord struct {
	VLPLA, QDATU   string
	NISTA           float64
	QNAME, KOBER, QZEIT, NLPLA, VBELN, VLTYP, LGNUM string
	BRGEW           float64
	LGORT           string
	VOLUM           float64
	ROUTE, LPRIO, FLOW string
}

// InsertRawPicking clears today's data and inserts new records
func (c *SQLiteClient) InsertRawPicking(date string, records []RawPickingRecord) error {
	tx, err := c.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Clear existing data for the specific date to avoid duplicates on re-runs
	_, err = tx.Exec("DELETE FROM raw_picking WHERE qdatu = ?", date)
	if err != nil {
		return fmt.Errorf("failed to clear old data for %s: %w", date, err)
	}

	stmt, err := tx.Prepare(`
		INSERT INTO raw_picking (
			vlpla, qdatu, nista, qname, kober, qzeit, nlpla, vbeln, vltyp, lgnum, brgew, lgort, volum, route, lprio, flow
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, r := range records {
		_, err = stmt.Exec(
			r.VLPLA, r.QDATU, r.NISTA, r.QNAME, r.KOBER, r.QZEIT, r.NLPLA, r.VBELN,
			r.VLTYP, r.LGNUM, r.BRGEW, r.LGORT, r.VOLUM, r.ROUTE, r.LPRIO, r.FLOW,
		)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
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
