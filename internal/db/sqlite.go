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
			flow TEXT,
			floor TEXT,
			operator TEXT
		);`,
		`CREATE TABLE IF NOT EXISTS raw_packing (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			objectclas TEXT,
			objectid TEXT,
			username TEXT,
			udate TEXT,
			utime TEXT,
			tcode TEXT,
			exidv TEXT,
			brgew REAL,
			zlaeng REAL,
			zbreit REAL,
			zhoehe REAL,
			vbeln TEXT,
			route TEXT,
			lprio TEXT,
			lgnum TEXT,
			znest TEXT,
			operator TEXT
		);`,
		`CREATE TABLE IF NOT EXISTS hourly_picking_productivity (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			date TEXT,
			lgnum TEXT,
			flow TEXT,
			floor TEXT,
			hour TEXT,
			operator TEXT,
			line_count INTEGER,
			item_quantity REAL,
			total_weight REAL,
			total_volume_m3 REAL,
			effective_hours REAL,
			base_productivity REAL,
			weight_intensity REAL,
			item_intensity REAL,
			adjusted_productivity REAL
		);`,
		`CREATE TABLE IF NOT EXISTS daily_picking_productivity (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			date TEXT,
			lgnum TEXT,
			flow TEXT,
			floor TEXT,
			operator TEXT,
			line_count INTEGER,
			item_quantity REAL,
			total_weight REAL,
			total_volume_m3 REAL,
			total_hours REAL,
			base_productivity REAL,
			weight_intensity REAL,
			item_intensity REAL,
			adjusted_productivity REAL
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

// GetRawPickingRecords returns all picking records for a specific date
func (c *SQLiteClient) GetRawPickingRecords(date string) ([]RawPickingRecord, error) {
	rows, err := c.db.Query(`
		SELECT vlpla, qdatu, nista, qname, kober, qzeit, nlpla, vbeln, vltyp, lgnum, brgew, lgort, volum, route, lprio, flow, floor, operator
		FROM raw_picking WHERE qdatu = ?`, date)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch raw picking: %w", err)
	}
	defer rows.Close()

	var records []RawPickingRecord
	for rows.Next() {
		var r RawPickingRecord
		err := rows.Scan(
			&r.VLPLA, &r.QDATU, &r.NISTA, &r.QNAME, &r.KOBER, &r.QZEIT, &r.NLPLA, &r.VBELN,
			&r.VLTYP, &r.LGNUM, &r.BRGEW, &r.LGORT, &r.VOLUM, &r.ROUTE, &r.LPRIO, &r.FLOW, &r.FLOOR, &r.OPERATOR,
		)
		if err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	return records, nil
}

// RawPickingRecord matches the schema for bulk insertion
type RawPickingRecord struct {
	VLPLA, QDATU   string
	NISTA           float64
	QNAME, KOBER, QZEIT, NLPLA, VBELN, VLTYP, LGNUM string
	BRGEW           float64
	LGORT           string
	VOLUM           float64
	ROUTE, LPRIO, FLOW, FLOOR, OPERATOR string
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
			vlpla, qdatu, nista, qname, kober, qzeit, nlpla, vbeln, vltyp, lgnum, brgew, lgort, volum, route, lprio, flow, floor, operator
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, r := range records {
		_, err = stmt.Exec(
			r.VLPLA, r.QDATU, r.NISTA, r.QNAME, r.KOBER, r.QZEIT, r.NLPLA, r.VBELN,
			r.VLTYP, r.LGNUM, r.BRGEW, r.LGORT, r.VOLUM, r.ROUTE, r.LPRIO, r.FLOW, r.FLOOR, r.OPERATOR,
		)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// RawPackingRecord matches the SQLite schema for packing data
type RawPackingRecord struct {
	OBJECTCLAS, OBJECTID, USERNAME, UDATE, UTIME, TCODE string
	EXIDV                                               string
	BRGEW, ZLAENG, ZBREIT, ZHOEHE                       float64
	VBELN, ROUTE, LPRIO, LGNUM, ZNEST, OPERATOR           string
}

// InsertRawPacking clears today's data and inserts new packing records
func (c *SQLiteClient) InsertRawPacking(date string, records []RawPackingRecord) error {
	tx, err := c.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Handle both YYYY-MM-DD and YYYYMMDD if necessary, 
	// but extraction date is standardized to YYYY-MM-DD
	_, err = tx.Exec("DELETE FROM raw_packing WHERE udate = ?", date)
	if err != nil {
		return fmt.Errorf("failed to clear old packing data for %s: %w", date, err)
	}

	stmt, err := tx.Prepare(`
		INSERT INTO raw_packing (
			objectclas, objectid, username, udate, utime, tcode, exidv, brgew, zlaeng, zbreit, zhoehe, 
			vbeln, route, lprio, lgnum, znest, operator
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, r := range records {
		_, err = stmt.Exec(
			r.OBJECTCLAS, r.OBJECTID, r.USERNAME, r.UDATE, r.UTIME, r.TCODE,
			r.EXIDV, r.BRGEW, r.ZLAENG, r.ZBREIT, r.ZHOEHE,
			r.VBELN, r.ROUTE, r.LPRIO, r.LGNUM, r.ZNEST, r.OPERATOR,
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

// HourlyProductivityRecord represents the calculated productivity data
type HourlyProductivityRecord struct {
	Date, LGNUM, Flow, Floor, Hour, Operator string
	LineCount                                int
	ItemQuantity, TotalWeight, TotalVolumeM3 float64
	EffectiveHours, BaseProductivity         float64
	WeightIntensity, ItemIntensity           float64
	AdjustedProductivity                     float64
}

// InsertProductivity clears and inserts new productivity records for a date
func (c *SQLiteClient) InsertProductivity(date string, records []HourlyProductivityRecord) error {
	tx, err := c.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.Exec("DELETE FROM hourly_picking_productivity WHERE date = ?", date)
	if err != nil {
		return fmt.Errorf("failed to clear old productivity: %w", err)
	}

	stmt, err := tx.Prepare(`
		INSERT INTO hourly_picking_productivity (
			date, lgnum, flow, floor, hour, operator, line_count, item_quantity, 
			total_weight, total_volume_m3, effective_hours, base_productivity,
			weight_intensity, item_intensity, adjusted_productivity
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, r := range records {
		_, err = stmt.Exec(
			r.Date, r.LGNUM, r.Flow, r.Floor, r.Hour, r.Operator, r.LineCount, r.ItemQuantity,
			r.TotalWeight, r.TotalVolumeM3, r.EffectiveHours, r.BaseProductivity,
			r.WeightIntensity, r.ItemIntensity, r.AdjustedProductivity,
		)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// DailyProductivityRecord represents the aggregated daily productivity data
type DailyProductivityRecord struct {
	Date, LGNUM, Flow, Floor, Operator string
	LineCount                          int
	ItemQuantity, TotalWeight, TotalVolumeM3 float64
	TotalHours, BaseProductivity        float64
	WeightIntensity, ItemIntensity      float64
	AdjustedProductivity                float64
}

// InsertDailyProductivity clears and inserts new daily productivity records for a date
func (c *SQLiteClient) InsertDailyProductivity(date string, records []DailyProductivityRecord) error {
	tx, err := c.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.Exec("DELETE FROM daily_picking_productivity WHERE date = ?", date)
	if err != nil {
		return fmt.Errorf("failed to clear old daily productivity: %w", err)
	}

	stmt, err := tx.Prepare(`
		INSERT INTO daily_picking_productivity (
			date, lgnum, flow, floor, operator, line_count, item_quantity, 
			total_weight, total_volume_m3, total_hours, base_productivity,
			weight_intensity, item_intensity, adjusted_productivity
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, r := range records {
		_, err = stmt.Exec(
			r.Date, r.LGNUM, r.Flow, r.Floor, r.Operator, r.LineCount, r.ItemQuantity,
			r.TotalWeight, r.TotalVolumeM3, r.TotalHours, r.BaseProductivity,
			r.WeightIntensity, r.ItemIntensity, r.AdjustedProductivity,
		)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// GetHourlyProductivityRecords returns all hourly records for a specific date
func (c *SQLiteClient) GetHourlyProductivityRecords(date string) ([]HourlyProductivityRecord, error) {
	rows, err := c.db.Query(`
		SELECT date, lgnum, flow, floor, hour, operator, line_count, item_quantity, 
		       total_weight, total_volume_m3, effective_hours, base_productivity,
		       weight_intensity, item_intensity, adjusted_productivity
		FROM hourly_picking_productivity WHERE date = ?`, date)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch hourly productivity: %w", err)
	}
	defer rows.Close()

	var records []HourlyProductivityRecord
	for rows.Next() {
		var r HourlyProductivityRecord
		err := rows.Scan(
			&r.Date, &r.LGNUM, &r.Flow, &r.Floor, &r.Hour, &r.Operator, &r.LineCount, &r.ItemQuantity,
			&r.TotalWeight, &r.TotalVolumeM3, &r.EffectiveHours, &r.BaseProductivity,
			&r.WeightIntensity, &r.ItemIntensity, &r.AdjustedProductivity,
		)
		if err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	return records, nil
}

