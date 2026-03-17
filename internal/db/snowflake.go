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

// LTAPRecord represents data from SDS_CP_LTAP
type LTAPRecord struct {
	VLPLA, QDATU   string
	NISTA           float64
	QNAME           string
	KOBER           sql.NullString
	QZEIT           string
	NLPLA           sql.NullString
	VBELN           string
	VLTYP           sql.NullString
	LGNUM           string
	BRGEW           sql.NullFloat64
	LGORT           string
	VOLUM           sql.NullFloat64
}

// StreamLTAPData fetches picking data and sends it to a channel for concurrent processing
func (c *SnowflakeClient) StreamLTAPData(date string, recordChan chan<- LTAPRecord, errChan chan<- error) {
	defer close(recordChan)

	query := `
		SELECT VLPLA, QDATU, NISTA, QNAME, KOBER, QZEIT, NLPLA, VBELN, VLTYP, LGNUM, BRGEW, LGORT, VOLUM
		FROM PROD_CDH_DB.SDS_MAIN.SDS_CP_LTAP
		WHERE LGNUM IN ('245', '266')
		  AND QDATU = ?
		  AND VBELN IS NOT NULL
		  AND LGORT = '4000'
	`
	rows, err := c.db.Query(query, date)
	if err != nil {
		errChan <- fmt.Errorf("failed to start LTAP stream: %w", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var r LTAPRecord
		err := rows.Scan(
			&r.VLPLA, &r.QDATU, &r.NISTA, &r.QNAME, &r.KOBER, &r.QZEIT, &r.NLPLA,
			&r.VBELN, &r.VLTYP, &r.LGNUM, &r.BRGEW, &r.LGORT, &r.VOLUM,
		)
		if err != nil {
			errChan <- fmt.Errorf("error scanning LTAP row: %w", err)
			return
		}
		recordChan <- r
	}
}

// RouteRecord represents route mapping data
type RouteRecord struct {
	VBELN, ROUTE, LPRIO string
}

// FetchRouteData fetches route information for a list of VBELNs
func (c *SnowflakeClient) FetchRouteData(vbelns []string) (map[string]RouteRecord, error) {
	if len(vbelns) == 0 {
		return make(map[string]RouteRecord), nil
	}

	// Step 1: Create a temporary table or use a very large IN clause (Snowflake supports up to 16,384 values)
	// For performance, since vbelns could be large, we'll chunk it if needed, but here we'll use a single query as a start.
	// In a production Go architected way, we'd use a temporary table for 10k+ IDs.
	
	inClause := ""
	args := make([]interface{}, len(vbelns))
	for i, v := range vbelns {
		inClause += "?"
		if i < len(vbelns)-1 {
			inClause += ","
		}
		args[i] = v
	}

	query := fmt.Sprintf(`
		SELECT VBELN, ROUTE, LPRIO FROM PROD_CDH_DB.SDS_MAIN.SDS_CP_ZORF_HU_TO_LINK WHERE VBELN IN (%s)
		UNION
		SELECT VBELN, ROUTE, LPRIO FROM PROD_CDH_DB.SDS_MAIN.SDS_CP_ZORF_HUTO_LNKHIS WHERE VBELN IN (%s)
	`, inClause, inClause)

	fullArgs := append(args, args...)

	rows, err := c.db.Query(query, fullArgs...)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch route mapping: %w", err)
	}
	defer rows.Close()

	routeMap := make(map[string]RouteRecord)
	for rows.Next() {
		var r RouteRecord
		if err := rows.Scan(&r.VBELN, &r.ROUTE, &r.LPRIO); err != nil {
			return nil, err
		}
		routeMap[r.VBELN] = r
	}
	return routeMap, nil
}

// TestQuery runs a simple query to confirm access to tables
func (c *SnowflakeClient) TestQuery() error {
	tables := []string{
		"PROD_CDH_DB.SDS_MAIN.SDS_CP_LTAP",
		"PROD_CDH_DB.SDS_MAIN.SDS_CP_ZORF_HU_TO_LINK",
		"PROD_CDH_DB.SDS_MAIN.SDS_CP_ZORF_HUTO_LNKHIS",
	}
// ... existing loop

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
