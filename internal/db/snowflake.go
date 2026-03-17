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

// LTAPUnifiedRecord represents joined picking data
type LTAPUnifiedRecord struct {
	LTAPRecord
	ROUTE string
	LPRIO string
}

// StreamPickingData fetches picking data joined with route mapping in one go
func (c *SnowflakeClient) StreamPickingData(date string, recordChan chan<- LTAPUnifiedRecord, errChan chan<- error) {
	defer close(recordChan)

	// Single query with LEFT JOINs to avoid client-side iteration
	query := `
		WITH LTAP AS (
			SELECT VLPLA, QDATU, NISTA, QNAME, KOBER, QZEIT, NLPLA, VBELN, VLTYP, LGNUM, BRGEW, LGORT, VOLUM
			FROM PROD_CDH_DB.SDS_MAIN.SDS_CP_LTAP
			WHERE LGNUM IN ('245', '266')
			  AND QDATU = ?
			  AND VBELN IS NOT NULL
			  AND LGORT = '4000'
		),
		ZORF AS (
			SELECT VBELN, CAST(ROUTE AS VARCHAR) as ROUTE, CAST(LPRIO AS VARCHAR) as LPRIO 
			FROM PROD_CDH_DB.SDS_MAIN.SDS_CP_ZORF_HU_TO_LINK
			UNION
			SELECT VBELN, CAST(ROUTE AS VARCHAR) as ROUTE, CAST(LPRIO AS VARCHAR) as LPRIO 
			FROM PROD_CDH_DB.SDS_MAIN.SDS_CP_ZORF_HUTO_LNKHIS
		)
		SELECT 
			L.*, 
			COALESCE(Z.ROUTE, 'NOT-FOUND') as ROUTE, 
			COALESCE(Z.LPRIO, 'NOT-FOUND') as LPRIO
		FROM LTAP L
		LEFT JOIN ZORF Z ON L.VBELN = Z.VBELN
	`
	rows, err := c.db.Query(query, date)
	if err != nil {
		errChan <- fmt.Errorf("failed to start picking stream: %w", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var r LTAPUnifiedRecord
		err := rows.Scan(
			&r.VLPLA, &r.QDATU, &r.NISTA, &r.QNAME, &r.KOBER, &r.QZEIT, &r.NLPLA,
			&r.VBELN, &r.VLTYP, &r.LGNUM, &r.BRGEW, &r.LGORT, &r.VOLUM,
			&r.ROUTE, &r.LPRIO,
		)
		if err != nil {
			errChan <- fmt.Errorf("error scanning picking row: %w", err)
			return
		}
		recordChan <- r
	}
}

// CDHDRUnifiedRecord represents joined packing data
type CDHDRUnifiedRecord struct {
	CDHDRRecord
	EXIDV                         string
	BRGEW, ZLAENG, ZBREIT, ZHOEHE float64
	VBELN, ROUTE, LPRIO, LGNUM    string
	ZNEST                         string
}

// StreamPackingData streams packing headers joined with VEKP and ZORF in one go
func (c *SnowflakeClient) StreamPackingData(date string, recordChan chan<- CDHDRUnifiedRecord, errChan chan<- error) {
	defer close(recordChan)

	query := `
		WITH HEADERS AS (
			SELECT OBJECTCLAS, OBJECTID, USERNAME, UDATE, UTIME, TCODE
			FROM PROD_CDH_DB.SDS_MAIN.SDS_CP_CDHDR
			WHERE OBJECTCLAS = 'HANDL_UNIT'
			  AND UDATE = ?
			  AND TCODE = 'ZORF_BOX_CLOSING'
		),
		VEKP AS (
			SELECT VENUM, EXIDV, BRGEW, ZLAENG, ZBREIT, ZHOEHE 
			FROM PROD_CDH_DB.SDS_MAIN.SDS_CP_VEKP
		),
		LINKS AS (
			SELECT EXIDV, VBELN, CAST(ROUTE AS VARCHAR) as ROUTE, CAST(LPRIO AS VARCHAR) as LPRIO, LGNUM, ZNEST 
			FROM PROD_CDH_DB.SDS_MAIN.SDS_CP_ZORF_HU_TO_LINK
			UNION
			SELECT EXIDV, VBELN, CAST(ROUTE AS VARCHAR) as ROUTE, CAST(LPRIO AS VARCHAR) as LPRIO, LGNUM, ZNEST 
			FROM PROD_CDH_DB.SDS_MAIN.SDS_CP_ZORF_HUTO_LNKHIS
		)
		SELECT 
			H.OBJECTCLAS, H.OBJECTID, H.USERNAME, H.UDATE, H.UTIME, H.TCODE,
			V.EXIDV, V.BRGEW, V.ZLAENG, V.ZBREIT, V.ZHOEHE,
			COALESCE(L.VBELN, 'NOT-FOUND') as VBELN, 
			COALESCE(L.ROUTE, 'NOT-FOUND') as ROUTE, 
			COALESCE(L.LPRIO, 'NOT-FOUND') as LPRIO, 
			COALESCE(L.LGNUM, 'NOT-FOUND') as LGNUM, 
			COALESCE(L.ZNEST, 'NOT-FOUND') as ZNEST
		FROM HEADERS H
		JOIN VEKP V ON H.OBJECTID = V.VENUM
		LEFT JOIN LINKS L ON V.EXIDV = L.EXIDV
		WHERE L.LGNUM IN ('245', '266')
	`
	rows, err := c.db.Query(query, date)
	if err != nil {
		errChan <- fmt.Errorf("failed to start packing stream: %w", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var r CDHDRUnifiedRecord
		var brf, zlf, zbf, zhf sql.NullFloat64
		var vb, rt, lp, lg, zn sql.NullString

		err := rows.Scan(
			&r.OBJECTCLAS, &r.OBJECTID, &r.USERNAME, &r.UDATE, &r.UTIME, &r.TCODE,
			&r.EXIDV, &brf, &zlf, &zbf, &zhf,
			&vb, &rt, &lp, &lg, &zn,
		)
		if err != nil {
			errChan <- fmt.Errorf("error scanning packing row: %w", err)
			return
		}
		r.BRGEW, r.ZLAENG, r.ZBREIT, r.ZHOEHE = brf.Float64, zlf.Float64, zbf.Float64, zhf.Float64
		r.VBELN, r.ROUTE, r.LPRIO, r.LGNUM, r.ZNEST = vb.String, rt.String, lp.String, lg.String, zn.String
		recordChan <- r
	}
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

// CDHDRRecord represents packing activity from SDS_CP_CDHDR
type CDHDRRecord struct {
	OBJECTCLAS, OBJECTID, USERNAME, UDATE, UTIME, TCODE string
}

// StreamCDHDRData streams packing headers for a specific date (format YYYYMMDD)
func (c *SnowflakeClient) StreamCDHDRData(date string, recordChan chan<- CDHDRRecord, errChan chan<- error) {
	defer close(recordChan)

	query := `
		SELECT OBJECTCLAS, OBJECTID, USERNAME, UDATE, UTIME, TCODE
		FROM PROD_CDH_DB.SDS_MAIN.SDS_CP_CDHDR
		WHERE OBJECTCLAS = 'HANDL_UNIT'
		  AND UDATE = ?
		  AND TCODE = 'ZORF_BOX_CLOSING'
	`
	rows, err := c.db.Query(query, date)
	if err != nil {
		errChan <- fmt.Errorf("failed to start CDHDR stream: %w", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var r CDHDRRecord
		if err := rows.Scan(&r.OBJECTCLAS, &r.OBJECTID, &r.USERNAME, &r.UDATE, &r.UTIME, &r.TCODE); err != nil {
			errChan <- fmt.Errorf("error scanning CDHDR row: %w", err)
			return
		}
		recordChan <- r
	}
}

// VEKPRecord represents HU metadata from SDS_CP_VEKP
type VEKPRecord struct {
	VENUM, EXIDV                  string
	BRGEW, ZLAENG, ZBREIT, ZHOEHE sql.NullFloat64
}

// FetchVEKPData fetches metadata for a list of internal HU IDs (VENUM)
func (c *SnowflakeClient) FetchVEKPData(venums []string) (map[string]VEKPRecord, error) {
	if len(venums) == 0 {
		return make(map[string]VEKPRecord), nil
	}

	inClause := ""
	for i := range venums {
		inClause += "?"
		if i < len(venums)-1 {
			inClause += ","
		}
	}

	query := fmt.Sprintf(`
		SELECT VENUM, EXIDV, BRGEW, ZLAENG, ZBREIT, ZHOEHE
		FROM PROD_CDH_DB.SDS_MAIN.SDS_CP_VEKP
		WHERE VENUM IN (%s)
	`, inClause)

	args := make([]interface{}, len(venums))
	for i, v := range venums {
		args[i] = v
	}

	rows, err := c.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch VEKP data: %w", err)
	}
	defer rows.Close()

	vekpMap := make(map[string]VEKPRecord)
	for rows.Next() {
		var r VEKPRecord
		if err := rows.Scan(&r.VENUM, &r.EXIDV, &r.BRGEW, &r.ZLAENG, &r.ZBREIT, &r.ZHOEHE); err != nil {
			return nil, err
		}
		vekpMap[r.VENUM] = r
	}
	return vekpMap, nil
}

// PackingLinkRecord represents distribution details from ZORF tables
type PackingLinkRecord struct {
	EXIDV, VBELN, ROUTE, LPRIO, LGNUM sql.NullString
	ZNEST                             sql.NullString
}

// FetchPackingLinkData fetches delivery/route links for a list of EXIDVs
func (c *SnowflakeClient) FetchPackingLinkData(exidvs []string) (map[string]PackingLinkRecord, error) {
	if len(exidvs) == 0 {
		return make(map[string]PackingLinkRecord), nil
	}

	inClause := ""
	for i := range exidvs {
		inClause += "?"
		if i < len(exidvs)-1 {
			inClause += ","
		}
	}

	query := fmt.Sprintf(`
		SELECT EXIDV, VBELN, ROUTE, LPRIO, LGNUM, ZNEST FROM PROD_CDH_DB.SDS_MAIN.SDS_CP_ZORF_HU_TO_LINK WHERE EXIDV IN (%s)
		UNION
		SELECT EXIDV, VBELN, ROUTE, LPRIO, LGNUM, ZNEST FROM PROD_CDH_DB.SDS_MAIN.SDS_CP_ZORF_HUTO_LNKHIS WHERE EXIDV IN (%s)
	`, inClause, inClause)

	args := make([]interface{}, len(exidvs))
	for i, v := range exidvs {
		args[i] = v
	}
	fullArgs := append(args, args...)

	rows, err := c.db.Query(query, fullArgs...)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch packing link data: %w", err)
	}
	defer rows.Close()

	linkMap := make(map[string]PackingLinkRecord)
	for rows.Next() {
		var r PackingLinkRecord
		if err := rows.Scan(&r.EXIDV, &r.VBELN, &r.ROUTE, &r.LPRIO, &r.LGNUM, &r.ZNEST); err != nil {
			return nil, err
		}
		linkMap[r.EXIDV.String] = r
	}
	return linkMap, nil
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
