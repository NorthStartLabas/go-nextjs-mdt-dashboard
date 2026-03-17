package logic

import (
	"extraction-pipeline/internal/db"
	"fmt"
	"time"
)

// PackingProcessor orchestrates the packing data extraction
type PackingProcessor struct {
	snowflake *db.SnowflakeClient
	sqlite    *db.SQLiteClient
}

// NewPackingProcessor initializes the processor
func NewPackingProcessor(snowflake *db.SnowflakeClient, sqlite *db.SQLiteClient) *PackingProcessor {
	return &PackingProcessor{
		snowflake: snowflake,
		sqlite:    sqlite,
	}
}

// RunPackingExtraction performs the packing data pull and transformation
func (p *PackingProcessor) RunPackingExtraction() error {
	todayRaw := time.Now().Format("20060102")    // YYYYMMDD for Snowflake CDHDR query
	todayFmt := time.Now().Format("2006-01-02") // YYYY-MM-DD for consistency in SQLite
	fmt.Printf("Starting packing extraction for date: %s\n", todayFmt)

	// 1. Prepare for streaming
	recordChan := make(chan db.CDHDRRecord, 1000)
	errChan := make(chan error, 1)

	// 2. Start Snowflake stream in a goroutine
	go p.snowflake.StreamCDHDRData(todayRaw, recordChan, errChan)

	// 3. Collect headers and extract unique ObjectIDs (VENUMs)
	var headers []db.CDHDRRecord
	vbelnMap := make(map[string]bool) // Using map as SET to collect unique IDs
	var venums []string

	fmt.Println("Streaming activity from SDS_CP_CDHDR...")
	for record := range recordChan {
		headers = append(headers, record)
		if !vbelnMap[record.OBJECTID] {
			vbelnMap[record.OBJECTID] = true
			venums = append(venums, record.OBJECTID)
		}
	}

	// Check for streaming errors
	select {
	case err := <-errChan:
		return err
	default:
	}

	fmt.Printf("Received %d activity headers (%d unique HUs)\n", len(headers), len(venums))
	if len(headers) == 0 {
		return nil
	}

	// 4. Fetch HU metadata from VEKP
	fmt.Println("Fetching HU details from SDS_CP_VEKP...")
	vekpData, err := p.snowflake.FetchVEKPData(venums)
	if err != nil {
		return fmt.Errorf("failed to fetch VEKP data: %w", err)
	}

	// Collect unique EXIDVs for the next step
	var exidvs []string
	exidvMap := make(map[string]bool)
	for _, v := range vekpData {
		if !exidvMap[v.EXIDV] {
			exidvMap[v.EXIDV] = true
			exidvs = append(exidvs, v.EXIDV)
		}
	}

	// 5. Fetch packing links from ZORF
	fmt.Println("Fetching distribution links from ZORF tables...")
	linkData, err := p.snowflake.FetchPackingLinkData(exidvs)
	if err != nil {
		return fmt.Errorf("failed to fetch packing link data: %w", err)
	}

	// 6. Join and process records
	fmt.Print("Consolidating and saving packing records to SQLite...")
	var finalRecords []db.RawPackingRecord
	for _, h := range headers {
		record := db.RawPackingRecord{
			OBJECTCLAS: h.OBJECTCLAS, OBJECTID: h.OBJECTID, USERNAME: h.USERNAME,
			UDATE: h.UDATE, UTIME: h.UTIME, TCODE: h.TCODE,
		}

		// Join VEKP
		if vekp, ok := vekpData[h.OBJECTID]; ok {
			record.EXIDV = vekp.EXIDV
			if vekp.BRGEW.Valid {
				record.BRGEW = vekp.BRGEW.Float64
			}
			if vekp.ZLAENG.Valid {
				record.ZLAENG = vekp.ZLAENG.Float64
			}
			if vekp.ZBREIT.Valid {
				record.ZBREIT = vekp.ZBREIT.Float64
			}
			if vekp.ZHOEHE.Valid {
				record.ZHOEHE = vekp.ZHOEHE.Float64
			}

			// Join ZORF
			if link, ok := linkData[vekp.EXIDV]; ok {
				// Filter by LGNUM (266 or 245)
				lgnum := ""
				if link.LGNUM.Valid {
					lgnum = link.LGNUM.String
				}
				if lgnum != "245" && lgnum != "266" {
					continue
				}

				record.LGNUM = lgnum
				if link.VBELN.Valid {
					record.VBELN = link.VBELN.String
				}
				if link.ROUTE.Valid {
					record.ROUTE = link.ROUTE.String
				}
				if link.LPRIO.Valid {
					record.LPRIO = link.LPRIO.String
				}
				if link.ZNEST.Valid {
					record.ZNEST = link.ZNEST.String
				}
			} else {
				// If no link found, we can't verify LGNUM, so skip
				continue
			}
		} else {
			// If no metadata found, skip
			continue
		}

		finalRecords = append(finalRecords, record)
	}

	if err := p.sqlite.InsertRawPacking(todayRaw, finalRecords); err != nil {
		return fmt.Errorf("sqlite packing insertion failed: %w", err)
	}

	fmt.Println(" Done.")
	return nil
}
