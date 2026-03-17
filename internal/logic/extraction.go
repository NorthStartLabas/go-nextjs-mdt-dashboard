package logic

import (
	"extraction-pipeline/internal/db"
	"fmt"
	"time"
)

// ExtractionProcessor orchestrates the Snowflake to SQLite pipeline
type ExtractionProcessor struct {
	snowflake *db.SnowflakeClient
	sqlite    *db.SQLiteClient
}

// NewExtractionProcessor initializes the processor
func NewExtractionProcessor(snowflake *db.SnowflakeClient, sqlite *db.SQLiteClient) *ExtractionProcessor {
	return &ExtractionProcessor{
		snowflake: snowflake,
		sqlite:    sqlite,
	}
}

// RunExtraction performs the data pull and transformation
func (p *ExtractionProcessor) RunExtraction() error {
	today := time.Now().Format("2006-01-02")
	fmt.Printf("Starting extraction for date: %s\n", today)

	// 1. Prepare for streaming
	recordChan := make(chan db.LTAPRecord, 1000)
	errChan := make(chan error, 1)

	// 2. Start Snowflake stream in a goroutine
	go p.snowflake.StreamLTAPData(today, recordChan, errChan)

	// 3. Collect records first to get the VBELNs for the join
	// Note: Mapping requires VBELNs from LTAP. To be truly parallel with JOINs,
	// we fetch LTAP first, then resolve routes, then save.
	// We can stream the INSERT part after we have the route mapping.
	var ltapRecords []db.LTAPRecord
	vbelnMap := make(map[string]bool)
	var vbelns []string

	fmt.Println("Streaming data from SDS_CP_LTAP...")
	for record := range recordChan {
		ltapRecords = append(ltapRecords, record)
		if !vbelnMap[record.VBELN] {
			vbelnMap[record.VBELN] = true
			vbelns = append(vbelns, record.VBELN)
		}
	}

	// Check for streaming errors
	select {
	case err := <-errChan:
		return err
	default:
	}

	fmt.Printf("Received %d records from LTAP (%d unique VBELNs)\n", len(ltapRecords), len(vbelns))
	if len(ltapRecords) == 0 {
		return nil
	}

	// 4. Fetch Route/HU mapping from Snowflake
	fmt.Println("Fetching Route mapping from ZORF tables...")
	routeRecords, err := p.snowflake.FetchRouteData(vbelns)
	if err != nil {
		return fmt.Errorf("failed to fetch route mapping: %w", err)
	}

	// 5. Load Flow map from local SQLite
	flowMap, err := p.sqlite.GetFlowMap()
	if err != nil {
		return fmt.Errorf("failed to load flow map: %w", err)
	}

	// 6. Process and Insert
	fmt.Print("Mapping and saving records to SQLite...")
	var finalRecords []db.RawPickingRecord
	for _, ltap := range ltapRecords {
		record := db.RawPickingRecord{
			VLPLA: ltap.VLPLA, QDATU: ltap.QDATU, NISTA: ltap.NISTA,
			QNAME: ltap.QNAME, QZEIT: ltap.QZEIT, VBELN: ltap.VBELN,
			LGNUM: ltap.LGNUM, LGORT: ltap.LGORT,
		}

		// Handle Nullable fields
		if ltap.KOBER.Valid {
			record.KOBER = ltap.KOBER.String
		}
		if ltap.NLPLA.Valid {
			record.NLPLA = ltap.NLPLA.String
		}
		if ltap.VLTYP.Valid {
			record.VLTYP = ltap.VLTYP.String
		}
		if ltap.BRGEW.Valid {
			record.BRGEW = ltap.BRGEW.Float64
		}
		if ltap.VOLUM.Valid {
			record.VOLUM = ltap.VOLUM.Float64
		}

		if routeInfo, ok := routeRecords[ltap.VBELN]; ok {
			record.ROUTE = routeInfo.ROUTE
			record.LPRIO = routeInfo.LPRIO
			if flow, ok := flowMap[routeInfo.ROUTE]; ok {
				record.FLOW = flow
			} else {
				record.FLOW = "UNKNOWN-ROUTE"
			}
		} else {
			record.ROUTE = "NOT-FOUND"
			record.LPRIO = "NOT-FOUND"
			record.FLOW = "NOT-FOUND"
		}
		finalRecords = append(finalRecords, record)
	}

	if err := p.sqlite.InsertRawPicking(today, finalRecords); err != nil {
		return fmt.Errorf("sqlite insertion failed: %w", err)
	}

	fmt.Println(" Done.")
	return nil
}
