package logic

import (
	"extraction-pipeline/internal/db"
	"fmt"
	"strings"
	"time"
)

// PickingProcessor orchestrates the Snowflake to SQLite picking pipeline
type PickingProcessor struct {
	snowflake   *db.SnowflakeClient
	sqlite      *db.SQLiteClient
	floorMap    map[string]string
	operatorMap map[string]string
}

// NewPickingProcessor initializes the processor
func NewPickingProcessor(snowflake *db.SnowflakeClient, sqlite *db.SQLiteClient, floorMap map[string]string, operatorMap map[string]string) *PickingProcessor {
	return &PickingProcessor{
		snowflake:   snowflake,
		sqlite:      sqlite,
		floorMap:    floorMap,
		operatorMap: operatorMap,
	}
}

// RunPicking performs the synchronized data pull and transformation
func (p *PickingProcessor) RunPicking() error {
	today := time.Now().Format("2006-01-02")
	fmt.Printf("Starting picking extraction for date: %s\n", today)

	// 1. Load context maps
	flowMap, err := p.sqlite.GetFlowMap()
	if err != nil {
		return fmt.Errorf("failed to load flow map: %w", err)
	}

	// 2. Start Unified Stream (Server-side Join)
	recordChan := make(chan db.LTAPUnifiedRecord, 1000)
	errChan := make(chan error, 1)
	go p.snowflake.StreamPickingData(today, recordChan, errChan)

	var finalRecords []db.RawPickingRecord
	fmt.Println("Streaming joined picking data from Snowflake...")

	for ltap := range recordChan {
		// Transform Date
		qdatu := ltap.QDATU
		if len(qdatu) >= 10 {
			qdatu = qdatu[:10]
		}

		// Transform Time
		qzeit := ltap.QZEIT
		if strings.Contains(qzeit, "T") {
			parts := strings.Split(qzeit, "T")
			if len(parts) > 1 {
				qzeit = strings.TrimSuffix(parts[1], "Z")
			}
		}

		record := db.RawPickingRecord{
			VLPLA: ltap.VLPLA, QDATU: qdatu, NISTA: ltap.NISTA,
			QNAME: ltap.QNAME, QZEIT: qzeit, VBELN: ltap.VBELN,
			LGNUM: ltap.LGNUM, LGORT: ltap.LGORT,
			ROUTE: ltap.ROUTE, LPRIO: ltap.LPRIO,
		}

		// Map Operator
		qname := strings.TrimSpace(strings.ToUpper(ltap.QNAME))
		if realName, ok := p.operatorMap[qname]; ok {
			record.OPERATOR = realName
		} else {
			record.OPERATOR = ltap.QNAME
		}

		// Map Floor
		if ltap.VLTYP.Valid {
			record.VLTYP = ltap.VLTYP.String
			if floorName, ok := p.floorMap[record.VLTYP]; ok {
				record.FLOOR = floorName
			} else {
				record.FLOOR = "UNKNOWN-TYPE"
			}
		} else {
			record.FLOOR = "NA"
		}

		// Nullable handling
		if ltap.KOBER.Valid { record.KOBER = ltap.KOBER.String }
		if ltap.NLPLA.Valid { record.NLPLA = ltap.NLPLA.String }
		if ltap.BRGEW.Valid { record.BRGEW = ltap.BRGEW.Float64 }
		if ltap.VOLUM.Valid { record.VOLUM = ltap.VOLUM.Float64 }

		// Map Flow
		if flow, ok := flowMap[ltap.ROUTE]; ok {
			if flow == "Y2-flow" {
				flow = "A-flow"
			}
			record.FLOW = flow
		} else {
			record.FLOW = "UNKNOWN-ROUTE"
		}

		finalRecords = append(finalRecords, record)
	}

	// Check stream errors
	select {
	case err := <-errChan:
		return err
	default:
	}

	fmt.Printf("Mapped %d records. Saving to SQLite...", len(finalRecords))
	if err := p.sqlite.InsertRawPicking(today, finalRecords); err != nil {
		return fmt.Errorf("sqlite insertion failed: %w", err)
	}

	fmt.Println(" Done.")
	return nil
}
