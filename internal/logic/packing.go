package logic

import (
	"extraction-pipeline/internal/db"
	"fmt"
	"strings"
	"time"
)

// PackingProcessor handles the extraction and transformation of packing data
type PackingProcessor struct {
	snowflake   *db.SnowflakeClient
	sqlite      *db.SQLiteClient
	operatorMap map[string]string
}

// NewPackingProcessor initializes a new PackingProcessor
func NewPackingProcessor(snowflake *db.SnowflakeClient, sqlite *db.SQLiteClient, operatorMap map[string]string) *PackingProcessor {
	return &PackingProcessor{
		snowflake:   snowflake,
		sqlite:      sqlite,
		operatorMap: operatorMap,
	}
}

// RunPackingExtraction performs the packing data extraction using unified streaming
func (p *PackingProcessor) RunPackingExtraction() error {
	today := time.Now().Format("2006-01-02")
	snowflakeDate := time.Now().Format("20060102")
	fmt.Printf("Starting packing extraction for date: %s\n", today)

	// 1. Start Unified Stream (Server-side Join)
	recordChan := make(chan db.CDHDRUnifiedRecord, 1000)
	errChan := make(chan error, 1)
	go p.snowflake.StreamPackingData(snowflakeDate, recordChan, errChan)

	var finalRecords []db.RawPackingRecord
	fmt.Println("Streaming joined packing data from Snowflake...")

	for r := range recordChan {
		// Basic transform
		record := db.RawPackingRecord{
			OBJECTCLAS: r.OBJECTCLAS,
			OBJECTID:   r.OBJECTID,
			USERNAME:   r.USERNAME,
			UDATE:      today, // Store standardized date
			UTIME:      r.UTIME,
			TCODE:      r.TCODE,
			EXIDV:      r.EXIDV,
			BRGEW:      r.BRGEW,
			ZLAENG:     r.ZLAENG,
			ZBREIT:     r.ZBREIT,
			ZHOEHE:     r.ZHOEHE,
			VBELN:      r.VBELN,
			ROUTE:      r.ROUTE,
			LPRIO:      r.LPRIO,
			LGNUM:      r.LGNUM,
			ZNEST:      r.ZNEST,
		}

		// Map Operator
		username := strings.TrimSpace(strings.ToUpper(r.USERNAME))
		if realName, ok := p.operatorMap[username]; ok {
			record.OPERATOR = realName
		} else {
			record.OPERATOR = r.USERNAME
		}

		// Correct Date/Time format for SQLite
		if len(record.UDATE) == 8 {
			record.UDATE = fmt.Sprintf("%s-%s-%s", record.UDATE[0:4], record.UDATE[4:6], record.UDATE[6:8])
		}
		if len(record.UTIME) == 6 {
			record.UTIME = fmt.Sprintf("%s:%s:%s", record.UTIME[0:2], record.UTIME[2:4], record.UTIME[4:6])
		}

		finalRecords = append(finalRecords, record)
	}

	// Check stream errors
	select {
	case err := <-errChan:
		return err
	default:
	}

	fmt.Printf("Mapped %d packing records. Saving to SQLite...", len(finalRecords))
	if err := p.sqlite.InsertRawPacking(today, finalRecords); err != nil {
		return fmt.Errorf("sqlite insertion failed: %w", err)
	}

	fmt.Println(" Done.")
	return nil
}
