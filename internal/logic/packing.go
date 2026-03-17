package logic

import (
	"context"
	"extraction-pipeline/internal/db"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// PackingProcessor handles the extraction and transformation of packing data
type PackingProcessor struct {
	snowflake   SnowflakeProvider
	sqlite      PackingRepository
	operatorMap map[string]string
	floorMap    map[string]string
}

// NewPackingProcessor initializes a new PackingProcessor with injected dependencies
func NewPackingProcessor(snowflake SnowflakeProvider, sqlite PackingRepository, operatorMap map[string]string, floorMap map[string]string) *PackingProcessor {
	return &PackingProcessor{
		snowflake:   snowflake,
		sqlite:      sqlite,
		operatorMap: operatorMap,
		floorMap:    floorMap,
	}
}

// RunPackingExtraction performs the packing data extraction using unified streaming
func (p *PackingProcessor) RunPackingExtraction(ctx context.Context) error {
	today := time.Now().Format("2006-01-02")
	snowflakeDate := time.Now().Format("20060102")
	slog.Info("starting pipelined packing extraction", "date", today)

	flowMap, err := p.sqlite.GetFlowMap(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch flow map: %w", err)
	}

	recordChan := make(chan db.CDHDRUnifiedRecord, 1000)
	transformedChan := make(chan db.RawPackingRecord, 1000)
	errChan := make(chan error, 1)

	// 1. STAGE 1: PRODUCER
	go p.snowflake.StreamPackingData(ctx, snowflakeDate, recordChan, errChan)

	// 2. STAGE 2: WORKERS
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := range recordChan {
				transformedChan <- p.transformPackingRecord(r, today, flowMap)
			}
		}()
	}

	// 3. STAGE 3: CONSUMER
	dbDone := make(chan error, 1)
	go func() {
		var batch []db.RawPackingRecord
		count := 0
		for record := range transformedChan {
			batch = append(batch, record)
			count++
			if count%1000 == 0 {
				slog.Info("packing extraction progress", "rows_processed", count)
			}

			if len(batch) >= batchSize {
				if err := p.sqlite.BatchInsertPacking(ctx, batch); err != nil {
					dbDone <- fmt.Errorf("batch insert failed: %w", err)
					return
				}
				batch = nil
			}
		}
		if len(batch) > 0 {
			if err := p.sqlite.BatchInsertPacking(ctx, batch); err != nil {
				dbDone <- fmt.Errorf("final batch insert failed: %w", err)
				return
			}
		}
		slog.Info("consumer finished processing all packing records", "total_rows", count)
		dbDone <- nil
	}()

	wg.Wait()
	close(transformedChan)

	select {
	case err := <-errChan:
		if err != nil {
			return fmt.Errorf("snowflake stream error: %w", err)
		}
	case err := <-dbDone:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	}

	slog.Info("packing extraction complete", "date", today)
	return nil
}

func (p *PackingProcessor) transformPackingRecord(r db.CDHDRUnifiedRecord, today string, flowMap map[string]string) db.RawPackingRecord {
	record := db.RawPackingRecord{
		OBJECTCLAS: r.OBJECTCLAS,
		OBJECTID:   r.OBJECTID,
		USERNAME:   r.USERNAME,
		UDATE:      today,
		UTIME:      r.UTIME,
		TCODE:      r.TCODE,
		EXIDV:      r.EXIDV,
		BRGEW:      r.BRGEW,
		ZLAENG:     r.ZLAENG,
		ZBREIT:     r.ZBREIT,
		ZHOEHE:     r.ZHOEHE,
		VBELN:      strings.TrimSpace(r.VBELN),
		ROUTE:      strings.TrimSpace(r.ROUTE),
		LPRIO:      strings.TrimSpace(r.LPRIO),
		LGNUM:      strings.TrimSpace(r.LGNUM),
		ZNEST:      r.ZNEST,
	}

	// Map Flow
	if flow, ok := flowMap[r.ROUTE]; ok {
		if flow == "Y2-flow" {
			record.FLOW = "A-flow"
		} else {
			record.FLOW = flow
		}
	} else {
		record.FLOW = "NOT-MAPPED"
	}

	// Map Floor
	if floor, ok := p.floorMap[r.VLTYP]; ok {
		record.FLOOR = floor
	} else {
		record.FLOOR = "NOT-MAPPED"
	}

	// Map Operator
	username := strings.TrimSpace(strings.ToUpper(r.USERNAME))
	if realName, ok := p.operatorMap[username]; ok {
		record.OPERATOR = realName
	} else {
		record.OPERATOR = username
	}

	// Ensure UTIME is HH:MM:SS
	t := strings.ReplaceAll(record.UTIME, ":", "")
	if len(t) == 5 {
		t = "0" + t
	}
	if len(t) == 6 {
		record.UTIME = fmt.Sprintf("%s:%s:%s", t[0:2], t[2:4], t[4:6])
	}

	return record
}
