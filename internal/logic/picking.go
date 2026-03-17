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

// PickingProcessor orchestrates the Snowflake to SQLite picking pipeline
type PickingProcessor struct {
	snowflake   SnowflakeProvider
	sqlite      PickingRepository
	floorMap    map[string]string
	operatorMap map[string]string
}

// NewPickingProcessor initializes the processor with injected dependencies
func NewPickingProcessor(snowflake SnowflakeProvider, sqlite PickingRepository, floorMap map[string]string, operatorMap map[string]string) *PickingProcessor {
	return &PickingProcessor{
		snowflake:   snowflake,
		sqlite:      sqlite,
		floorMap:    floorMap,
		operatorMap: operatorMap,
	}
}

const (
	workerCount = 4
	batchSize   = 2000
)

// RunPicking performs the synchronized data pull and transformation
func (p *PickingProcessor) RunPicking(ctx context.Context) error {
	today := time.Now().Format("2006-01-02")
	snowflakeDate := time.Now().Format("20060102")
	slog.Info("starting pipelined picking extraction", "date", today, "snowflake_date", snowflakeDate)

	flowMap, err := p.sqlite.GetFlowMap(ctx)
	if err != nil {
		return fmt.Errorf("failed to load flow map: %w", err)
	}

	recordChan := make(chan db.LTAPUnifiedRecord, 1000)
	transformedChan := make(chan db.RawPickingRecord, 1000)
	errChan := make(chan error, 1)

	// 1. STAGE 1: PRODUCER (Snowflake Stream)
	// Reverted to dashed today (YYYY-MM-DD) as LTAP expects this format,
	// but kept query optimizations to prevent hang.
	go p.snowflake.StreamPickingData(ctx, today, recordChan, errChan)

	// 2. STAGE 2: WORKERS (Transformations)
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ltap := range recordChan {
				transformedChan <- p.transformPickingRecord(ltap, today, flowMap)
			}
		}()
	}

	// 3. STAGE 3: CONSUMER (SQLite Batch Writer)
	dbDone := make(chan error, 1)
	go func() {
		var batch []db.RawPickingRecord
		count := 0
		for record := range transformedChan {
			batch = append(batch, record)
			count++
			if count%1000 == 0 {
				slog.Info("picking extraction progress", "rows_processed", count)
			}

			if len(batch) >= batchSize {
				if err := p.sqlite.BatchInsertPicking(ctx, batch); err != nil {
					dbDone <- fmt.Errorf("batch insert failed: %w", err)
					return
				}
				batch = nil
			}
		}
		if len(batch) > 0 {
			if err := p.sqlite.BatchInsertPicking(ctx, batch); err != nil {
				dbDone <- fmt.Errorf("final batch insert failed: %w", err)
				return
			}
		}
		slog.Info("consumer finished processing all picking records", "total_rows", count)
		dbDone <- nil
	}()

	// Wait for workers to finish, then close transformed channel
	wg.Wait()
	close(transformedChan)

	// Handle errors
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

	slog.Info("picking extraction complete", "date", today)
	return nil
}

func (p *PickingProcessor) transformPickingRecord(ltap db.LTAPUnifiedRecord, today string, flowMap map[string]string) db.RawPickingRecord {
	// Standardize Date to YYYY-MM-DD for SQLite consistency
	qdatu := today

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

	// Nullable handling from LTAPUnifiedRecord
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

	// Map Flow
	if flow, ok := flowMap[ltap.ROUTE]; ok {
		if flow == "Y2-flow" {
			record.FLOW = "A-flow"
		} else {
			record.FLOW = flow
		}
	} else {
		record.FLOW = "NOT-MAPPED"
	}

	// Map Floor
	if floor, ok := p.floorMap[record.VLTYP]; ok {
		record.FLOOR = floor
	} else {
		record.FLOOR = "NOT-MAPPED"
	}

	// Map Operator
	username := strings.TrimSpace(strings.ToUpper(ltap.QNAME))
	if realName, ok := p.operatorMap[username]; ok {
		record.OPERATOR = realName
	} else {
		record.OPERATOR = username
	}

	return record
}
