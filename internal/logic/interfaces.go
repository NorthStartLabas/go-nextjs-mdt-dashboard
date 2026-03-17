package logic

import (
	"context"
	"extraction-pipeline/internal/db"
)

// SnowflakeProvider defines what we need from Snowflake
type SnowflakeProvider interface {
	StreamPickingData(ctx context.Context, date string, recordChan chan<- db.LTAPUnifiedRecord, errChan chan<- error)
	StreamPackingData(ctx context.Context, date string, recordChan chan<- db.CDHDRUnifiedRecord, errChan chan<- error)
}

// PickingRepository defines SQLite operations for picking
type PickingRepository interface {
	GetFlowMap(ctx context.Context) (map[string]string, error)
	ClearPickingDate(ctx context.Context, date string) error
	BatchInsertPicking(ctx context.Context, records []db.RawPickingRecord) error
}

// PackingRepository defines SQLite operations for packing
type PackingRepository interface {
	GetFlowMap(ctx context.Context) (map[string]string, error)
	ClearPackingDate(ctx context.Context, date string) error
	BatchInsertPacking(ctx context.Context, records []db.RawPackingRecord) error
}

// ProductivityRepository defines operations for calculating analytics
type ProductivityRepository interface {
	GetHourlyPickingAggregation(ctx context.Context, date string) ([]db.PickingAggRow, error)
	InsertProductivity(ctx context.Context, date string, records []db.HourlyProductivityRecord) error
	GetHourlyProductivityRecords(ctx context.Context, date string) ([]db.HourlyProductivityRecord, error)
	InsertDailyProductivity(ctx context.Context, date string, records []db.DailyProductivityRecord) error
	GetHourlyPackingAggregation(ctx context.Context, date string) ([]db.PackingAggRow, error)
	InsertPackingProductivity(ctx context.Context, date string, records []db.HourlyPackingRecord) error
	GetHourlyPackingProductivityRecords(ctx context.Context, date string) ([]db.HourlyPackingRecord, error)
	InsertDailyPackingProductivity(ctx context.Context, date string, records []db.DailyPackingRecord) error
}

// RouteRepository defines operations for route sync
type RouteRepository interface {
	UpsertRoutes(ctx context.Context, routes [][]string) error
}
