# Changelog

## [0.11.0] - 2026-03-17
### Added
- **Daily Productivity Analytics**:
    - New `daily_picking_productivity` table for aggregated daily performance.
    - Automatic aggregation of hourly metrics into daily totals per operator, LGNUM, flow, and floor.
    - Recalculated daily base productivity and intensity indices for accurate cohort comparison.

## [0.10.1] - 2026-03-17
### Changed
- Standardized all productivity metrics to 2 decimal places for cleaner reporting.
- Applied rounding to weight, volume, intensity indices, and productivity values.

## [0.10.0] - 2026-03-17
### Added
- **Advanced Productivity Analytics**:
    - New `hourly_picking_productivity` table for granular operational insights.
    - Context-aware time splitting: Automatically adjusts effective hours when operators switch LGNUM, Flow, or Floor within the same hour.
    - Integrated Break Logic via `breaks_config.json`: Allows LGNUM-specific break deductions.
    - **Intensity Multipliers**: Peer-reviewed multipliers for Weight and Item counts compared to cohort averages.
    - Adjusted Productivity calculation using combined intensity indices.
    - Automatic volume conversion from `ccm` to `m3`.

## [0.9.1] - 2026-03-17
### Fixed
- Snowflake numeric casting bug in unified queries: Added explicit `CAST(LPRIO AS VARCHAR)` to prevent conversion errors during server-side JOINs.
- Defensive time string slicing in `PickingProcessor`.
### Added
- Unified Snowflake streaming with server-side JOINs for both Picking and Packing.
- Explicit "Wipe-on-Write" policy in SQLite to ensure only latest date data is kept.
### Changed
- Renamed `extraction.go` to `picking.go`.
- Optimized Packing extraction from 3 Snowflake queries per batch to 1 unified JOIN.
- Reduced memory overhead by streaming directly from Snowflake to SQLite insertion buffers.

## [0.8.1] - 2026-03-17
### Fixed
- Operator mapping now handles case-sensitivity and trailing spaces for both Picking and Packing IDs.
- Added missing `strings` import in `packing.go`.

## [0.8.0] - 2026-03-17
### Added
- Operator mapping via `operator_mapping.json`: Maps `QNAME` (Picking) and `USERNAME` (Packing) to real operator names in a new `operator` column.
### Changed
- Packing data transformations:
    - `udate`: Transformed from `YYYYMMDD` to `YYYY-MM-DD`.
    - `utime`: Transformed from `HHMMSS` to `HH:MM:SS`.

## [0.7.0] - 2026-03-17
### Changed
- Packing extraction: Added server-side row skipping to only keep records where `LGNUM` is `245` or `266`.
- Packing extraction: Added guard clauses to skip records without VEKP or Link metadata.

## [0.6.1] - 2026-03-17
### Fixed
- Robust NULL handling for packing data (VEKP dimensions and ZORF links).
- Fixed "converting NULL to string" error in `ZNEST` and other nullable columns.

## [0.6.0] - 2026-03-17
### Added
- Concurrent extraction execution: Picking and Packing now run in parallel using goroutines and `sync.WaitGroup`.
- Error aggregation for parallel tasks.
### Removed
- `VASSAP` and `VASNOSAP` columns from `raw_packing` as they are not present in Snowflake source.

## [0.5.0] - 2026-03-17
### Added
- Packing extraction pipeline:
    - Queries `SDS_CP_CDHDR` for `ZORF_BOX_CLOSING` activity.
    - Joins with `SDS_CP_VEKP` to fetch HU dimensions and external IDs.
    - Joins with `ZORF` tables to link HUs to deliveries and routes.
- New `raw_packing` table in SQLite with 18 consolidated columns.
- Support for streaming `CDHDR` headers for packing extraction.

## [0.4.1] - 2026-03-17
### Fixed
- Missing `strings` package import in `extraction.go` causing compilation errors.

## [0.4.0] - 2026-03-17
### Added
- Floor mapping configuration (`floor_mapping.json`) to map `VLTYP` to human-readable floor names.
- New `floor` column in `raw_picking` table.
### Changed
- Date/Time transformations:
    - `qdatu`: Truncated to `YYYY-MM-DD`.
    - `qzeit`: Extracted time portion `HH:MM:SS`.
- Flow logic override: Automatically converts 'Y2-flow' to 'A-flow'.

## [0.3.0] - 2026-03-17
### Added
- Concurrent data streaming from Snowflake using Go channels and goroutines.
- Robust NULL value handling for Snowflake columns using `sql.NullString` and `sql.NullFloat64`.
### Changed
- Removed redundant connection tests to speed up execution.
- Optimized `LTAP` processing to stream records for mapping.

## [0.2.0] - 2026-03-17
### Added
- `raw_picking` table in SQLite for consolidated data storage.
- Extraction logic to fetch data from Snowflake `SDS_CP_LTAP`.
- Join logic with Snowflake `ZORF` tables for Route mapping.
- Integration with local `routes` table to map `FLOW` based on `ROUTE`.
- Support for chunked/multiple table lookups for `VBELN` in Snowflake.

## [0.1.1] - 2026-03-17
### Fixed
- Connection string parsing logic to handle surrounding double quotes.

## [0.1.0] - 2026-03-17
### Added
- Initial project structure for Snowflake to SQLite extraction pipeline.
- SQLite database initialization and `routes` table creation.
- Snowflake connection test logic.
- Route upsert logic from `routes.csv`.
- Logic, Execution, and Configuration separation.

### Follow-up
Implemented `CalculateDailyProductivity` to aggregate hourly metrics into a new `daily_picking_productivity` table. This allows for high-level performance tracking without losing the granular intensity context, as indices are recalculated against the daily cohort averages. This ensures that the "Daily Productivity" is not just a sum of averages, but a statistically sound daily metric.
