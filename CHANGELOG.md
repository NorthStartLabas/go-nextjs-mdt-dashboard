# Changelog

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
Fixed a compilation error by adding the missing `strings` import. The system is now fully functional with the new floor mapping and data transformation features.
