# Changelog

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
Resolved a "failed to parse authenticator" issue by correctly trimming double quotes from the `snowflake_go_driver_connection_string.txt` file. Connection verification is now ready to proceed.
