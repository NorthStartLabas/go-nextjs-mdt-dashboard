# Changelog

## [0.1.0] - 2026-03-17
### Added
- Initial project structure for Snowflake to SQLite extraction pipeline.
- SQLite database initialization and `routes` table creation.
- Snowflake connection test logic.
- Route upsert logic from `routes.csv`.
- Logic, Execution, and Configuration separation.

### Follow-up
Initialized the foundational architecture. Built decoupled modules for Snowflake (gosnowflake) and SQLite (modernc-org/sqlite). Implemented a robust route synchronization logic that ensures the local DB stays updated with `routes.csv`. Ready for Snowflake-to-SQLite data extraction.
