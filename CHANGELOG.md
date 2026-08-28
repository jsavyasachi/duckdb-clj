# Changelog

All notable changes to this project are documented here. Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.8.0] - 2026-08-28

### Added

- Added opt-in `append!` support for using SQL defaults on omitted columns via
  `{:omitted-columns :default}`. Explicit `nil` remains SQL `NULL`.

## [0.7.0] - 2026-08-27

### Added

- Added `read-json` and `read-ndjson` helpers for JSON table-function ingestion.
- Added reusable-statement helpers for UUID, decimal, date, time, timestamp, and binary bindings.
- Added scalar UDF registration modes for `Function`, `BiFunction`, `Supplier`, and varargs callbacks.
- Added copy-paste README examples for execution, metadata, UDF, and Arrow APIs.
- Added Arrow failure-path cleanup coverage, broader native scalar coverage, and a CI DuckDB version axis.

## [0.6.0] - 2026-08-27

### Added

- Added `copy-to-parquet!`, `copy-to-csv!`, `copy-to-json!`, and generic
  `copy-to!` helpers for safe DuckDB `COPY ... TO` exports, including format
  options and Hive-partitioned output.

## [0.5.0] - 2026-08-27

### Added

- `read-chunks` and `reduce-chunks` now support UUID, JSON, BLOB, TIME, ENUM,
  LIST, STRUCT, and MAP result columns, including recursively converted nested
  values.

### Performance

- Queries containing UUID, JSON, BLOB, TIME, ENUM, LIST, STRUCT, or MAP columns
  use row-based JDBC `ResultSet` iteration batched into pseudo-chunks because
  `duckdb_jdbc` does not expose native chunk-vector accessors for these types.
  This fallback applies to the entire result set, while queries with only
  previously supported primitive types retain native chunked performance.

## [0.4.0] - 2026-08-17

### Changed

- `append-row!` now throws when a column is absent from the row map instead of
  silently appending NULL over a SQL default or NOT NULL constraint. An
  explicit `nil` still appends NULL.
- `reduce-streaming` reuses the datasource's configured connection (read-only
  mode, credentials, session-init SQL) instead of rebuilding it from the URL.

## [0.3.1] - 2026-08-01
### Changed
- Declare the optional Arrow integration dependencies in the pom so cljdoc can analyze the integration namespace. They remain non-transitive for consumers.

## [0.3.0] - 2026-07-16
### Added
- Broad parity pass over the capabilities the DuckDB JDBC driver (1.5.4.0) exposes. All additions are backward compatible.
- **core**: configurable `datasource` (read-only/access-mode, arbitrary settings, session-init SQL, auto-commit, user-agent, pin-db); `duplicate` for a second connection sharing the same database; a full Appender surface (catalog/schema-qualified targets, `byte[]` BLOB, DEFAULT/UNION values, primitive arrays with validity masks, explicit HUGEINT/UUID/temporal encodings); a single-value appender; and explicit process-global lifecycle controls (`release-db!`, cancel-scheduler shutdown, function-registry inspection).
- **`duckdb.exec`** (new): native streaming-result reduction (`reduce-streaming`), chunked columnar reads (`read-chunks`/`reduce-chunks`), reusable prepared-statement control with parameter/return metadata and typed HUGEINT binding, query progress/timeout/cancellation, and native profiling output.
- **`duckdb.arrow`** (new, optional): Apache Arrow result export (`arrow-export`/`with-arrow-reader`) and Arrow stream registration (`register-arrow!`) via the Arrow C Data interface. Arrow Java is an **optional** dependency (`:arrow` alias) and requires the `--add-opens=java.base/java.nio=ALL-UNNAMED` runtime flag; it is not pulled by base users.
- **`duckdb.udf`** (new): register Clojure functions as DuckDB scalar (`register-scalar!`) and table (`register-table!`) functions. Note: duckdb_jdbc 1.5.4.0 provides no working deregistration for Java UDFs, so registrations are process-global and persist for the lifetime of the JVM — use stable, unique names. Aggregate UDFs are not exposed by this driver.
- **`duckdb.meta`** (new): DuckDB-specific typed result accessors (HUGEINT/UHUGEINT, UUID, JSON node, lazy string, offset-time, BLOB, ARRAY, STRUCT) and JDBC metadata introspection (catalogs, schemas, tables, columns, keys, indexes, type info, driver capabilities) realized into Clojure data.

## [0.2.0] - 2026-07-11
### Added
- ENUM parameters can be written from Clojure keywords, with keyword names bound as DuckDB ENUM strings.
- `duckdb.core/append!` bulk-inserts map rows through DuckDB's Appender API, using table column order and supporting scalar values plus nested LIST/STRUCT values.

## [0.1.2] - 2026-07-09
### Fixed
- POM now includes the project description, homepage URL, and full SCM connection metadata, so Clojars shows a description/homepage and cljdoc has complete source-link data.
