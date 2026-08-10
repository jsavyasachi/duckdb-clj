# Changes

## 0.1.0 (unreleased)

Initial release.

- `duckdb.types`: next.jdbc protocol extensions let DuckDB LIST / STRUCT /
  MAP columns round-trip as Clojure vectors and maps in both directions. STRUCT
  fields are keywordized on read and bind positionally on write. Absent keys
  throw. MAP keys keep their natural type. Untyped parameters use raw binding.
- `duckdb.core`: `memory-datasource` / `file-datasource`, `read-parquet` /
  `read-csv` with validated named options, `attach!` / `detach!`,
  `install-extension!` / `load-extension!`, `duckdb-version`.
