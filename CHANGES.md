# Changes

## 0.1.0 (unreleased)

Initial release.

- `duckdb.types`: next.jdbc protocol extensions so DuckDB LIST / STRUCT /
  MAP columns round-trip as Clojure vectors and maps, recursively, in both
  directions. STRUCT fields keywordized on read and bound positionally on
  write (absent keys throw); MAP keys keep their natural type. Untyped
  parameters fall back to raw binding.
- `duckdb.core`: `memory-datasource` / `file-datasource`, `read-parquet` /
  `read-csv` with validated named options, `attach!` / `detach!`,
  `install-extension!` / `load-extension!`, `duckdb-version`.
