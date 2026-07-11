# Changelog

All notable changes to this project are documented here. Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project adheres to [Semantic Versioning](https://semver.org/).

## [0.2.0] - 2026-07-11
### Added
- ENUM parameters can be written from Clojure keywords, with keyword names bound as DuckDB ENUM strings.
- `duckdb.core/append!` bulk-inserts map rows through DuckDB's Appender API, using table column order and supporting scalar values plus nested LIST/STRUCT values.

## [0.1.2] - 2026-07-09
### Fixed
- POM now includes the project description, homepage URL, and full SCM connection metadata, so Clojars shows a description/homepage and cljdoc has complete source-link data.
