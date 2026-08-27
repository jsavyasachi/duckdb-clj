# duckdb-clj

[![Clojars Project](https://img.shields.io/clojars/v/net.clojars.savya/duckdb-clj.svg)](https://clojars.org/net.clojars.savya/duckdb-clj)
[![cljdoc](https://cljdoc.org/badge/net.clojars.savya/duckdb-clj)](https://cljdoc.org/d/net.clojars.savya/duckdb-clj/CURRENT)
[![test](https://github.com/jsavyasachi/duckdb-clj/actions/workflows/test.yml/badge.svg)](https://github.com/jsavyasachi/duckdb-clj/actions/workflows/test.yml)

DuckDB type coercion and helpers for Clojure over `next.jdbc`. Chunked reads
cover UUID, JSON, BLOB, TIME, ENUM, LIST, STRUCT, and MAP columns, converting
them to ordinary Clojure and Java values. The library also has wrappers for
`read_parquet`, `read_csv`, `ATTACH`, extensions, and Appender bulk inserts.

`read-chunks` and `reduce-chunks` use row-based JDBC `ResultSet` iteration,
batched into pseudo-chunks, whenever a query result contains a UUID, JSON, BLOB,
TIME, ENUM, LIST, STRUCT, or MAP column. DuckDB's Java driver
(`duckdb_jdbc`) does not expose native chunk-vector accessors for these types,
so this fallback applies to the entire result set, including other columns in a
mixed-type query. Queries containing only the previously supported primitive
types are unaffected and retain full native chunked performance.

## Stack

<a href="https://clojure.org"><img src="https://img.shields.io/badge/Clojure-5881D8?style=flat&logo=clojure&logoColor=fff" alt="Clojure" /></a>
<a href="https://duckdb.org"><img src="https://img.shields.io/badge/DuckDB-FFF000?style=flat&logo=duckdb&logoColor=000" alt="DuckDB" /></a>
<a href="https://github.com/seancorfield/next-jdbc"><img src="https://img.shields.io/badge/next.jdbc-5881D8?style=flat&logo=clojure&logoColor=fff" alt="next.jdbc" /></a>

## Installation

deps.edn:

```clojure
net.clojars.savya/duckdb-clj {:mvn/version "0.4.0"}
```

Leiningen:

```clojure
[net.clojars.savya/duckdb-clj "0.4.0"]
```

The library bundles `org.duckdb/duckdb_jdbc`, an embedded database with no
server. It extends `next.jdbc` protocols on load.

### Arrow

`duckdb.arrow` is optional. Add compatible Apache Arrow modules in an alias,
not the base dependency set:

```clojure
{:aliases
 {:arrow
  {:extra-deps
   {org.apache.arrow/arrow-vector {:mvn/version "19.0.0"}
    org.apache.arrow/arrow-memory-core {:mvn/version "19.0.0"}
    org.apache.arrow/arrow-memory-unsafe {:mvn/version "19.0.0"}
    org.apache.arrow/arrow-c-data {:mvn/version "19.0.0"}}
   :jvm-opts ["--add-opens=java.base/java.nio=ALL-UNNAMED"]}}}
```

Run with the alias, for example `clojure -M:arrow`. Arrow off-heap memory access
on JDK 16 and later requires the `--add-opens` runtime flag.

## Usage

```clojure
(require '[duckdb.core :as duck]   ; requiring this also activates the type coercion
         '[next.jdbc :as jdbc])

(def ds (duck/file-datasource "analytics.db"))   ; or (duck/memory-datasource)

(jdbc/execute! ds ["create type mood as enum ('sad', 'ok', 'happy')"])

(jdbc/execute! ds ["create table events (
                      id int,
                      tags varchar[],
                      user struct(name varchar, age int),
                      counts map(varchar, int),
                      mood mood)"])

;; write: plain Clojure data binds into LIST / STRUCT / MAP / ENUM parameters
(jdbc/execute! ds ["insert into events values (?, ?, ?, ?, ?)"
                   1
                   ["signup" "mobile"]
                   {:name "alice" :age 30}
                   {"clicks" 12 "views" 40}
                   :happy])

;; read: they come back as Clojure data
(jdbc/execute! ds ["select * from events"])
;; => [{:id 1
;;      :tags ["signup" "mobile"]
;;      :user {:name "alice" :age 30}
;;      :counts {"clicks" 12 "views" 40}
;;      :mood "happy"}]
```

Nesting works in both directions: LIST of STRUCT and STRUCT with MAP.
ENUM columns read as strings; write keywords or strings as parameters.

### Bulk append

```clojure
(jdbc/execute! ds ["create table metrics (
                     id int,
                     name varchar,
                     score double,
                     tags varchar[],
                     info struct(source varchar, batch int))"])

(duck/append!
 ds
 :metrics
 [{:name "alpha" :score 1.5 :id 1
   :tags ["daily" "mobile"]
   :info {:source "api" :batch 7}}
  {:id 2 :name "beta" :score 2.25
   :tags []
   :info {:source "job" :batch 8}}])
;; => 2
```

`append!` takes rows as maps. It appends values in the table's declared column
order, not map iteration order. It uses DuckDB's Appender API. It supports
common scalar values and nested LIST and STRUCT values.

### File helpers

```clojure
(duck/read-parquet ds "data/*.parquet")
(duck/read-parquet ds "data/*.parquet" {:union-by-name true :file-row-number true})
(duck/read-csv ds "data.csv" {:header true})

(duck/attach! ds "other.db" "other")          ; then: select * from other.t
(duck/attach! ds "other.db" "other" {:read-only true})
(duck/detach! ds "other")

(duck/install-extension! ds "httpfs")
(duck/load-extension! ds "httpfs")
(duck/duckdb-version ds)
```

Option maps render as DuckDB named arguments (`{:union-by-name true}` →
`union_by_name = true`). The library validates option names as identifiers and
SQL-escapes string values.

## Semantics worth knowing

- **STRUCT** fields are keywordized on read (`{:name "alice"}`); on write, the
  Clojure map binds positionally to the column's declared field order. An absent
  field key throws (`:duckdb/error :missing-struct-field`). Explicit `nil` values are valid.
- **MAP** keys are *not* keywordized (DuckDB map keys can be any type, e.g.
  `MAP(INT, VARCHAR)` reads back as `{1 "x"}`).
- **In-memory databases**: Each connection to a `(memory-datasource)` is a
  separate database. Use one connection (`jdbc/get-connection`) for
  multi-statement work.
- Result keys are unqualified (`:id`, not `:events/id`). DuckDB's JDBC driver
  does not report table names for result columns.
- The `java.util.Map` `ReadableColumn` extension is process-global across all
  `next.jdbc` usage in the JVM. It converts Java maps to Clojure maps.
- **Bulk transfer**: `append!` uses DuckDB's Appender API for row ingest from
  Clojure maps. For bulk columnar extracts and large analytical transfers, use
  [tmducken](https://github.com/techascent/tmducken) (DuckDB C API ->
  tech.ml.dataset) or DuckDB's
  [ADBC](https://duckdb.org/docs/current/clients/adbc) client instead.

Errors are `ex-info` maps keyed `:duckdb/error`
(`:missing-struct-field`, `:invalid-option`, `:invalid-alias`,
`:append-failed`).

## Running tests

```bash
clojure -M:test
```

All tests run against in-memory DuckDB. No services are needed.

## License

Copyright © 2026 Savyasachi.

Distributed under the [Eclipse Public License 2.0](https://www.eclipse.org/legal/epl-2.0/).
