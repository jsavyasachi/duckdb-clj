# duckdb-clj

[![Clojars Project](https://img.shields.io/clojars/v/net.clojars.savya/duckdb-clj.svg)](https://clojars.org/net.clojars.savya/duckdb-clj)
[![cljdoc](https://cljdoc.org/badge/net.clojars.savya/duckdb-clj)](https://cljdoc.org/d/net.clojars.savya/duckdb-clj/CURRENT)
[![test](https://github.com/jsavyasachi/duckdb-clj/actions/workflows/test.yml/badge.svg)](https://github.com/jsavyasachi/duckdb-clj/actions/workflows/test.yml)

DuckDB type coercion and helpers for Clojure over `next.jdbc`: LIST, STRUCT,
MAP, and ENUM columns round-trip as plain Clojure data, plus thin wrappers for
`read_parquet`, `read_csv`, `ATTACH`, extensions, and Appender bulk inserts.

## Stack

<a href="https://clojure.org"><img src="https://img.shields.io/badge/Clojure-5881D8?style=flat&logo=clojure&logoColor=fff" alt="Clojure" /></a>
<a href="https://duckdb.org"><img src="https://img.shields.io/badge/DuckDB-FFF000?style=flat&logo=duckdb&logoColor=000" alt="DuckDB" /></a>
<a href="https://github.com/seancorfield/next-jdbc"><img src="https://img.shields.io/badge/next.jdbc-5881D8?style=flat&logo=clojure&logoColor=fff" alt="next.jdbc" /></a>

## Installation

deps.edn:

```clojure
net.clojars.savya/duckdb-clj {:mvn/version "0.2.0"}
```

Leiningen:

```clojure
[net.clojars.savya/duckdb-clj "0.2.0"]
```

Bundles `org.duckdb/duckdb_jdbc` (the embedded database - no server) and
extends `next.jdbc`'s protocols on load.

### Arrow

`duckdb.arrow` is optional. Add mutually compatible Apache Arrow modules in an
alias rather than the base dependency set:

```clojure
{:aliases
 {:arrow
  {:extra-deps
   {org.apache.arrow/arrow-vector {:mvn/version "18.3.0"}
    org.apache.arrow/arrow-memory-core {:mvn/version "18.3.0"}
    org.apache.arrow/arrow-memory-unsafe {:mvn/version "18.3.0"}
    org.apache.arrow/arrow-c-data {:mvn/version "18.3.0"}}
   :jvm-opts ["--add-opens=java.base/java.nio=ALL-UNNAMED"]}}}
```

Run with the alias, for example `clojure -M:arrow`. The `--add-opens` runtime
flag is required for Arrow off-heap memory access on JDK 16 and newer.

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

Nesting works in both directions (LIST of STRUCT, STRUCT containing MAP, ...).
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

`append!` takes rows as maps and appends values in the table's declared column
order, not the map's iteration order. It uses DuckDB's Appender API and supports
common scalar values plus nested LIST and STRUCT values.

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

Option maps render to DuckDB named arguments (`{:union-by-name true}` →
`union_by_name = true`); option names are validated as identifiers and string
values are SQL-escaped.

## Semantics worth knowing

- **STRUCT** fields are keywordized on read (`{:name "alice"}`); on write, the
  Clojure map is bound positionally against the column's declared field order,
  and an absent field key throws (`:duckdb/error :missing-struct-field`) -
  explicit `nil` values are fine.
- **MAP** keys are *not* keywordized (DuckDB map keys can be any type, e.g.
  `MAP(INT, VARCHAR)` reads back as `{1 "x"}`).
- **In-memory databases**: each connection to a `(memory-datasource)` is a
  separate database. Hold one connection (`jdbc/get-connection`) for
  multi-statement work.
- Result keys are unqualified (`:id`, not `:events/id`) - DuckDB's JDBC driver
  does not report table names for result columns.
- The `java.util.Map` `ReadableColumn` extension is process-global across all
  `next.jdbc` usage in the JVM (it converts Java maps to Clojure maps - benign,
  but noted).
- **Bulk transfer**: `append!` uses DuckDB's Appender API for faster row ingest
  from Clojure maps. For bulk columnar extracts and very large analytical
  transfers, use
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

Everything runs against in-memory DuckDB - no services, nothing to download.

## License

Copyright © 2026 Savyasachi.

Distributed under the [Eclipse Public License 2.0](https://www.eclipse.org/legal/epl-2.0/).
