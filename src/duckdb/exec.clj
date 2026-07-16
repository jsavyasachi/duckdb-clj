(ns duckdb.exec
  "DuckDB-native query execution helpers over next.jdbc."
  (:require [duckdb.types]
            [next.jdbc :as jdbc]
            [next.jdbc.connection :as connection])
  (:import (java.sql Connection)
           (java.util Properties)
           (javax.sql DataSource)
           (org.duckdb DuckDBDriver)))

(set! *warn-on-reflection* true)

(defn- jdbc-url [ds]
  (cond
    (string? ds) ds
    (map? ds) (connection/jdbc-url ds)
    (instance? DataSource ds) (str ds)
    (instance? Connection ds)
    (throw (ex-info "Streaming mode must be enabled when the connection is opened"
                    {:duckdb/error :streaming-requires-datasource}))
    :else
    (throw (ex-info (str "Unsupported DuckDB streaming source: " (pr-str (class ds)))
                    {:duckdb/error :invalid-streaming-source
                     :source-class (.getName (class ds))}))))

(defn- streaming-connection ^Connection [ds]
  (let [properties (doto (Properties.)
                     (.setProperty DuckDBDriver/JDBC_STREAM_RESULTS "true"))
        con (.connect (DuckDBDriver.) (jdbc-url ds) properties)]
    (or con
        (throw (ex-info "Source is not a DuckDB JDBC URL"
                        {:duckdb/error :invalid-streaming-source})))))

(defn reduce-streaming
  "Reduces a query using DuckDB's native streaming result mode.

  sql is a next.jdbc SQL vector (or a SQL string). The Connection, statement,
  and ResultSet remain open until transduction finishes, including early
  termination via reduced. A pre-opened Connection cannot be used because
  JDBC_STREAM_RESULTS is a connection property."
  ([ds sql xf f init]
   (reduce-streaming ds sql nil xf f init))
  ([ds sql opts xf f init]
   (with-open [con (streaming-connection ds)]
     (transduce xf f init
                (jdbc/plan con (if (string? sql) [sql] sql) opts)))))
