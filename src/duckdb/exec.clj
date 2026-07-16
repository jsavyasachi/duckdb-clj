(ns duckdb.exec
  "DuckDB-native query execution helpers over next.jdbc."
  (:require [duckdb.types]
            [next.jdbc :as jdbc]
            [next.jdbc.connection :as connection])
  (:import (java.sql Connection)
           (java.util Properties)
           (javax.sql DataSource)
           (org.duckdb DuckDBChunkedResult DuckDBColumnType
                       DuckDBDataChunkReader DuckDBDriver
                       DuckDBPreparedStatement DuckDBReadableVector)))

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

(defn- unsupported-chunk-type [^DuckDBReadableVector vector]
  (let [type (.getType vector)]
    (throw (ex-info (str "Unsupported DuckDB chunk column type: " type)
                    {:duckdb/error :unsupported-chunk-type
                     :type (keyword (str type))}))))

(defn- chunk-value [^DuckDBReadableVector vector ^long row]
  (if (.isNull vector row)
    nil
    (let [type (.getType vector)]
      (condp = type
        DuckDBColumnType/BOOLEAN (.getBoolean vector row)
        DuckDBColumnType/TINYINT (.getByte vector row)
        DuckDBColumnType/UTINYINT (.getUint8 vector row)
        DuckDBColumnType/SMALLINT (.getShort vector row)
        DuckDBColumnType/USMALLINT (.getUint16 vector row)
        DuckDBColumnType/INTEGER (.getInt vector row)
        DuckDBColumnType/UINTEGER (.getUint32 vector row)
        DuckDBColumnType/BIGINT (.getLong vector row)
        DuckDBColumnType/UBIGINT (.getUint64 vector row)
        DuckDBColumnType/HUGEINT (.getHugeInt vector row)
        DuckDBColumnType/UHUGEINT (.getUHugeInt vector row)
        DuckDBColumnType/FLOAT (.getFloat vector row)
        DuckDBColumnType/DOUBLE (.getDouble vector row)
        DuckDBColumnType/DECIMAL (.getBigDecimal vector row)
        DuckDBColumnType/VARCHAR (.getString vector row)
        DuckDBColumnType/DATE (.getLocalDate vector row)
        DuckDBColumnType/TIMESTAMP (.getLocalDateTime vector row)
        DuckDBColumnType/TIMESTAMP_MS (.getLocalDateTime vector row)
        DuckDBColumnType/TIMESTAMP_NS (.getLocalDateTime vector row)
        DuckDBColumnType/TIMESTAMP_S (.getLocalDateTime vector row)
        DuckDBColumnType/TIMESTAMP_WITH_TIME_ZONE (.getOffsetDateTime vector row)
        (unsupported-chunk-type vector)))))

(defn- chunk->columns [^DuckDBChunkedResult result]
  (let [^DuckDBDataChunkReader chunk (.chunk result)
        row-count (.rowCount chunk)]
    (into {}
          (map (fn [column]
                 (let [^DuckDBReadableVector vector (.vector chunk column)]
                   [(keyword (.columnName result column))
                    (mapv #(chunk-value vector %) (range row-count))])))
          (range (.columnCount chunk)))))

(defn reduce-chunks
  "Reduces native columnar chunks from a prepared statement.

  Each chunk is a map of column-name keywords to value vectors. The native
  DuckDBChunkedResult is always closed before this function returns or throws."
  [^DuckDBPreparedStatement stmt f init]
  (with-open [^DuckDBChunkedResult result (.query stmt)]
    (loop [acc init]
      (if (.nextChunk result)
        (let [next-acc (f acc (chunk->columns result))]
          (if (reduced? next-acc)
            @next-acc
            (recur next-acc)))
        acc))))

(defn read-chunks
  "Returns native columnar chunks from a prepared statement.

  Each chunk is a map of column-name keywords to value vectors. All values
  are copied before the native DuckDBChunkedResult is closed."
  [^DuckDBPreparedStatement stmt]
  (reduce-chunks stmt conj []))
