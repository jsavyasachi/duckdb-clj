(ns duckdb.exec
  "DuckDB-native query execution helpers over next.jdbc."
  (:require [clojure.string :as str]
            [duckdb.types]
            [next.jdbc :as jdbc]
            [next.jdbc.connection :as connection]
            [next.jdbc.prepare :as prep])
  (:import (java.math BigInteger)
           (java.sql Connection ParameterMetaData ResultSetMetaData)
           (java.util Properties)
           (java.util.concurrent Executors ScheduledExecutorService
                                 ScheduledFuture ThreadFactory TimeUnit)
           (javax.sql DataSource)
           (org.duckdb DuckDBChunkedResult DuckDBColumnType
                       DuckDBConnection DuckDBDataChunkReader DuckDBDriver
                       DuckDBPreparedStatement DuckDBReadableVector
                       DuckDBResultSetMetaData ProfilerPrintFormat)))

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

(defn prepare
  "Creates a reusable native DuckDB prepared statement.

  The returned DuckDBPreparedStatement is AutoCloseable and should be scoped
  with with-open. Its Connection must remain open for the statement lifetime."
  ^DuckDBPreparedStatement [^Connection con sql]
  (when-not (string? sql)
    (throw (ex-info "Prepared SQL must be a string"
                    {:duckdb/error :invalid-sql
                     :sql sql})))
  (let [^DuckDBConnection duck-con (.unwrap con DuckDBConnection)]
    (.prepare duck-con sql)))

(defn parameter-metadata
  "Returns ordered JDBC parameter-type metadata for stmt."
  [^DuckDBPreparedStatement stmt]
  (let [^ParameterMetaData metadata (.getParameterMetaData stmt)]
    (mapv (fn [index]
            {:index index
             :type (.getParameterType metadata index)
             :type-name (.getParameterTypeName metadata index)
             :class-name (.getParameterClassName metadata index)
             :precision (.getPrecision metadata index)
             :scale (.getScale metadata index)
             :nullable (.isNullable metadata index)
             :signed? (.isSigned metadata index)})
          (range 1 (inc (.getParameterCount metadata))))))

(defn- return-type-keyword [return-type]
  (-> return-type str str/lower-case (str/replace "_" "-") keyword))

(defn return-metadata
  "Returns the native statement return type and ordered result columns."
  [^DuckDBPreparedStatement stmt]
  (let [^DuckDBResultSetMetaData metadata (.getMetaData stmt)
        ^ResultSetMetaData rsmeta metadata]
    {:return-type (return-type-keyword (.getReturnType metadata))
     :columns (mapv (fn [index]
                      {:index index
                       :name (.getColumnLabel rsmeta index)
                       :type-name (.getColumnTypeName rsmeta index)})
                    (range 1 (inc (.getColumnCount rsmeta))))}))

(defn set-fetch-size!
  "Sets the JDBC fetch size on stmt and returns stmt."
  [^DuckDBPreparedStatement stmt rows]
  (.setFetchSize stmt rows)
  stmt)

(defn bind-parameters!
  "Binds params in order on a reusable statement and returns stmt."
  [^DuckDBPreparedStatement stmt params]
  (prep/set-parameters stmt params)
  stmt)

(defn bind-hugeint!
  "Binds an integer as DuckDB HUGEINT at the one-based parameter index."
  [^DuckDBPreparedStatement stmt index value]
  (when-not (integer? value)
    (throw (ex-info (str "DuckDB HUGEINT value must be an integer: " (pr-str value))
                    {:duckdb/error :invalid-hugeint
                     :value value})))
  (.setBigInteger stmt index (if (instance? BigInteger value)
                               value
                               (biginteger value)))
  stmt)

(defn query-progress
  "Returns the current connection-local progress reported for stmt."
  [^DuckDBPreparedStatement stmt]
  (let [progress (.getQueryProgress stmt)]
    {:processed (.getRowsProcessed progress)
     :total (.getTotalRowsToProcess progress)
     :percentage (.getPercentage progress)}))

(defn set-query-timeout!
  "Sets stmt's JDBC query timeout in seconds and returns stmt."
  [^DuckDBPreparedStatement stmt seconds]
  (.setQueryTimeout stmt seconds)
  stmt)

(defn cancel!
  "Cancels stmt if it is currently executing and returns stmt.

  Call this from a different thread than the executing query."
  [^DuckDBPreparedStatement stmt]
  (.cancel stmt)
  stmt)

(defn- cancel-executor ^ScheduledExecutorService []
  (Executors/newSingleThreadScheduledExecutor
   (reify ThreadFactory
     (newThread [_ runnable]
       (doto (Thread. runnable "duckdb-exec-cancel")
         (.setDaemon true))))))

(defn execute-with-timeout!
  "Executes stmt and cancels it from another thread after timeout-ms.

  on-timeout, when supplied, receives stmt immediately before cancellation.
  The callback can inspect query-progress. SQLExceptions from the interrupted
  execution are propagated to the calling thread."
  ([stmt timeout-ms]
   (execute-with-timeout! stmt timeout-ms nil))
  ([^DuckDBPreparedStatement stmt timeout-ms on-timeout]
   (when (neg? timeout-ms)
     (throw (ex-info (str "Timeout must be non-negative: " timeout-ms)
                     {:duckdb/error :invalid-timeout
                      :timeout-ms timeout-ms})))
   (let [^ScheduledExecutorService executor (cancel-executor)
         task (reify Runnable
                (run [_]
                  (try
                    (when on-timeout
                      (on-timeout stmt))
                    (finally
                      (cancel! stmt)))))
         ^ScheduledFuture scheduled (.schedule executor task (long timeout-ms)
                                                TimeUnit/MILLISECONDS)]
     (try
       (.execute stmt)
       (finally
         (.cancel scheduled false)
         (.shutdownNow executor))))))

(defn profiling-information
  "Returns native connection-local profiling output in format.

  format is :query-tree, :json, :optimizer, :html, or :graphviz. Profiling
  must first be enabled on the same Connection with DuckDB SQL configuration,
  such as SET enable_profiling = 'no_output'."
  [^Connection con format]
  (let [print-format ({:query-tree ProfilerPrintFormat/QUERY_TREE
                       :json ProfilerPrintFormat/JSON
                       :optimizer ProfilerPrintFormat/QUERY_TREE_OPTIMIZER
                       :html ProfilerPrintFormat/HTML
                       :graphviz ProfilerPrintFormat/GRAPHVIZ}
                      format)]
    (when-not print-format
      (throw (ex-info (str "Invalid DuckDB profiling format: " format)
                      {:duckdb/error :invalid-profile-format
                       :format format})))
    (let [^DuckDBConnection duck-con (.unwrap con DuckDBConnection)]
      (.getProfilingInformation duck-con print-format))))
