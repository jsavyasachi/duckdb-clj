(ns duckdb.exec
  "DuckDB-native query execution helpers over next.jdbc."
  (:require [clojure.string :as str]
            [duckdb.core :as duckdb]
            [duckdb.types]
            [next.jdbc :as jdbc]
            [next.jdbc.prepare :as prep])
  (:import (java.math BigDecimal BigInteger)
           (java.sql Array Connection ParameterMetaData ResultSet ResultSetMetaData Struct)
           (java.sql Date Time Timestamp)
           (java.time LocalDate LocalDateTime LocalTime)
           (java.util Map Properties)
           (java.util.concurrent Executors ScheduledExecutorService
                                 ScheduledFuture ThreadFactory TimeUnit)
           (org.duckdb DuckDBChunkedResult DuckDBColumnType JsonNode
                       DuckDBConnection DuckDBDataChunkReader DuckDBDriver
                       DuckDBPreparedStatement DuckDBReadableVector
                       DuckDBResultSetMetaData ProfilerPrintFormat)))

(set! *warn-on-reflection* true)

(defn- jdbc-url ^String [ds]
  (cond
    (string? ds) ds
    (instance? Connection ds)
    (throw (ex-info "Streaming mode must be enabled when the connection is opened"
                    {:duckdb/error :streaming-requires-datasource}))
    :else
    (throw (ex-info (str "Unsupported DuckDB streaming source: " (pr-str (class ds)))
                    {:duckdb/error :invalid-streaming-source
                     :source-class (.getName (class ds))}))))

(defn- streaming-connection ^Connection [ds]
  (let [con (or (duckdb/open-streaming-connection ds)
                (when (string? ds)
                  (.connect (DuckDBDriver.) (jdbc-url ds)
                            (doto (Properties.)
                              (.setProperty DuckDBDriver/JDBC_STREAM_RESULTS "true"))))
                (throw (ex-info
                        "Streaming cannot preserve the configuration of a foreign datasource"
                        {:duckdb/error :unsupported-streaming-datasource
                         :source-class (.getName (class ds))})))]
    (or con
        (throw (ex-info "Source is not a DuckDB JDBC URL"
                        {:duckdb/error :invalid-streaming-source})))))

(defn reduce-streaming
  "Reduces a query with DuckDB's native streaming result mode.

  sql is a next.jdbc SQL vector (or a SQL string). The Connection, statement,
  and ResultSet remain open until transduction finishes, including early
  termination via reduced. A pre-opened Connection cannot be used because
  JDBC_STREAM_RESULTS is a connection property. Strings and datasources
  created by duckdb.core are supported; foreign datasource implementations
  are rejected because their connection configuration cannot be preserved."
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

(def ^:private native-chunk-type-names
  #{"BOOLEAN" "TINYINT" "UTINYINT" "SMALLINT" "USMALLINT" "INTEGER"
    "UINTEGER" "BIGINT" "UBIGINT" "HUGEINT" "UHUGEINT" "FLOAT"
    "DOUBLE" "DECIMAL" "VARCHAR" "DATE" "TIMESTAMP" "TIMESTAMP_MS"
    "TIMESTAMP_NS" "TIMESTAMP_S" "TIMESTAMP WITH TIME ZONE"})

(defn- native-chunk-type? [^String type-name]
  (contains? native-chunk-type-names (str/upper-case (str/trim type-name))))

(declare jdbc-value->clj)

(defn- jdbc-array->vector [^Array value]
  (mapv jdbc-value->clj (seq (object-array (.getArray value)))))

(defn- jdbc-struct->map [^Struct value]
  (if (instance? org.duckdb.DuckDBStruct value)
    (into {}
          (map (fn [[k v]] [(keyword k) (jdbc-value->clj v)]))
          (.getMap ^org.duckdb.DuckDBStruct value))
    (into {}
          (map-indexed (fn [index attribute]
                         [(keyword (str index)) (jdbc-value->clj attribute)]))
          (.getAttributes value))))

(defn- jdbc-value->clj [value]
  (cond
    (instance? JsonNode value) (.toString ^JsonNode value)
    (instance? Array value) (jdbc-array->vector value)
    (instance? Struct value) (jdbc-struct->map value)
    (instance? Map value) (into {} (map (fn [[k v]] [k (jdbc-value->clj v)])) value)
    :else value))

(defn- rows->columns [rows]
  (into {}
        (map (fn [column]
               [column (mapv column rows)]))
        (keys (first rows))))

(defn- jdbc-row [^ResultSet rs ^ResultSetMetaData metadata column-count]
  (into {}
        (map (fn [column]
               (let [^int column column
                     type-name (.getColumnTypeName metadata column)
                     value (if (= "BLOB" (str/upper-case type-name))
                             (.getBytes rs column)
                             (.getObject rs column))]
                 [(keyword (.getColumnLabel metadata column))
                  (jdbc-value->clj value)])))
        (range 1 (inc column-count))))

(defn- reduce-jdbc-chunks [^DuckDBPreparedStatement stmt f init]
  (with-open [^ResultSet rs (.executeQuery stmt)]
    (let [^ResultSetMetaData metadata (.getMetaData rs)
          column-count (.getColumnCount metadata)]
      (loop [acc init
             rows []]
        (if (.next rs)
          (let [next-rows (conj rows (jdbc-row rs metadata column-count))]
            (if (= 2048 (count next-rows))
              (let [next-acc (f acc (rows->columns next-rows))]
                (if (reduced? next-acc)
                  @next-acc
                  (recur next-acc [])))
              (recur acc next-rows)))
          (if (seq rows)
            (let [next-acc (f acc (rows->columns rows))]
              (if (reduced? next-acc) @next-acc next-acc))
            acc))))))

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
  (let [metadata (.getMetaData stmt)
        column-count (.getColumnCount metadata)
        has-unsupported? (some (fn [column]
                                 (not (native-chunk-type?
                                       (.getColumnTypeName metadata column))))
                               (range 1 (inc column-count)))]
    (if has-unsupported?
      (reduce-jdbc-chunks stmt f init)
      (with-open [^DuckDBChunkedResult result (.query stmt)]
        (loop [acc init]
          (if (.nextChunk result)
            (let [next-acc (f acc (chunk->columns result))]
              (if (reduced? next-acc)
                @next-acc
                (recur next-acc)))
            acc))))))

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
  "Returns JDBC parameter-type metadata in order for stmt."
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
  "Returns the native statement return type and result columns in order."
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
  "Binds params in order on a reusable statement. Returns stmt."
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

(defn bind-uuid!
  "Binds a UUID at the one-based parameter index." 
  [^DuckDBPreparedStatement stmt index ^java.util.UUID value]
  (.setObject stmt index value)
  stmt)

(defn bind-decimal!
  "Binds a BigDecimal at the one-based parameter index."
  [^DuckDBPreparedStatement stmt index value]
  (.setBigDecimal stmt index ^BigDecimal value)
  stmt)

(defn bind-date!
  "Binds a LocalDate or java.sql.Date at the one-based parameter index."
  [^DuckDBPreparedStatement stmt index value]
  (.setDate stmt index (if (instance? LocalDate value)
                         (Date/valueOf ^LocalDate value)
                         ^Date value))
  stmt)

(defn bind-time!
  "Binds a LocalTime or java.sql.Time at the one-based parameter index."
  [^DuckDBPreparedStatement stmt index value]
  (.setTime stmt index (if (instance? LocalTime value)
                         (Time/valueOf ^LocalTime value)
                         ^Time value))
  stmt)

(defn bind-timestamp!
  "Binds a LocalDateTime or java.sql.Timestamp at the one-based parameter index."
  [^DuckDBPreparedStatement stmt index value]
  (.setTimestamp stmt index (if (instance? LocalDateTime value)
                              (Timestamp/valueOf ^LocalDateTime value)
                              ^Timestamp value))
  stmt)

(defn bind-bytes!
  "Binds a byte array at the one-based parameter index."
  [^DuckDBPreparedStatement stmt index ^bytes value]
  (.setBytes stmt index value)
  stmt)

(defn query-progress
  "Returns the current connection-local progress for stmt."
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
  "Cancels stmt if it executes. Returns stmt.

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
  "Executes stmt. Cancels it from another thread after timeout-ms.

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
         gate (Object.)
         active (atom true)
         task (reify Runnable
                (run [_]
                  (locking gate
                    (when @active
                      (when on-timeout
                        (on-timeout stmt))
                      (cancel! stmt)))))
         ^ScheduledFuture scheduled (.schedule executor task (long timeout-ms)
                                                TimeUnit/MILLISECONDS)]
     (try
       (.execute stmt)
       (finally
         (locking gate
           (reset! active false))
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
