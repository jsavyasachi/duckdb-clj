(ns duckdb.core
  "Helpers for DuckDB-specific next.jdbc operations."
  (:require [clojure.string :as str]
            [duckdb.types :as types]
            [next.jdbc :as jdbc])
  (:import (java.io PrintWriter)
           (java.math BigDecimal BigInteger)
           (java.sql Connection ResultSetMetaData SQLException SQLFeatureNotSupportedException Statement)
           (java.time LocalDate LocalDateTime LocalTime OffsetDateTime OffsetTime)
           (java.util Collection Collections Date LinkedHashMap Map Properties UUID WeakHashMap)
           (java.util.logging Logger)
           (javax.sql DataSource)
           (org.duckdb DuckDBAppender DuckDBConnection DuckDBDriver
                       DuckDBFunctions$RegisteredFunction DuckDBSingleValueAppender)))

(set! *warn-on-reflection* true)

(def ^:private simple-option-pattern #"^[a-z0-9_]+$")
(def ^:private identifier-pattern #"^[A-Za-z_][A-Za-z0-9_]*$")
(def ^:private datasource-option-keys
  #{:access-mode :auto-commit :pin-db :read-only :session-init-sql :settings :user-agent})
(def ^:private connection-session-init
  (Collections/synchronizedMap (WeakHashMap.)))
(def ^:private datasource-connect
  (Collections/synchronizedMap (WeakHashMap.)))

(defn- invalid-datasource-option [message data]
  (throw (ex-info message (assoc data :duckdb/error :invalid-datasource-option))))

(defn- property-name [k]
  (let [property (str/replace (name k) "-" "_")]
    (when-not (re-matches simple-option-pattern property)
      (invalid-datasource-option
       (str "Invalid DuckDB connection setting: " k)
       {:setting k}))
    property))

(defn- property-value [value]
  (cond
    (true? value) "true"
    (false? value) "false"
    (keyword? value) (name value)
    :else (str value)))

(defn- access-mode-value [access-mode]
  (case access-mode
    :read-only DuckDBDriver/DUCKDB_ACCESS_MODE_READ_ONLY
    :read-write DuckDBDriver/DUCKDB_ACCESS_MODE_READ_WRITE
    :automatic DuckDBDriver/DUCKDB_ACCESS_MODE_AUTOMATIC
    (let [value (str/upper-case
                 (str/replace (if (or (keyword? access-mode) (symbol? access-mode))
                                (name access-mode)
                                (str access-mode))
                              "-" "_"))]
      (when-not (#{DuckDBDriver/DUCKDB_ACCESS_MODE_READ_ONLY
                   DuckDBDriver/DUCKDB_ACCESS_MODE_READ_WRITE
                   DuckDBDriver/DUCKDB_ACCESS_MODE_AUTOMATIC}
                 value)
        (invalid-datasource-option
         (str "Invalid DuckDB access mode: " access-mode)
         {:option :access-mode :value access-mode}))
      value)))

(defn- connection-properties [opts]
  (let [unknown (seq (remove datasource-option-keys (keys opts)))]
    (when unknown
      (invalid-datasource-option
       (str "Unknown DuckDB datasource option: " (first unknown))
       {:option (first unknown)})))
  (let [properties (Properties.)]
    (doseq [[k v] (:settings opts)]
      (.setProperty properties (property-name k) (property-value v)))
    (when (contains? opts :read-only)
      (.setProperty properties DuckDBDriver/DUCKDB_READONLY_PROPERTY
                    (property-value (:read-only opts))))
    (when (contains? opts :access-mode)
      (.setProperty properties DuckDBDriver/DUCKDB_ACCESS_MODE_PROPERTY
                    (access-mode-value (:access-mode opts))))
    (when (contains? opts :auto-commit)
      (.setProperty properties DuckDBDriver/JDBC_AUTO_COMMIT
                    (property-value (:auto-commit opts))))
    (when (contains? opts :pin-db)
      (.setProperty properties DuckDBDriver/JDBC_PIN_DB
                    (property-value (:pin-db opts))))
    (when (contains? opts :user-agent)
      (.setProperty properties DuckDBDriver/DUCKDB_USER_AGENT_PROPERTY
                    (str (:user-agent opts))))
    properties))

(defn- initialize-connection! [^Connection con session-init-sql]
  (when session-init-sql
    (try
      (with-open [^Statement stmt (.createStatement con)]
        (.execute stmt ^String session-init-sql))
      (.put ^Map connection-session-init con session-init-sql)
      (catch Throwable cause
        (.close con)
        (throw cause))))
  con)

(defn datasource
  "Returns a DuckDB datasource for jdbc-url. Uses java.util.Properties.

  Options are :read-only, :access-mode (:read-only, :read-write, or
  :automatic), :settings, :session-init-sql, :auto-commit, :user-agent, and
  :pin-db."
  ([jdbc-url]
   (datasource jdbc-url nil))
  ([jdbc-url opts]
   (let [opts (or opts {})
         properties (connection-properties opts)
         session-init-sql (:session-init-sql opts)
         driver (DuckDBDriver.)
         connect (fn [extra-properties]
                   (let [^Properties props (.clone ^Properties properties)]
                     (doseq [[k v] extra-properties]
                       (.setProperty ^Properties props k v))
                     (initialize-connection!
                      (.connect driver (str jdbc-url) ^Properties props)
                      session-init-sql)))
         source (reify DataSource
                  (getConnection [_]
                    (connect nil))
                  (getConnection [_ username password]
                    (connect {"user" (str username) "password" (str password)}))
                  (getLogWriter [_] nil)
                  (^void setLogWriter [_ ^PrintWriter _writer])
                  (getLoginTimeout [_] 0)
                  (^void setLoginTimeout [_ ^int _seconds])
                  (getParentLogger [_] (Logger/getGlobal))
                  (isWrapperFor [this iface] (.isInstance ^Class iface this))
                  (unwrap [this iface]
                    (if (.isInstance ^Class iface this)
                      (.cast ^Class iface this)
                      (throw (SQLFeatureNotSupportedException.
                              (str "Cannot unwrap DuckDB datasource to " (.getName ^Class iface)))))))]
     (.put ^Map datasource-connect source connect)
     source)))

(defn open-streaming-connection
  "Opens ds with its configured properties plus DuckDB streaming results.

  Returns nil for datasource implementations not created by `datasource`."
  ^Connection [ds]
  (when-let [connect (.get ^Map datasource-connect ds)]
    (connect {DuckDBDriver/JDBC_STREAM_RESULTS "true"})))

(defn memory-datasource
  "Returns a next.jdbc datasource for an in-memory DuckDB database.

  Each memory datasource connection is a SEPARATE in-memory database. Use one
  connection with next.jdbc/get-connection for multi-statement work."
  ([]
   (memory-datasource nil))
  ([opts]
   (datasource "jdbc:duckdb:" opts)))

(defn file-datasource
  "Returns a next.jdbc datasource for a DuckDB database at path."
  ([path]
   (file-datasource path nil))
  ([path opts]
   (datasource (str "jdbc:duckdb:" path) opts)))

(defn- sql-string [value]
  (str "'" (str/replace (str value) "'" "''") "'"))

(defn- option-name [k]
  (let [option (str/replace (name k) "-" "_")]
    (when-not (re-matches simple-option-pattern option)
      (throw (ex-info (str "Invalid DuckDB table function option: " k)
                      {:duckdb/error :invalid-option
                       :option k})))
    option))

(defn- option-value [v]
  (cond
    (string? v) (sql-string v)
    (or (true? v) (false? v)) (str v)
    (number? v) (str v)
    :else (sql-string v)))

(defn- table-function-sql [function-name opts]
  (let [options (map (fn [[k v]]
                       (str (option-name k) " = " (option-value v)))
                     opts)]
    (str "select * from " function-name "(?"
         (when (seq options)
           (str ", " (str/join ", " options)))
         ")")))

(defn- read-table-function [ds function-name path opts]
  (jdbc/execute! ds [(table-function-sql function-name opts) path]))

(defn read-parquet
  "Reads path through DuckDB read_parquet and returns next.jdbc result rows."
  ([ds path]
   (read-parquet ds path nil))
  ([ds path opts]
   (read-table-function ds "read_parquet" path opts)))

(defn read-csv
  "Reads path through DuckDB read_csv and returns next.jdbc result rows."
  ([ds path]
   (read-csv ds path nil))
  ([ds path opts]
   (read-table-function ds "read_csv" path opts)))

(defn read-json
  "Reads a JSON array through DuckDB read_json and returns result rows."
  ([ds path]
   (read-json ds path nil))
  ([ds path opts]
   (read-table-function ds "read_json" path opts)))

(defn read-ndjson
  "Reads newline-delimited JSON through DuckDB read_json and returns rows."
  ([ds path]
   (read-ndjson ds path nil))
  ([ds path opts]
   (read-table-function ds "read_json" path (assoc (or opts {}) :format "newline_delimited"))))

(declare identifier)

(def ^:private copy-formats #{:parquet :csv :json})
(def ^:private parquet-compressions
  #{:uncompressed :snappy :gzip :zstd :brotli :lz4 :lz4-raw})
(def ^:private json-compressions #{:none :gzip :zstd})
(def ^:private copy-option-keys
  #{:format :compression :row-group-size :header? :delimiter :quote :escape
    :array? :partition-by :overwrite-or-ignore?})

(defn- invalid-copy-option [message option value]
  (throw (ex-info message {:duckdb/error :invalid-option
                           :option option
                           :value value})))

(defn- copy-format [format]
  (let [format (if (keyword? format) format (keyword (str/lower-case (str format))))]
    (when-not (contains? copy-formats format)
      (invalid-copy-option (str "Invalid COPY format: " format) :format format))
    format))

(defn- copy-bool [option value]
  (if (or (true? value) (false? value))
    (str value)
    (invalid-copy-option (str "COPY option " option " must be boolean") option value)))

(defn- copy-positive-integer [option value]
  (if (and (integer? value) (pos? value))
    (str value)
    (invalid-copy-option (str "COPY option " option " must be a positive integer") option value)))

(defn- copy-character [option value]
  (if (and (string? value) (= 1 (count value)))
    (sql-string value)
    (invalid-copy-option (str "COPY option " option " must be a one-character string") option value)))

(defn- copy-partition-by [value]
  (if (and (not (string? value)) (seqable? value) (seq value))
    (str "(" (str/join ", " (map #(identifier :column %) value)) ")")
    (invalid-copy-option "COPY option :partition-by must contain column names"
                         :partition-by value)))

(defn- copy-option-sql [format [option value]]
  (case option
    :format nil
    :compression
    (do
      (when-not (#{:parquet :json} format)
        (invalid-copy-option (str "COPY option :compression is not valid for " format)
                             option value))
      (let [compression (if (keyword? value) value (keyword (str/lower-case (str value))))
            valid-compressions (if (= :parquet format)
                                 parquet-compressions
                                 json-compressions)]
        (when-not (contains? valid-compressions compression)
          (invalid-copy-option (str "Invalid COPY compression: " value) option value))
        (str "COMPRESSION " (sql-string (str/replace (name compression) "-" "_")))))
    :row-group-size
    (do
      (when-not (= :parquet format)
        (invalid-copy-option "COPY option :row-group-size is only valid for parquet"
                             option value))
      (str "ROW_GROUP_SIZE " (copy-positive-integer option value)))
    :header?
    (do
      (when-not (= :csv format)
        (invalid-copy-option "COPY option :header? is only valid for csv" option value))
      (str "HEADER " (copy-bool option value)))
    :delimiter (do (when-not (= :csv format)
                     (invalid-copy-option "COPY option :delimiter is only valid for csv" option value))
                   (str "DELIMITER " (copy-character option value)))
    :quote (do (when-not (= :csv format)
                 (invalid-copy-option "COPY option :quote is only valid for csv" option value))
               (str "QUOTE " (copy-character option value)))
    :escape (do (when-not (= :csv format)
                  (invalid-copy-option "COPY option :escape is only valid for csv" option value))
                (str "ESCAPE " (copy-character option value)))
    :array?
    (do
      (when-not (= :json format)
        (invalid-copy-option "COPY option :array? is only valid for json" option value))
      (str "ARRAY " (copy-bool option value)))
    :partition-by (str "PARTITION_BY " (copy-partition-by value))
    :overwrite-or-ignore? (str "OVERWRITE_OR_IGNORE " (copy-bool option value))
    (invalid-copy-option (str "Unknown COPY option: " option) option value)))

(defn- copy-source [source]
  (cond
    (keyword? source) (identifier :table source)
    (symbol? source) (identifier :table source)
    (map? source)
    (cond
      (contains? source :table) (identifier :table (:table source))
      (contains? source :query) (copy-source (:query source))
      :else (throw (ex-info "COPY source map requires :table or :query"
                            {:duckdb/error :invalid-copy-source :source source})))
    (vector? source) (str "QUERY " (first source))
    (string? source)
    (if (re-find #"(?is)^\s*(select|with|from|values)\b" source)
      (str "QUERY " source)
      (identifier :table source))
    :else (throw (ex-info (str "Unsupported COPY source: " (pr-str source))
                          {:duckdb/error :invalid-copy-source :source source}))))

(defn- copy-sql [source path format opts]
  (let [query-source (if (and (map? source) (contains? source :query))
                       (:query source)
                       source)
        source-sql (copy-source source)
        query? (= "QUERY" (subs source-sql 0 5))
        source-sql (if query? (subs source-sql 6) source-sql)
        options (->> opts
                     (map (partial copy-option-sql format))
                     (remove nil?)
                     (cons (str "FORMAT " (name format))))]
    (into [(str "COPY " (if query? (str "(" source-sql ")") source-sql)
                 " TO " (sql-string path) " (" (str/join ", " options) ")")]
          (when (vector? query-source) (rest query-source)))))

(defn copy-to!
  "Exports a table or query to a Parquet, CSV, or JSON file.

  source may be a table keyword/symbol, a table name string, a query string,
  or a next.jdbc SQL vector. opts requires :format and supports format-specific
  keyword options such as :compression, :row-group-size, :header?, :delimiter,
  :array?, :partition-by, and :overwrite-or-ignore?."
  [ds source path opts]
  (let [opts (or opts {})
        unknown (seq (remove copy-option-keys (keys opts)))
        format (copy-format (:format opts))]
    (when unknown
      (invalid-copy-option (str "Unknown COPY option: " (first unknown))
                           (first unknown) (get opts (first unknown))))
    (jdbc/execute! ds (copy-sql source path format opts))))

(defn copy-to-parquet!
  "Exports a table or query to a Parquet file."
  ([ds source path]
   (copy-to-parquet! ds source path nil))
  ([ds source path opts]
   (copy-to! ds source path (assoc (or opts {}) :format :parquet))))

(defn copy-to-csv!
  "Exports a table or query to a CSV file."
  ([ds source path]
   (copy-to-csv! ds source path nil))
  ([ds source path opts]
   (copy-to! ds source path (assoc (or opts {}) :format :csv))))

(defn copy-to-json!
  "Exports a table or query to a JSON file."
  ([ds source path]
   (copy-to-json! ds source path nil))
  ([ds source path opts]
   (copy-to! ds source path (assoc (or opts {}) :format :json))))

(defn- identifier [kind value]
  (let [identifier (if (or (keyword? value) (symbol? value))
                     (name value)
                     (str value))]
    (when-not (re-matches identifier-pattern identifier)
      (throw (ex-info (str "Invalid DuckDB " kind ": " value)
                      {:duckdb/error :invalid-alias
                       kind value})))
    identifier))

(defn- table-columns [^Connection con table]
  (with-open [^Statement stmt (.createStatement con)
              rs (.executeQuery stmt (str "select * from " table " limit 0"))]
    (let [^ResultSetMetaData rsmeta (.getMetaData rs)]
      (mapv (fn [ix]
              {:key (keyword (.getColumnLabel rsmeta ix))
               :type-name (some-> (.getColumnTypeName rsmeta ix) str str/trim str/upper-case)})
            (range 1 (inc (.getColumnCount rsmeta)))))))

(defn- duckdb-connection [^Connection con]
  (.unwrap con DuckDBConnection))

(defn duplicate
  "Opens an independently closeable connection to the same DuckDB database.

  The duplicate inherits read-only mode, session initialization SQL, and the
  auto-commit default from con."
  [^Connection con]
  (let [^DuckDBConnection duck-con (duckdb-connection con)
        session-init-sql (.get ^Map connection-session-init duck-con)]
    (initialize-connection! (.duplicate duck-con) session-init-sql)))

(deftype ^:private DefaultValue [])
(deftype ^:private UnionValue [tag value])
(deftype ^:private ArrayValue [values validity])
(deftype ^:private FixedSizeValue [values size])
(deftype ^:private HugeIntValue [lower upper])
(deftype ^:private UUIDValue [upper lower])
(deftype ^:private TemporalValue [encoding value offset])

(doseq [constructor [#'->DefaultValue #'->UnionValue #'->ArrayValue
                     #'->FixedSizeValue #'->HugeIntValue #'->UUIDValue
                     #'->TemporalValue]]
  (alter-meta! constructor assoc :private true))

(def ^:private default-value-instance (DefaultValue.))
(def ^:private byte-array-class (class (byte-array 0)))
(def ^:private byte-array-2d-class (class (make-array Byte/TYPE 0 0)))
(def ^:private boolean-array-classes
  #{(class (boolean-array 0)) (class (make-array Boolean/TYPE 0 0))})
(def ^:private primitive-array-classes
  (set (map class [(boolean-array 0)
                   (byte-array 0)
                   (char-array 0)
                   (short-array 0)
                   (int-array 0)
                   (long-array 0)
                   (float-array 0)
                   (double-array 0)
                   (make-array Boolean/TYPE 0 0)
                   (make-array Byte/TYPE 0 0)
                   (make-array Short/TYPE 0 0)
                   (make-array Integer/TYPE 0 0)
                   (make-array Long/TYPE 0 0)
                   (make-array Float/TYPE 0 0)
                   (make-array Double/TYPE 0 0)])))

(defn default-value
  "Returns an append value that uses the DEFAULT expression of the column."
  []
  default-value-instance)

(defn union-value
  "Returns a UNION append value that selects tag and contains value."
  [tag value]
  (UnionValue. (identifier :union-tag tag) value))

(defn array-value
  "Returns a primitive-array append value with a boolean validity mask."
  [values validity]
  (when-not (and (contains? primitive-array-classes (class values))
                 (contains? boolean-array-classes (class validity)))
    (throw (ex-info "Array values require a primitive array and boolean[] validity mask"
                    {:duckdb/error :invalid-array-value})))
  (ArrayValue. values validity))

(defn fixed-size-value
  "Returns a fixed-size Iterable append value containing exactly size values."
  [values size]
  (when-not (instance? Iterable values)
    (throw (ex-info "Fixed-size append values require an Iterable"
                    {:duckdb/error :invalid-fixed-size-value
                     :value-class (.getName (class values))})))
  (FixedSizeValue. values (int size)))

(defn hugeint-value
  "Returns an explicitly encoded HUGEINT from unsigned lower and signed upper words."
  [lower upper]
  (HugeIntValue. (long lower) (long upper)))

(defn uuid-value
  "Returns an explicitly encoded UUID from upper and lower 64-bit words."
  [upper lower]
  (UUIDValue. (long upper) (long lower)))

(defn epoch-days
  "Returns an explicitly encoded DATE measured in days since the Unix epoch."
  [days]
  (TemporalValue. :epoch-days (long days) nil))

(defn day-micros
  "Returns an explicitly encoded TIME measured in microseconds since midnight.

  With offset-seconds, encodes a TIME WITH TIME ZONE value."
  ([micros]
   (TemporalValue. :day-micros (long micros) nil))
  ([micros offset-seconds]
   (TemporalValue. :day-micros (long micros) (int offset-seconds))))

(defn epoch-seconds
  "Returns an explicitly encoded TIMESTAMP measured in Unix epoch seconds."
  [seconds]
  (TemporalValue. :epoch-seconds (long seconds) nil))

(defn epoch-millis
  "Returns an explicitly encoded TIMESTAMP measured in Unix epoch milliseconds."
  [millis]
  (TemporalValue. :epoch-millis (long millis) nil))

(defn epoch-micros
  "Returns an explicitly encoded TIMESTAMP measured in Unix epoch microseconds."
  [micros]
  (TemporalValue. :epoch-micros (long micros) nil))

(defn epoch-nanos
  "Returns an explicitly encoded TIMESTAMP measured in Unix epoch nanoseconds."
  [nanos]
  (TemporalValue. :epoch-nanos (long nanos) nil))

(defn create-appender
  "Creates a DuckDB appender. The caller must flush and close it.

  Arity selects an unqualified table, schema/table, or catalog/schema/table."
  (^DuckDBAppender [^Connection con table]
   (let [^DuckDBConnection duck-con (duckdb-connection con)]
     (.createAppender duck-con (identifier :table table))))
  (^DuckDBAppender [^Connection con schema table]
   (let [^DuckDBConnection duck-con (duckdb-connection con)]
     (.createAppender duck-con
                      (identifier :schema schema)
                      (identifier :table table))))
  (^DuckDBAppender [^Connection con catalog schema table]
   (let [^DuckDBConnection duck-con (duckdb-connection con)]
     (.createAppender duck-con
                      (identifier :catalog catalog)
                      (identifier :schema schema)
                      (identifier :table table)))))

(defn create-single-value-appender
  "Creates a DuckDB appender for a single-column schema/table.

  The caller must frame every value with beginRow/endRow, then flush and close
  the appender."
  ^DuckDBSingleValueAppender [^Connection con schema table]
  (let [^DuckDBConnection duck-con (duckdb-connection con)]
    (.createSingleValueAppender duck-con
                                (identifier :schema schema)
                                (identifier :table table))))

(declare append-value!)

(defn- append-primitive-array!
  [^DuckDBAppender app type-name values validity]
  (let [byte-array? (#{byte-array-class byte-array-2d-class} (class values))
        blob? (= "BLOB" type-name)
        method-name (if (and byte-array? (not blob?)) "appendByteArray" "append")
        args (if validity [values validity] [values])]
    (clojure.lang.Reflector/invokeInstanceMethod app method-name (to-array args))))

(defn- validity->null-mask [validity]
  (if (= (class validity) (class (boolean-array 0)))
    (boolean-array (map not validity))
    (into-array (class (boolean-array 0))
                (map #(boolean-array (map not %)) validity))))

(defn- append-temporal-value! [^DuckDBAppender app ^TemporalValue temporal]
  (let [value (.-value temporal)]
    (case (.-encoding temporal)
      :epoch-days (.appendEpochDays app (int value))
      :day-micros (if-some [offset (.-offset temporal)]
                    (.appendDayMicros app (long value) (int offset))
                    (.appendDayMicros app (long value)))
      :epoch-seconds (.appendEpochSeconds app (long value))
      :epoch-millis (.appendEpochMillis app (long value))
      :epoch-micros (.appendEpochMicros app (long value))
      :epoch-nanos (.appendEpochNanos app (long value)))))

(defn- coerce-fixed-values [type-name values]
  (let [coerce (cond
                 (str/starts-with? type-name "TINYINT[") byte
                 (str/starts-with? type-name "SMALLINT[") short
                 (str/starts-with? type-name "INTEGER[") int
                 (str/starts-with? type-name "BIGINT[") long
                 (str/starts-with? type-name "FLOAT[") float
                 (str/starts-with? type-name "DOUBLE[") double
                 :else identity)]
    (mapv #(if (nil? %) nil (coerce %)) values)))

(defn- append-map-value [^DuckDBAppender app value]
  (let [m (LinkedHashMap.)]
    (doseq [[k v] value]
      (.put m (if (keyword? k) (name k) k) v))
    (.append app ^Map m)))

(defn- map-entry [m field]
  (some #(find m %)
        [(keyword field)
         (keyword (str/lower-case field))
         (keyword (str/upper-case field))
         field
         (str/lower-case field)
         (str/upper-case field)]))

(defn- append-struct-value! [^DuckDBAppender app type-name value]
  (let [fields (types/struct-fields type-name)
        field-types (types/struct-field-types type-name)]
    (.beginStruct app)
    (doseq [[field field-type] (map vector fields field-types)
            :let [entry (map-entry value field)]]
      (when-not entry
        (throw (ex-info (str "Missing STRUCT field " field)
                        {:duckdb/error :missing-struct-field
                         :field field
                         :type type-name})))
      (append-value! app field-type (val entry)))
    (.endStruct app)))

(defn- append-value! [^DuckDBAppender app type-name value]
  (cond
    (nil? value) (.appendNull app)
    (instance? DefaultValue value) (.appendDefault app)
    (instance? UnionValue value)
    (let [^UnionValue union value]
      (.beginUnion app (.-tag union))
      (let [fields (types/struct-fields type-name)
            field-types (types/struct-field-types type-name)
            member-type (some (fn [[field field-type]]
                                (when (.equalsIgnoreCase ^String field (.-tag union)) field-type))
                              (map vector fields field-types))]
        (append-value! app member-type (.-value union)))
      (.endUnion app))
    (instance? ArrayValue value)
    (let [^ArrayValue array-value value]
      (append-primitive-array! app type-name
                               (.-values array-value)
                               (validity->null-mask (.-validity array-value))))
    (instance? FixedSizeValue value)
    (let [^FixedSizeValue fixed value]
      (.append app
               ^Iterable (coerce-fixed-values type-name (.-values fixed))
               (int (.-size fixed))))
    (instance? HugeIntValue value)
    (let [^HugeIntValue huge value]
      (.appendHugeInt app (long (.-lower huge)) (long (.-upper huge))))
    (instance? UUIDValue value)
    (let [^UUIDValue uuid value]
      (.appendUUID app (long (.-upper uuid)) (long (.-lower uuid))))
    (instance? TemporalValue value) (append-temporal-value! app value)
    (contains? primitive-array-classes (class value))
    (append-primitive-array! app type-name value nil)
    (keyword? value) (.append app ^String (name value))
    (string? value) (.append app ^String value)
    (instance? Boolean value) (.append app ^Boolean value)
    (and (= "TINYINT" type-name) (number? value)) (.append app (byte value))
    (and (= "SMALLINT" type-name) (number? value)) (.append app (short value))
    (and (= "INTEGER" type-name) (number? value)) (.append app (int value))
    (and (= "BIGINT" type-name) (number? value)) (.append app (long value))
    (instance? Byte value) (.append app ^Byte value)
    (instance? Short value) (.append app ^Short value)
    (instance? Integer value) (.append app ^Integer value)
    (instance? Long value) (.append app ^Long value)
    (and (= "FLOAT" type-name) (number? value)) (.append app (float value))
    (and (= "DOUBLE" type-name) (number? value)) (.append app (double value))
    (instance? Float value) (.append app ^Float value)
    (instance? Double value) (.append app ^Double value)
    (instance? BigInteger value) (.append app ^BigInteger value)
    (instance? BigDecimal value) (.append app ^BigDecimal value)
    (instance? Character value) (.append app ^Character value)
    (instance? UUID value) (.append app ^UUID value)
    (instance? LocalDate value) (.append app ^LocalDate value)
    (instance? LocalTime value) (.append app ^LocalTime value)
    (instance? LocalDateTime value) (.append app ^LocalDateTime value)
    (instance? OffsetTime value) (.append app ^OffsetTime value)
    (instance? OffsetDateTime value) (.append app ^OffsetDateTime value)
    (instance? Date value) (.append app ^Date value)
    (and (types/struct-type? type-name) (map? value)) (append-struct-value! app type-name value)
    (map? value) (append-map-value app value)
    (instance? Collection value) (.append app ^Collection value)
    :else (throw (ex-info (str "Unsupported DuckDB append value: " (pr-str value))
                          {:duckdb/error :unsupported-append-value
                           :value-class (.getName (class value))}))))

(defn- append-row! [^DuckDBAppender app columns row]
  (doseq [{:keys [key]} columns]
    (when-not (contains? row key)
      (throw (ex-info (str "DuckDB append row is missing column " key)
                      {:duckdb/error :missing-append-column
                       :column key}))))
  (.beginRow app)
  (doseq [{:keys [key type-name]} columns]
    (append-value! app type-name (get row key)))
  (.endRow app))

(defn- append-failed [^SQLException cause row-index]
  (ex-info
   (str "DuckDB append failed at row " row-index)
   {:duckdb/error :append-failed
    :row-index row-index
    :cause-class (.getName (class cause))
    :cause-message (.getMessage cause)}
   cause))

(defn- append-with-connection! [^Connection con table rows]
  (let [table (identifier :table table)
        columns (table-columns con table)
        ^DuckDBConnection duck-con (duckdb-connection con)]
    (with-open [^DuckDBAppender app (.createAppender duck-con table)]
      (doseq [[row-index row] (map-indexed vector rows)]
        (try
          (append-row! app columns row)
          (catch SQLException e
            (throw (append-failed e row-index)))))
      (.flush app)
      (count rows))))

(defn- qualified-target [target]
  (let [{:keys [catalog schema table]} target]
    (when-not table
      (throw (ex-info "Qualified DuckDB append target requires :table"
                      {:duckdb/error :invalid-append-target
                       :target target})))
    (when (and catalog (not schema))
      (throw (ex-info "Catalog-qualified DuckDB append target requires :schema"
                      {:duckdb/error :invalid-append-target
                       :target target})))
    {:catalog (some->> catalog (identifier :catalog))
     :schema (some->> schema (identifier :schema))
     :table (identifier :table table)}))

(defn- qualified-name [{:keys [catalog schema table]}]
  (str/join "." (remove nil? [catalog schema table])))

(defn- append-qualified-with-connection! [^Connection con target rows]
  (let [{:keys [catalog schema table] :as target} (qualified-target target)
        columns (table-columns con (qualified-name target))]
    (with-open [^DuckDBAppender app (if catalog
                                     (create-appender con catalog schema table)
                                     (if schema
                                       (create-appender con schema table)
                                       (create-appender con table)))]
      (doseq [[row-index row] (map-indexed vector rows)]
        (try
          (append-row! app columns row)
          (catch SQLException e
            (throw (append-failed e row-index)))))
      (.flush app)
      (count rows))))

(defn append!
  "Bulk inserts map rows into table with DuckDB's Appender API.

  Row values are appended in the table's declared column order, not map order."
  [ds table rows]
  (let [rows (vec rows)]
    (if (instance? Connection ds)
      (if (map? table)
        (append-qualified-with-connection! ds table rows)
        (append-with-connection! ds table rows))
      (with-open [con (jdbc/get-connection ds)]
        (if (map? table)
          (append-qualified-with-connection! con table rows)
          (append-with-connection! con table rows))))))

(defn- append-single-value!
  [^DuckDBSingleValueAppender app type-name value]
  (cond
    (nil? value) (let [^String null-value nil] (.append app null-value))
    (instance? Boolean value) (.append app (boolean value))
    (instance? LocalDateTime value) (.appendLocalDateTime app value)
    (instance? BigDecimal value) (.appendBigDecimal app value)
    (= byte-array-class (class value)) (.append app ^bytes value)
    (string? value) (.append app ^String value)
    (and (= "TINYINT" type-name) (number? value)) (.append app (byte value))
    (and (= "SMALLINT" type-name) (number? value)) (.append app (short value))
    (and (= "INTEGER" type-name) (number? value)) (.append app (int value))
    (and (= "BIGINT" type-name) (number? value)) (.append app (long value))
    (and (= "FLOAT" type-name) (number? value)) (.append app (float value))
    (and (= "DOUBLE" type-name) (number? value)) (.append app (double value))
    (instance? Byte value) (.append app (byte value))
    (instance? Short value) (.append app (short value))
    (instance? Integer value) (.append app (int value))
    (instance? Long value) (.append app (long value))
    (instance? Float value) (.append app (float value))
    (instance? Double value) (.append app (double value))
    :else (throw (ex-info (str "Unsupported DuckDB single-value append value: "
                               (pr-str value))
                          {:duckdb/error :unsupported-single-append-value
                           :value-class (.getName (class value))}))))

(defn- append-single-with-connection!
  [^Connection con schema table values]
  (let [schema (identifier :schema schema)
        table (identifier :table table)
        columns (table-columns con (str schema "." table))]
    (when-not (= 1 (count columns))
      (throw (ex-info "Single-value appender requires a one-column table"
                      {:duckdb/error :invalid-single-value-table
                       :column-count (count columns)})))
    (let [type-name (:type-name (first columns))]
      (with-open [^DuckDBSingleValueAppender app
                  (create-single-value-appender con schema table)]
        (doseq [[row-index value] (map-indexed vector values)]
          (try
            (.beginRow app)
            (append-single-value! app type-name value)
            (.endRow app)
            (catch SQLException e
              (throw (append-failed e row-index)))))
        (.flush app)
        (count values)))))

(defn append-single!
  "Bulk inserts values into a one-column schema/table with DuckDB's narrow appender."
  [ds schema table values]
  (let [values (vec values)]
    (if (instance? Connection ds)
      (append-single-with-connection! ds schema table values)
      (with-open [con (jdbc/get-connection ds)]
        (append-single-with-connection! con schema table values)))))

(defn attach!
  "Attaches the DuckDB database at path as alias."
  ([ds path alias]
   (attach! ds path alias nil))
  ([ds path alias opts]
   (let [alias (identifier :alias alias)
         read-only? (:read-only opts)]
     (jdbc/execute!
      ds
      [(str "attach " (sql-string path) " as " alias
            (when read-only?
              " (read_only)"))]))))

(defn detach!
  "Detaches an attached DuckDB database alias."
  [ds alias]
  (jdbc/execute! ds [(str "detach " (identifier :alias alias))]))

(defn install-extension!
  "Installs a DuckDB extension by name."
  [ds name]
  (jdbc/execute! ds [(str "install " (identifier :name name))]))

(defn load-extension!
  "Loads a DuckDB extension by name."
  [ds name]
  (jdbc/execute! ds [(str "load " (identifier :name name))]))

(defn release-db!
  "Releases the database pinned to jdbc-url.

  PROCESS-GLOBAL: Call only as an explicit application lifecycle operation.
  Do not use this in automatic connection or datasource cleanup."
  [jdbc-url]
  (DuckDBDriver/releaseDB (str jdbc-url)))

(defn shutdown-cancel-scheduler!
  "Shuts down DuckDB JDBC's query-cancellation scheduler.

  PROCESS-GLOBAL: Call only as an explicit application lifecycle operation.
  Do not use this in automatic connection or datasource cleanup."
  []
  (DuckDBDriver/shutdownQueryCancelScheduler))

(defn registered-functions
  "Returns DuckDB JDBC's process-global registered UDFs as maps.

  PROCESS-GLOBAL: This inspects the registry shared by every DuckDB connection
  in the JVM."
  []
  (mapv (fn [^DuckDBFunctions$RegisteredFunction function]
          {:name (.name function)
           :kind (keyword (str/lower-case (str (.functionKind function))))})
        (DuckDBDriver/registeredFunctions)))

(defn clear-functions-registry!
  "Clears DuckDB JDBC's registered UDF metadata.

  PROCESS-GLOBAL: Call only as an explicit application lifecycle operation.
  Do not use this in automatic connection or datasource cleanup."
  []
  (DuckDBDriver/clearFunctionsRegistry))

(defn duckdb-version
  "Returns the DuckDB version string for ds."
  [ds]
  (:version (first (jdbc/execute! ds ["select version() as version"]))))
