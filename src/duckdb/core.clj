(ns duckdb.core
  "Convenience helpers for DuckDB-specific next.jdbc operations."
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
           (org.duckdb DuckDBAppender DuckDBConnection DuckDBDriver)))

(set! *warn-on-reflection* true)

(def ^:private simple-option-pattern #"^[a-z0-9_]+$")
(def ^:private identifier-pattern #"^[A-Za-z_][A-Za-z0-9_]*$")
(def ^:private datasource-option-keys
  #{:access-mode :auto-commit :read-only :session-init-sql :settings :user-agent})
(def ^:private connection-session-init
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
  "Returns a DuckDB datasource for jdbc-url using java.util.Properties.

  Options are :read-only, :access-mode (:read-only, :read-write, or
  :automatic), :settings, :session-init-sql, :auto-commit, and :user-agent."
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
                      session-init-sql)))]
     (reify DataSource
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
                   (str "Cannot unwrap DuckDB datasource to " (.getName ^Class iface))))))))))

(defn memory-datasource
  "Returns a next.jdbc datasource for an in-memory DuckDB database.

  Each connection from a memory datasource is a SEPARATE in-memory database;
  hold one connection with next.jdbc/get-connection for multi-statement work."
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

(declare append-value!)

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

(defn append!
  "Bulk-inserts map rows into table using DuckDB's Appender API.

  Row values are appended in the table's declared column order, not map order."
  [ds table rows]
  (let [rows (vec rows)]
    (if (instance? Connection ds)
      (append-with-connection! ds table rows)
      (with-open [con (jdbc/get-connection ds)]
        (append-with-connection! con table rows)))))

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

(defn duckdb-version
  "Returns the DuckDB version string for ds."
  [ds]
  (:version (first (jdbc/execute! ds ["select version() as version"]))))
