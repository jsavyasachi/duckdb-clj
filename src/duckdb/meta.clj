(ns duckdb.meta
  "DuckDB-specific result accessors and JDBC metadata introspection."
  (:refer-clojure :exclude [struct])
  (:require [clojure.string :as str])
  (:import (java.sql Array Connection DatabaseMetaData ResultSet ResultSetMetaData)
           (java.util Map UUID)
           (org.duckdb DuckDBConnection DuckDBResultSet DuckDBStruct JsonNode)))

(set! *warn-on-reflection* true)

(defn- duckdb-result-set [^ResultSet rs]
  (.unwrap rs DuckDBResultSet))

(defn- column-index [^DuckDBResultSet rs column]
  (if (number? column)
    (int column)
    (.findColumn rs (if (or (keyword? column) (symbol? column))
                      (name column)
                      (str column)))))

(declare nested-value->clj)

(defn- array->vector [^Array value]
  (mapv nested-value->clj (seq (object-array (.getArray value)))))

(defn- struct->map [^DuckDBStruct value]
  (into {}
        (map (fn [[k v]]
               [(keyword k) (nested-value->clj v)]))
        (.getMap value)))

(defn- java-map->clj [^Map value]
  (into {}
        (map (fn [[k v]]
               [k (nested-value->clj v)]))
        value))

(defn- nested-value->clj [value]
  (cond
    (instance? DuckDBStruct value) (struct->map value)
    (instance? Array value) (array->vector value)
    (instance? Map value) (java-map->clj value)
    :else value))

(defn hugeint
  "Reads column from the current row as a signed HUGEINT.

  rs and its current row must remain open while this function is called."
  [^ResultSet rs column]
  (let [^DuckDBResultSet rs (duckdb-result-set rs)
        value (.getHugeint rs (column-index rs column))]
    (when-not (.wasNull rs)
      value)))

(defn uhugeint
  "Reads column from the current row as an unsigned UHUGEINT.

  rs and its current row must remain open while this function is called."
  [^ResultSet rs column]
  (let [^DuckDBResultSet rs (duckdb-result-set rs)
        value (.getUhugeint rs (column-index rs column))]
    (when-not (.wasNull rs)
      value)))

(defn uuid
  "Reads column from the current row as a UUID.

  rs and its current row must remain open while this function is called."
  ^UUID [^ResultSet rs column]
  (let [^DuckDBResultSet rs (duckdb-result-set rs)]
    (.getUuid rs (column-index rs column))))

(defn json-node
  "Reads column from the current row as a DuckDB JsonNode.

  rs and its current row must remain open while this function is called."
  ^JsonNode [^ResultSet rs column]
  (let [^DuckDBResultSet rs (duckdb-result-set rs)]
    (.getJsonObject rs (column-index rs column))))

(defn lazy-string
  "Reads column from the current row through DuckDB's lazy string accessor.

  rs and its current row must remain open while this function is called."
  ^String [^ResultSet rs column]
  (let [^DuckDBResultSet rs (duckdb-result-set rs)]
    (.getLazyString rs (column-index rs column))))

(defn offset-time
  "Reads column from the current row as an OffsetTime.

  rs and its current row must remain open while this function is called."
  [^ResultSet rs column]
  (let [^DuckDBResultSet rs (duckdb-result-set rs)]
    (.getOffsetTime rs (column-index rs column))))

(defn blob
  "Reads column from the current row as a java.sql.Blob.

  Consume the Blob while rs and its current row remain open."
  [^ResultSet rs column]
  (let [^DuckDBResultSet rs (duckdb-result-set rs)]
    (.getBlob rs (int (column-index rs column)))))

(defn array
  "Reads column from the current row as recursively converted Clojure data.

  rs and its current row must remain open while this function is called."
  [^ResultSet rs column]
  (let [^DuckDBResultSet rs (duckdb-result-set rs)]
    (some-> (.getArray rs (int (column-index rs column))) array->vector)))

(defn struct
  "Reads column from the current row as a keyword-keyed Clojure map.

  Nested DuckDB ARRAY and STRUCT values are converted recursively. rs and its
  current row must remain open while this function is called."
  [^ResultSet rs column]
  (let [^DuckDBResultSet rs (duckdb-result-set rs)
        ^DuckDBStruct value (.getStruct rs (column-index rs column))]
    (some-> value struct->map)))

(defn- ^DatabaseMetaData database-metadata [^Connection con]
  (let [^DuckDBConnection con (.unwrap con DuckDBConnection)]
    (.getMetaData con)))

(defn- result-set->maps [^ResultSet rs]
  (with-open [rs rs]
    (let [^ResultSetMetaData metadata (.getMetaData rs)
          indexes (range 1 (inc (.getColumnCount metadata)))
          keys (mapv #(-> (.getColumnLabel metadata %)
                          str/lower-case
                          keyword)
                     indexes)]
      (loop [rows []]
        (if (.next rs)
          (recur (conj rows
                       (zipmap keys (mapv #(.getObject rs (int %)) indexes))))
          rows)))))

(defn catalogs
  "Returns the catalogs visible through JDBC metadata."
  [^Connection con]
  (result-set->maps (.getCatalogs (database-metadata con))))

(defn schemas
  "Returns schemas visible through JDBC metadata.

  opts supports :catalog and :schema-pattern."
  ([con]
   (schemas con nil))
  ([^Connection con {:keys [catalog schema-pattern]}]
   (result-set->maps
    (.getSchemas (database-metadata con) catalog schema-pattern))))

(defn tables
  "Returns tables visible through JDBC metadata.

  opts supports :catalog, :schema-pattern, :table-pattern, and :types. JDBC
  percent and underscore wildcards are accepted in pattern values."
  ([con]
   (tables con nil))
  ([^Connection con {:keys [catalog schema-pattern table-pattern types]
                     :or {table-pattern "%"}}]
   (result-set->maps
    (.getTables (database-metadata con)
                catalog schema-pattern table-pattern
                (when types (into-array String types))))))

(defn columns
  "Returns columns visible through JDBC metadata.

  opts supports :catalog, :schema-pattern, :table-pattern, and
  :column-pattern. JDBC percent and underscore wildcards are accepted."
  ([con]
   (columns con nil))
  ([^Connection con {:keys [catalog schema-pattern table-pattern column-pattern]
                     :or {table-pattern "%" column-pattern "%"}}]
   (result-set->maps
    (.getColumns (database-metadata con)
                 catalog schema-pattern table-pattern column-pattern))))

(defn primary-keys
  "Returns primary-key columns for table.

  opts supports :catalog and :schema."
  ([con table]
   (primary-keys con table nil))
  ([^Connection con table {:keys [catalog schema]}]
   (result-set->maps
    (.getPrimaryKeys (database-metadata con) catalog schema (name table)))))

(defn imported-keys
  "Returns foreign keys imported by table.

  opts supports :catalog and :schema."
  ([con table]
   (imported-keys con table nil))
  ([^Connection con table {:keys [catalog schema]}]
   (result-set->maps
    (.getImportedKeys (database-metadata con) catalog schema (name table)))))

(defn exported-keys
  "Returns foreign keys that reference table.

  opts supports :catalog and :schema."
  ([con table]
   (exported-keys con table nil))
  ([^Connection con table {:keys [catalog schema]}]
   (result-set->maps
    (.getExportedKeys (database-metadata con) catalog schema (name table)))))

(defn indexes
  "Returns index metadata for table.

  opts supports :catalog, :schema, :unique, and :approximate."
  ([con table]
   (indexes con table nil))
  ([^Connection con table {:keys [catalog schema unique approximate]
                           :or {unique false approximate false}}]
   (result-set->maps
    (.getIndexInfo (database-metadata con) catalog schema (name table)
                   unique approximate))))

(defn type-info
  "Returns the JDBC types supported by DuckDB."
  [^Connection con]
  (result-set->maps (.getTypeInfo (database-metadata con))))

(defn driver-info
  "Returns DuckDB driver, database product, and JDBC version information."
  [^Connection con]
  (let [^DatabaseMetaData metadata (database-metadata con)]
    {:driver-name (.getDriverName metadata)
     :driver-version (.getDriverVersion metadata)
     :driver-major-version (.getDriverMajorVersion metadata)
     :driver-minor-version (.getDriverMinorVersion metadata)
     :database-product-name (.getDatabaseProductName metadata)
     :database-product-version (.getDatabaseProductVersion metadata)
     :database-major-version (.getDatabaseMajorVersion metadata)
     :database-minor-version (.getDatabaseMinorVersion metadata)
     :jdbc-major-version (.getJDBCMajorVersion metadata)
     :jdbc-minor-version (.getJDBCMinorVersion metadata)
     :url (.getURL metadata)
     :user-name (.getUserName metadata)}))

(defn capabilities
  "Returns commonly useful JDBC capability flags reported by DuckDB."
  [^Connection con]
  (let [^DatabaseMetaData metadata (database-metadata con)]
    {:transactions (.supportsTransactions metadata)
     :batch-updates (.supportsBatchUpdates metadata)
     :savepoints (.supportsSavepoints metadata)
     :named-parameters (.supportsNamedParameters metadata)
     :multiple-open-results (.supportsMultipleOpenResults metadata)
     :multiple-result-sets (.supportsMultipleResultSets metadata)
     :get-generated-keys (.supportsGetGeneratedKeys metadata)
     :stored-procedures (.supportsStoredProcedures metadata)
     :outer-joins (.supportsOuterJoins metadata)
     :full-outer-joins (.supportsFullOuterJoins metadata)
     :limited-outer-joins (.supportsLimitedOuterJoins metadata)
     :union (.supportsUnion metadata)
     :union-all (.supportsUnionAll metadata)
     :column-aliasing (.supportsColumnAliasing metadata)
     :like-escape-clause (.supportsLikeEscapeClause metadata)
     :minimum-sql-grammar (.supportsMinimumSQLGrammar metadata)
     :core-sql-grammar (.supportsCoreSQLGrammar metadata)
     :ansi-92-entry-level-sql (.supportsANSI92EntryLevelSQL metadata)}))
