(ns duckdb.udf
  "Register-only helpers for Java-backed DuckDB scalar and table functions.

  Registrations are process-global. duckdb_jdbc 1.5.4.0 does not expose a
  working way to remove them, so registered callbacks live for the JVM's
  lifetime."
  (:require [next.jdbc :as jdbc])
  (:import (java.math BigDecimal BigInteger)
           (java.sql Connection Date Timestamp)
           (java.time LocalDate LocalDateTime OffsetDateTime)
           (org.duckdb DuckDBColumnType DuckDBDataChunkReader
                       DuckDBFunctions
                       DuckDBFunctions$FunctionException DuckDBLogicalType
                       DuckDBReadableVector DuckDBScalarFunction
                       DuckDBScalarFunctionBuilder DuckDBTableFunction
                       DuckDBTableFunctionBindInfo DuckDBTableFunctionBuilder
                       DuckDBValue DuckDBWritableVector)))

(set! *warn-on-reflection* true)

(defn- function-name [value]
  (if (or (keyword? value) (symbol? value))
    (name value)
    (str value)))

(defn- with-connection [connectable f]
  (if (instance? Connection connectable)
    (f connectable)
    (with-open [con (jdbc/get-connection connectable)]
      (f con))))

(defn- add-scalar-parameter!
  [^DuckDBScalarFunctionBuilder builder type]
  (cond
    (instance? Class type) (.withParameter builder ^Class type)
    (instance? DuckDBColumnType type) (.withParameter builder ^DuckDBColumnType type)
    (instance? DuckDBLogicalType type) (.withParameter builder ^DuckDBLogicalType type)
    :else (throw (IllegalArgumentException.
                  (str "Unsupported DuckDB parameter type descriptor: " (pr-str type))))))

(defn- add-table-parameter!
  [^DuckDBTableFunctionBuilder builder type]
  (cond
    (instance? Class type) (.withParameter builder ^Class type)
    (instance? DuckDBColumnType type) (.withParameter builder ^DuckDBColumnType type)
    (instance? DuckDBLogicalType type) (.withParameter builder ^DuckDBLogicalType type)
    :else (throw (IllegalArgumentException.
                  (str "Unsupported DuckDB parameter type descriptor: " (pr-str type))))))

(defn- add-named-parameter!
  [^DuckDBTableFunctionBuilder builder parameter-name type]
  (let [parameter-name (name parameter-name)]
    (cond
      (instance? Class type) (.withNamedParameter builder parameter-name ^Class type)
      (instance? DuckDBColumnType type)
      (.withNamedParameter builder parameter-name ^DuckDBColumnType type)
      (instance? DuckDBLogicalType type)
      (.withNamedParameter builder parameter-name ^DuckDBLogicalType type)
      :else (throw (IllegalArgumentException.
                    (str "Unsupported DuckDB parameter type descriptor: " (pr-str type)))))))

(defn- set-return-type!
  [^DuckDBScalarFunctionBuilder builder type]
  (cond
    (instance? Class type) (.withReturnType builder ^Class type)
    (instance? DuckDBColumnType type) (.withReturnType builder ^DuckDBColumnType type)
    (instance? DuckDBLogicalType type) (.withReturnType builder ^DuckDBLogicalType type)
    :else (throw (IllegalArgumentException.
                  (str "Unsupported DuckDB return type descriptor: " (pr-str type))))))

(defn- add-result-column!
  [^DuckDBTableFunctionBindInfo info [column-name type]]
  (let [column-name (name column-name)]
    (cond
      (instance? Class type) (.addResultColumn info column-name ^Class type)
      (instance? DuckDBColumnType type) (.addResultColumn info column-name ^DuckDBColumnType type)
      (instance? DuckDBLogicalType type) (.addResultColumn info column-name ^DuckDBLogicalType type)
      :else (throw (IllegalArgumentException.
                    (str "Unsupported DuckDB result type descriptor: " (pr-str type)))))))

(defn- read-vector-value [^DuckDBReadableVector vector ^long row]
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
        DuckDBColumnType/DATE (.getLocalDate vector row)
        DuckDBColumnType/TIMESTAMP (.getLocalDateTime vector row)
        DuckDBColumnType/TIMESTAMP_MS (.getLocalDateTime vector row)
        DuckDBColumnType/TIMESTAMP_NS (.getLocalDateTime vector row)
        DuckDBColumnType/TIMESTAMP_S (.getLocalDateTime vector row)
        DuckDBColumnType/TIMESTAMP_WITH_TIME_ZONE (.getOffsetDateTime vector row)
        DuckDBColumnType/VARCHAR (.getString vector row)
        DuckDBColumnType/JSON (.getString vector row)
        DuckDBColumnType/UUID (.getString vector row)
        (throw (UnsupportedOperationException.
                (str "Unsupported scalar UDF parameter type: " type)))))))

(defn- write-vector-value! [^DuckDBWritableVector vector ^long row value]
  (if (nil? value)
    (.setNull vector row)
    (let [type (.getType vector)]
      (condp = type
        DuckDBColumnType/BOOLEAN (.setBoolean vector row (boolean value))
        DuckDBColumnType/TINYINT (.setByte vector row (byte value))
        DuckDBColumnType/UTINYINT (.setUint8 vector row (int value))
        DuckDBColumnType/SMALLINT (.setShort vector row (short value))
        DuckDBColumnType/USMALLINT (.setUint16 vector row (int value))
        DuckDBColumnType/INTEGER (.setInt vector row (int value))
        DuckDBColumnType/UINTEGER (.setUint32 vector row (long value))
        DuckDBColumnType/BIGINT (.setLong vector row (long value))
        DuckDBColumnType/UBIGINT (.setUint64 vector row ^BigInteger value)
        DuckDBColumnType/HUGEINT (.setHugeInt vector row ^BigInteger value)
        DuckDBColumnType/UHUGEINT (.setUHugeInt vector row ^BigInteger value)
        DuckDBColumnType/FLOAT (.setFloat vector row (float value))
        DuckDBColumnType/DOUBLE (.setDouble vector row (double value))
        DuckDBColumnType/DECIMAL (.setBigDecimal vector row ^BigDecimal value)
        DuckDBColumnType/DATE
        (cond
          (instance? LocalDate value) (.setDate vector row ^LocalDate value)
          (instance? Date value) (.setDate vector row ^Date value)
          :else (.setDate vector row ^java.util.Date value))
        DuckDBColumnType/TIMESTAMP
        (cond
          (instance? LocalDateTime value) (.setTimestamp vector row ^LocalDateTime value)
          (instance? Timestamp value) (.setTimestamp vector row ^Timestamp value)
          :else (.setTimestamp vector row ^java.util.Date value))
        DuckDBColumnType/TIMESTAMP_MS (.setTimestamp vector row ^LocalDateTime value)
        DuckDBColumnType/TIMESTAMP_NS (.setTimestamp vector row ^LocalDateTime value)
        DuckDBColumnType/TIMESTAMP_S (.setTimestamp vector row ^LocalDateTime value)
        DuckDBColumnType/TIMESTAMP_WITH_TIME_ZONE
        (.setOffsetDateTime vector row ^OffsetDateTime value)
        DuckDBColumnType/VARCHAR (.setString vector row (str value))
        DuckDBColumnType/JSON (.setString vector row (str value))
        DuckDBColumnType/UUID (.setString vector row (str value))
        (throw (UnsupportedOperationException.
                (str "Unsupported scalar UDF return type: " type)))))))

(defn- callback-error [name cause]
  (DuckDBFunctions$FunctionException.
   (str "Clojure scalar UDF " name " failed: "
        (or (.getMessage ^Throwable cause) (.getName (class cause))))
   cause))

(defn- scalar-callback [name f]
  (reify DuckDBScalarFunction
    (apply [_ input output]
      (try
        (dotimes [row (.rowCount ^DuckDBDataChunkReader input)]
          (let [args (mapv (fn [column]
                             (read-vector-value (.vector ^DuckDBDataChunkReader input column) row))
                           (range (.columnCount ^DuckDBDataChunkReader input)))]
            (write-vector-value! output row (apply f args))))
        (catch Throwable cause
          (throw (callback-error name cause)))))))

(defn register-scalar!
  "Registers f as a typed scalar function and returns the driver's registration.

  Options:
  - :parameters is a vector of Class, DuckDBColumnType, or DuckDBLogicalType.
  - :return-type is a required descriptor of the same kinds.
  - :null-handling is :special (default) or :null-in-null-out.
  - :volatility is :immutable (default) or :volatile.

  Registrations are PROCESS-GLOBAL and, in duckdb_jdbc 1.5.4.0, CANNOT be removed
  for the lifetime of the JVM because the driver exposes no working
  deregistration. Use stable, unique names and avoid re-registering the same
  name in one JVM."
  [connectable name f {:keys [parameters return-type null-handling volatility]
                       :or {parameters []
                            null-handling :special
                            volatility :immutable}}]
  (let [name (function-name name)]
    (with-connection
      connectable
      (fn [^Connection con]
        (with-open [^DuckDBScalarFunctionBuilder builder (DuckDBFunctions/scalarFunction)]
          (.withName builder name)
          (doseq [type parameters]
            (add-scalar-parameter! builder type))
          (set-return-type! builder return-type)
          (.withVectorizedFunction builder (scalar-callback name f))
          (case null-handling
            :special nil
            :null-in-null-out (.withNullInNullOut builder)
            (throw (IllegalArgumentException.
                    (str "Unsupported :null-handling value: " null-handling))))
          (case volatility
            :immutable nil
            :volatile (.withVolatile builder)
            (throw (IllegalArgumentException.
                    (str "Unsupported :volatility value: " volatility))))
          (.register builder con))))))

(defn- column-type-for-class [^Class type]
  (condp = type
    Boolean DuckDBColumnType/BOOLEAN
    Boolean/TYPE DuckDBColumnType/BOOLEAN
    Byte DuckDBColumnType/TINYINT
    Byte/TYPE DuckDBColumnType/TINYINT
    Short DuckDBColumnType/SMALLINT
    Short/TYPE DuckDBColumnType/SMALLINT
    Integer DuckDBColumnType/INTEGER
    Integer/TYPE DuckDBColumnType/INTEGER
    Long DuckDBColumnType/BIGINT
    Long/TYPE DuckDBColumnType/BIGINT
    Float DuckDBColumnType/FLOAT
    Float/TYPE DuckDBColumnType/FLOAT
    Double DuckDBColumnType/DOUBLE
    Double/TYPE DuckDBColumnType/DOUBLE
    BigInteger DuckDBColumnType/HUGEINT
    BigDecimal DuckDBColumnType/DECIMAL
    String DuckDBColumnType/VARCHAR
    LocalDate DuckDBColumnType/DATE
    LocalDateTime DuckDBColumnType/TIMESTAMP
    OffsetDateTime DuckDBColumnType/TIMESTAMP_WITH_TIME_ZONE
    nil))

(defn- read-parameter [^DuckDBValue value descriptor]
  (if (.isNull value)
    nil
    (let [type (if (instance? Class descriptor)
                 (column-type-for-class descriptor)
                 descriptor)]
      (if (instance? DuckDBLogicalType type)
        value
        (condp = type
          DuckDBColumnType/BOOLEAN (.getBoolean value)
          DuckDBColumnType/TINYINT (.getByte value)
          DuckDBColumnType/UTINYINT (.getUint8 value)
          DuckDBColumnType/SMALLINT (.getShort value)
          DuckDBColumnType/USMALLINT (.getUint16 value)
          DuckDBColumnType/INTEGER (.getInt value)
          DuckDBColumnType/UINTEGER (.getUint32 value)
          DuckDBColumnType/BIGINT (.getLong value)
          DuckDBColumnType/UBIGINT (.getUint64 value)
          DuckDBColumnType/HUGEINT (.getHugeInt value)
          DuckDBColumnType/UHUGEINT (.getUHugeInt value)
          DuckDBColumnType/FLOAT (.getFloat value)
          DuckDBColumnType/DOUBLE (.getDouble value)
          DuckDBColumnType/DECIMAL (.getBigDecimal value)
          DuckDBColumnType/DATE (.getLocalDate value)
          DuckDBColumnType/TIMESTAMP (.getLocalDateTime value)
          DuckDBColumnType/TIMESTAMP_MS (.getLocalDateTime value)
          DuckDBColumnType/TIMESTAMP_NS (.getLocalDateTime value)
          DuckDBColumnType/TIMESTAMP_S (.getLocalDateTime value)
          DuckDBColumnType/VARCHAR (.getString value)
          DuckDBColumnType/JSON (.getString value)
          DuckDBColumnType/UUID (.getString value)
          (throw (UnsupportedOperationException.
                  (str "Unsupported table UDF parameter type: " descriptor))))))))

(defn- set-cardinality! [^DuckDBTableFunctionBindInfo info cardinality]
  (when (some? cardinality)
    (if (map? cardinality)
      (.setCardinality info (long (:rows cardinality)) (boolean (:exact? cardinality)))
      (.setCardinality info (long cardinality) false))))

(defn- table-function
  [{:keys [parameters named-parameters columns cardinality bind init local-init apply
           max-threads]
    :or {parameters []
         named-parameters {}
         bind identity
         init (constantly nil)}}]
  (reify DuckDBTableFunction
    (bind [_ info]
      (doseq [column columns]
        (add-result-column! info column))
      (set-cardinality! info cardinality)
      (bind {:parameters (mapv (fn [index descriptor]
                                 (read-parameter (.getParameter info index) descriptor))
                               (range (count parameters))
                               parameters)
             :named-parameters
             (into (empty named-parameters)
                   (map (fn [[parameter-name descriptor]]
                          [parameter-name
                           (read-parameter (.getNamedParameter info (name parameter-name))
                                           descriptor)]))
                   named-parameters)}))
    (init [_ info]
      (when max-threads
        (.setMaxThreads info (long max-threads)))
      (init (.getBindData info)))
    (localInit [_ info]
      (when local-init
        (local-init (.getBindData info))))
    (apply [_ info output]
      (let [capacity (.capacity output)
            row-count (long
                       (apply {:bind-data (.getBindData info)
                               :state (.getInitData info)
                               :local-state (.getLocalInitData info)
                               :capacity capacity
                               :vectors (mapv #(.vector output %)
                                              (range (.columnCount output)))}))]
        (when (or (neg? row-count) (> row-count capacity))
          (throw (IllegalArgumentException.
                  (str "Table UDF returned " row-count
                       " rows for output capacity " capacity))))
        row-count))))

(defn register-table!
  "Registers a JVM-backed table function and returns the driver's registration.

  Options:
  - :parameters and :named-parameters contain DuckDB type descriptors.
  - :columns is a vector of [name type] result columns.
  - :cardinality is an estimated row count or {:rows n :exact? boolean}.
  - :bind receives converted positional and named parameters and returns bind data.
  - :init receives bind data and returns per-query state.
  - :local-init optionally returns per-thread state.
  - :apply receives bind/state data, capacity, and writable :vectors, and returns
    the number of rows written. :max-threads is optional.

  Registrations are PROCESS-GLOBAL and, in duckdb_jdbc 1.5.4.0, CANNOT be removed
  for the lifetime of the JVM because the driver exposes no working
  deregistration. Use stable, unique names and avoid re-registering the same
  name in one JVM."
  [connectable name {:keys [parameters named-parameters]
                     :or {parameters [] named-parameters {}}
                     :as opts}]
  (let [name (function-name name)]
    (with-connection
      connectable
      (fn [^Connection con]
        (with-open [^DuckDBTableFunctionBuilder builder (DuckDBFunctions/tableFunction)]
          (.withName builder name)
          (doseq [type parameters]
            (add-table-parameter! builder type))
          (doseq [[parameter-name type] named-parameters]
            (add-named-parameter! builder parameter-name type))
          (.withFunction builder (table-function opts))
          (.register builder con))))))
