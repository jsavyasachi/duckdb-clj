(ns duckdb.core
  "Convenience helpers for DuckDB-specific next.jdbc operations."
  (:require [clojure.string :as str]
            [duckdb.types]
            [next.jdbc :as jdbc]))

(set! *warn-on-reflection* true)

(def ^:private simple-option-pattern #"^[a-z0-9_]+$")
(def ^:private identifier-pattern #"^[A-Za-z_][A-Za-z0-9_]*$")

(defn memory-datasource
  "Returns a next.jdbc datasource for an in-memory DuckDB database.

  Each connection from a memory datasource is a SEPARATE in-memory database;
  hold one connection with next.jdbc/get-connection for multi-statement work."
  []
  (jdbc/get-datasource "jdbc:duckdb:"))

(defn file-datasource
  "Returns a next.jdbc datasource for a DuckDB database at path."
  [path]
  (jdbc/get-datasource (str "jdbc:duckdb:" path)))

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
