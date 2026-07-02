(ns duckdb.types
  "Read-side conversions for DuckDB JDBC nested values.

  Extending java.util.Map is process-global across all next.jdbc usage in this
  JVM. For DuckDB this is benign: Java maps are converted to Clojure maps."
  (:require [next.jdbc.result-set :as rs])
  (:import (java.sql Array)
           (java.util Map)
           (org.duckdb DuckDBArray DuckDBStruct)))

(set! *warn-on-reflection* true)

(declare duckdb-value->clj)

(defn- array->vector [^Array value]
  (mapv duckdb-value->clj (seq (object-array (.getArray value)))))

(defn- struct->map [^DuckDBStruct value]
  (into {}
        (map (fn [[k v]]
               [(keyword k) (duckdb-value->clj v)]))
        (.getMap value)))

(defn- java-map->clj [^Map value]
  (into {}
        (map (fn [[k v]]
               [k (duckdb-value->clj v)]))
        value))

(defn- duckdb-value->clj [value]
  (cond
    (instance? DuckDBArray value) (array->vector value)
    (instance? DuckDBStruct value) (struct->map value)
    (instance? Map value) (java-map->clj value)
    (instance? Array value) (array->vector value)
    :else value))

(extend-protocol rs/ReadableColumn
  DuckDBArray
  (read-column-by-label [value _label]
    (duckdb-value->clj value))
  (read-column-by-index [value _rsmeta _idx]
    (duckdb-value->clj value))

  DuckDBStruct
  (read-column-by-label [value _label]
    (duckdb-value->clj value))
  (read-column-by-index [value _rsmeta _idx]
    (duckdb-value->clj value))

  Map
  (read-column-by-label [value _label]
    (duckdb-value->clj value))
  (read-column-by-index [value _rsmeta _idx]
    (duckdb-value->clj value)))
