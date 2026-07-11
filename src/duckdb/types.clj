(ns duckdb.types
  "Read-side conversions for DuckDB JDBC nested values.

  Extending java.util.Map is process-global across all next.jdbc usage in this
  JVM. For DuckDB this is benign: Java maps are converted to Clojure maps."
  (:require [clojure.string :as str]
            [next.jdbc.prepare :as prep]
            [next.jdbc.result-set :as rs])
  (:import (java.sql Array)
           (java.sql PreparedStatement)
           (java.util LinkedHashMap Map)
           (org.duckdb DuckDBArray DuckDBStruct)
           (org.duckdb.user DuckDBMap DuckDBUserArray DuckDBUserStruct)))

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

(defn- starts-with-ci? [s prefix]
  (str/starts-with? (str/upper-case (str/trim (str s))) prefix))

(defn- inside-parens [^String s]
  (subs s (inc (.indexOf s "(")) (dec (.length s))))

(defn- top-level-split [s]
  (let [text (str s)
        len (.length ^String text)]
    (loop [idx 0
           start 0
           depth 0
           in-quote? false
           parts []]
      (if (= idx len)
        (conj parts (str/trim (subs text start idx)))
        (let [ch (.charAt ^String text idx)]
          (cond
            (= ch \") (recur (inc idx) start depth (not in-quote?) parts)
            in-quote? (recur (inc idx) start depth in-quote? parts)
            (= ch \() (recur (inc idx) start (inc depth) in-quote? parts)
            (= ch \)) (recur (inc idx) start (dec depth) in-quote? parts)
            (and (= ch \,) (zero? depth))
            (recur (inc idx) (inc idx) depth in-quote?
                   (conj parts (str/trim (subs text start idx))))
            :else (recur (inc idx) start depth in-quote? parts)))))))

(defn- fixed-list-element-type [^String type-name]
  (when-let [[_ elem] (re-matches #"(?is)^\s*([A-Z0-9_]+)\s*\[\d+\]\s*$" type-name)]
    elem))

(defn ^:no-doc list-type? [type-name]
  (let [type-name (str/trim (str type-name))]
    (or (str/ends-with? type-name "[]")
        (some? (fixed-list-element-type type-name)))))

(defn- list-element-type [type-name]
  (let [type-name (str/trim (str type-name))]
    (if (str/ends-with? type-name "[]")
      (subs type-name 0 (- (.length ^String type-name) 2))
      (fixed-list-element-type type-name))))

(defn ^:no-doc struct-type? [type-name]
  (starts-with-ci? type-name "STRUCT("))

(defn ^:no-doc map-type? [type-name]
  (starts-with-ci? type-name "MAP("))

(defn ^:no-doc enum-type? [type-name]
  (= "ENUM" (some-> type-name str str/trim str/upper-case)))

(defn- field-name-and-type [segment]
  (let [segment (str/trim (str segment))]
    (if (str/starts-with? segment "\"")
      (let [end (.indexOf ^String segment "\"" 1)]
        [(subs segment 1 end) (str/trim (subs segment (inc end)))])
      (let [[field-name field-type] (str/split segment #"\s+" 2)]
        [field-name (str/trim field-type)]))))

(defn- struct-field-pairs [type-name]
  (mapv field-name-and-type (top-level-split (inside-parens (str/trim (str type-name))))))

(defn ^:no-doc struct-fields [type-name]
  (mapv first (struct-field-pairs type-name)))

(defn ^:no-doc struct-field-types [type-name]
  (mapv second (struct-field-pairs type-name)))

(defn- map-key-value-types [type-name]
  (let [[key-type value-type] (top-level-split (inside-parens (str/trim (str type-name))))]
    [key-type value-type]))

(declare clj->duckdb)

(defn- clj->duckdb-list [type-name value]
  (let [element-type (list-element-type type-name)]
    (DuckDBUserArray.
     element-type
     (object-array (map #(clj->duckdb element-type %) value)))))

(defn- missing-struct-field [type-name field]
  (ex-info
   (str "Missing STRUCT field " field)
   {:duckdb/error :missing-struct-field
    :field field
    :type type-name}))

(defn- clj->duckdb-struct [type-name value]
  (let [fields (struct-fields type-name)
        field-types (struct-field-types type-name)]
    (DuckDBUserStruct.
     type-name
     (object-array
      (map (fn [field field-type]
             (let [k (keyword field)]
               (when-not (contains? value k)
                 (throw (missing-struct-field type-name field)))
               (clj->duckdb field-type (get value k))))
           fields
           field-types)))))

(defn- clj->duckdb-map [type-name value]
  (let [[key-type value-type] (map-key-value-types type-name)
        m (LinkedHashMap.)]
    (doseq [[k v] value]
      (.put m (clj->duckdb key-type k) (clj->duckdb value-type v)))
    (DuckDBMap. type-name m)))

(defn- clj->duckdb [type-name value]
  (cond
    (nil? value) nil
    (list-type? type-name) (clj->duckdb-list type-name value)
    (struct-type? type-name) (clj->duckdb-struct type-name value)
    (map-type? type-name) (clj->duckdb-map type-name value)
    :else value))

(defn- parameter-type-name [^PreparedStatement stmt ^long ix]
  (try
    (let [type-name (.getParameterTypeName (.getParameterMetaData stmt) ix)]
      (when-not (#{"INVALID" "UNKNOWN" ""} (some-> type-name str str/trim str/upper-case))
        type-name))
    (catch Exception _
      nil)))

(defn- set-duckdb-parameter [value ^PreparedStatement stmt ^long ix]
  (let [type-name (parameter-type-name stmt ix)]
    (if (and type-name
             (or (list-type? type-name) (struct-type? type-name) (map-type? type-name)))
      (.setObject stmt ix (clj->duckdb type-name value))
      (.setObject stmt ix value))))

(defn- set-duckdb-keyword-parameter [^clojure.lang.Keyword value ^PreparedStatement stmt ^long ix]
  (let [type-name (parameter-type-name stmt ix)
        value-name (name value)]
    (if (enum-type? type-name)
      (.setString stmt ix value-name)
      (.setObject stmt ix value-name))))

(extend-protocol prep/SettableParameter
  clojure.lang.IPersistentVector
  (set-parameter [v stmt ix]
    (set-duckdb-parameter v stmt ix))

  clojure.lang.Seqable
  (set-parameter [v stmt ix]
    (set-duckdb-parameter (vec v) stmt ix))

  clojure.lang.IPersistentMap
  (set-parameter [v stmt ix]
    (set-duckdb-parameter v stmt ix))

  clojure.lang.Keyword
  (set-parameter [v stmt ix]
    (set-duckdb-keyword-parameter v stmt ix)))

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
