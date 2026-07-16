(ns duckdb.meta-test
  (:require [clojure.test :refer [deftest is testing]]
            [duckdb.core :as duckdb]
            [duckdb.meta :as meta]
            [next.jdbc :as jdbc])
  (:import (java.math BigInteger)
           (java.sql Blob Connection ResultSet Statement)
           (java.time OffsetTime)
           (java.util UUID)
           (org.duckdb JsonNode)))

(set! *warn-on-reflection* true)

(defn- typed-row [^Connection con]
  (let [^Statement stmt (.createStatement con)
        ^ResultSet rs
        (.executeQuery
         stmt
         "select
               170141183460469231731687303715884105727::hugeint as h,
               340282366920938463463374607431768211455::uhugeint as uh,
               '123e4567-e89b-12d3-a456-426614174000'::uuid as id,
               '{\"answer\":42}'::json as doc,
               'lazy value'::varchar as txt,
               '12:34:56+02:30'::timetz as at,
               from_hex('0001ff') as payload,
               [1, 2, 3]::integer[] as nums,
               {'name': 'Ada', 'details': {'active': true}} as person")]
    [stmt rs]))

(deftest reads-duckdb-specific-result-values
  (with-open [con (jdbc/get-connection (duckdb/memory-datasource))]
    (let [[^Statement stmt ^ResultSet rs] (typed-row con)]
      (with-open [stmt stmt rs rs]
        (is (.next rs))
        (is (= (BigInteger. "170141183460469231731687303715884105727")
               (meta/hugeint rs :h)))
        (is (= (BigInteger. "340282366920938463463374607431768211455")
               (meta/uhugeint rs "uh")))
        (is (= (UUID/fromString "123e4567-e89b-12d3-a456-426614174000")
               (meta/uuid rs :id)))
        (is (instance? JsonNode (meta/json-node rs :doc)))
        (is (= "{\"answer\":42}" (str (meta/json-node rs :doc))))
        (is (= "lazy value" (meta/lazy-string rs :txt)))
        (is (= (OffsetTime/parse "12:34:56+02:30")
               (meta/offset-time rs :at)))
        (let [value (meta/blob rs :payload)]
          (is (instance? Blob value))
          (is (= [0 1 -1] (vec (.getBytes ^Blob value 1 3)))))
        (is (= [1 2 3] (meta/array rs :nums)))
        (is (= {:name "Ada" :details {:active true}}
               (meta/struct rs :person)))))))

(deftest typed-result-values-preserve-sql-null
  (with-open [con (jdbc/get-connection (duckdb/memory-datasource))
              stmt (.createStatement con)
              rs (.executeQuery stmt "select null::hugeint as h, null::uhugeint as uh")]
    (is (.next rs))
    (is (nil? (meta/hugeint rs :h)))
    (is (nil? (meta/uhugeint rs :uh)))))

(deftest lists-catalogs-schemas-tables-and-columns
  (with-open [con (jdbc/get-connection (duckdb/memory-datasource))]
    (jdbc/execute! con ["create schema app"])
    (jdbc/execute! con ["create table app.accounts
                          (id integer primary key,
                           email varchar not null)"])
    (is (some #(= "memory" (:table_cat %)) (meta/catalogs con)))
    (is (some #(and (= "app" (:table_schem %))
                    (= "memory" (:table_catalog %)))
              (meta/schemas con {:catalog "memory" :schema-pattern "app"})))
    (is (= [{:table_cat "memory"
             :table_schem "app"
             :table_name "accounts"
             :table_type "TABLE"
             :remarks nil
             :type_cat nil
             :type_schem nil
             :type_name nil
             :self_referencing_col_name nil
             :ref_generation nil}]
           (meta/tables con {:catalog "memory"
                             :schema-pattern "app"
                             :table-pattern "accounts"
                             :types ["TABLE"]})))
    (let [columns (meta/columns con {:catalog "memory"
                                     :schema-pattern "app"
                                     :table-pattern "accounts"})]
      (is (= ["id" "email"] (mapv :column_name columns)))
      (is (= ["INTEGER" "VARCHAR"] (mapv :type_name columns)))
      (is (= ["NO" "NO"] (mapv :is_nullable columns))))))

(deftest lists-keys-and-indexes
  (with-open [con (jdbc/get-connection (duckdb/memory-datasource))]
    (jdbc/execute! con ["create table authors (id integer primary key)"])
    (jdbc/execute! con ["create table books
                          (id integer primary key,
                           author_id integer references authors(id))"])
    (jdbc/execute! con ["create index books_author_idx on books(author_id)"])
    (is (= ["id"]
           (mapv :column_name (meta/primary-keys con "authors"))))
    (is (= [["authors" "id" "books" "author_id"]]
           (mapv (juxt :pktable_name :pkcolumn_name
                       :fktable_name :fkcolumn_name)
                 (meta/imported-keys con "books"))))
    (is (= ["books"]
           (mapv :fktable_name (meta/exported-keys con "authors"))))
    (is (some #(= "books_author_idx" (:index_name %))
              (meta/indexes con "books")))))

(deftest returns-type-driver-and-jdbc-capability-info
  (with-open [con (jdbc/get-connection (duckdb/memory-datasource))]
    (is (some #(and (= "INTEGER" (:type_name %))
                    (integer? (:data_type %)))
              (meta/type-info con)))
    (let [info (meta/driver-info con)]
      (is (= "DuckDBJ" (:driver-name info)))
      (is (= "DuckDB" (:database-product-name info)))
      (is (pos-int? (:jdbc-major-version info)))
      (is (string? (:driver-version info))))
    (let [capabilities (meta/capabilities con)]
      (is (true? (:transactions capabilities)))
      (is (true? (:batch-updates capabilities)))
      (is (true? (:outer-joins capabilities)))
      (is (contains? capabilities :savepoints)))))
