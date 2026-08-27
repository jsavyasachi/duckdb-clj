(ns duckdb.copy-test
  (:require [clojure.test :refer [deftest is testing]]
            [duckdb.core :as duckdb]
            [next.jdbc :as jdbc])
  (:import (java.nio.file Files Path)
           (java.nio.file.attribute FileAttribute)))

(set! *warn-on-reflection* true)

(defn- temp-dir []
  (Files/createTempDirectory "duckdb-clj-copy-test-" (make-array FileAttribute 0)))

(defn- delete-tree [^Path path]
  (when (Files/exists path (make-array java.nio.file.LinkOption 0))
    (let [paths (reverse (iterator-seq (.iterator (Files/walk path (make-array java.nio.file.FileVisitOption 0)))))]
      (doseq [^Path p paths]
        (Files/deleteIfExists p)))))

(defn- with-temp-dir [f]
  (let [dir (temp-dir)]
    (try
      (f dir)
      (finally
        (delete-tree dir)))))

(defn- path-str [^Path dir name]
  (str (.resolve dir ^String name)))

(defn- seed! [con]
  (jdbc/execute!
   con
   ["create table copy_seed as
     select 1 as id, 'alice' as name, 'west' as region
     union all
     select 2 as id, 'bob' as name, 'east' as region
     union all
     select 3 as id, 'cara' as name, 'west' as region"])
  (jdbc/execute! con ["select * from copy_seed order by id"]))

(deftest copy-to-parquet-round-trips-query
  (with-temp-dir
    (fn [dir]
      (with-open [con (jdbc/get-connection (duckdb/memory-datasource))]
        (let [path (path-str dir "result'quoted.parquet")]
          (seed! con)
          (duckdb/copy-to-parquet! con ["select id, name from copy_seed where id > ? order by id" 1]
                                    path {:compression :zstd :row-group-size 2048})
          (is (= [{:id 2 :name "bob"} {:id 3 :name "cara"}]
                 (duckdb/read-parquet con path))))))))

(deftest copy-to-csv-round-trips-table
  (with-temp-dir
    (fn [dir]
      (with-open [con (jdbc/get-connection (duckdb/memory-datasource))]
        (let [path (path-str dir "result.csv")]
          (seed! con)
          (duckdb/copy-to-csv! con :copy_seed path {:header? true :delimiter ";"})
          (is (= [{:id 1 :name "alice" :region "west"}
                  {:id 2 :name "bob" :region "east"}
                  {:id 3 :name "cara" :region "west"}]
                 (duckdb/read-csv con path {:header true :delim ";"}))))))))

(deftest copy-to-json-round-trips-array
  (with-temp-dir
    (fn [dir]
      (with-open [con (jdbc/get-connection (duckdb/memory-datasource))]
        (let [path (path-str dir "result.json")]
          (seed! con)
          (duckdb/copy-to-json! con "select id, name from copy_seed order by id" path {:array? true})
          (is (= [{:id 1 :name "alice"} {:id 2 :name "bob"} {:id 3 :name "cara"}]
                 (jdbc/execute! con ["select id, name from read_json(?, format = 'array') order by id" path]))))))))

(deftest copy-to-partitioned-parquet-round-trips
  (with-temp-dir
    (fn [dir]
      (with-open [con (jdbc/get-connection (duckdb/memory-datasource))]
        (let [path (path-str dir "partitioned")]
          (seed! con)
          (duckdb/copy-to-parquet! con :copy_seed path
                                    {:partition-by [:region]
                                     :overwrite-or-ignore? true})
          (is (= [{:id 1 :name "alice" :region "west"}
                  {:id 2 :name "bob" :region "east"}
                  {:id 3 :name "cara" :region "west"}]
                 (jdbc/execute!
                  con
                  ["select id, name, region from read_parquet(?, hive_partitioning = true) order by id"
                   (str path "/*/*.parquet")]))))))))

(deftest copy-to-rejects-invalid-options
  (with-open [con (jdbc/get-connection (duckdb/memory-datasource))]
    (let [error (is (thrown? clojure.lang.ExceptionInfo
                             (duckdb/copy-to-parquet! con "select 1 as id" "/tmp/nope.parquet"
                                                       {:compression :definitely-not-valid})))]
      (is (= :invalid-option (:duckdb/error (ex-data error)))))))
