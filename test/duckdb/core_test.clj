(ns duckdb.core-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [duckdb.core :as duckdb]
            [next.jdbc :as jdbc])
  (:import (java.nio.file Files Path)
           (java.sql SQLException)))

(set! *warn-on-reflection* true)

(defn- temp-dir []
  (Files/createTempDirectory "duckdb-clj-core-test-" (make-array java.nio.file.attribute.FileAttribute 0)))

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

(defn- sql-string [s]
  (str "'" (str/replace (str s) "'" "''") "'"))

(deftest datasource-helpers
  (with-open [con (jdbc/get-connection (duckdb/memory-datasource))]
    (is (= [{:v 1}] (jdbc/execute! con ["select 1 as v"])))))

(deftest reads-parquet-with-coercion-and-options
  (with-temp-dir
    (fn [dir]
      (let [parquet-path (path-str dir "t.parquet")
            ds (duckdb/memory-datasource)]
        (with-open [con (jdbc/get-connection ds)]
          (jdbc/execute!
           con
           ["create table seed as
             select 1 as id, ['a', 'b']::varchar[] as tags
             union all
             select 2 as id, ['c']::varchar[] as tags"])
          (jdbc/execute!
           con
           [(str "copy (select * from seed order by id) to "
                 (sql-string parquet-path)
                 " (format parquet)")])
          (is (= [{:id 1 :tags ["a" "b"]}
                  {:id 2 :tags ["c"]}]
                 (duckdb/read-parquet con parquet-path)))
          (let [rows (duckdb/read-parquet con parquet-path {:file-row-number true})]
            (is (= [0 1] (mapv :file_row_number rows)))
            (is (= [["a" "b"] ["c"]] (mapv :tags rows))))
          (let [e (is (thrown? clojure.lang.ExceptionInfo
                               (duckdb/read-parquet con parquet-path {(keyword "bad-\"opt") true})))]
            (is (= :invalid-option (:duckdb/error (ex-data e))))))))))

(deftest reads-csv-with-options
  (with-temp-dir
    (fn [dir]
      (let [csv-path (path-str dir "t.csv")
            ds (duckdb/memory-datasource)]
        (with-open [con (jdbc/get-connection ds)]
          (jdbc/execute!
           con
           ["create table seed as
             select 1 as id, 'alice' as name
             union all
             select 2 as id, 'bob' as name"])
          (jdbc/execute!
           con
           [(str "copy (select * from seed order by id) to "
                 (sql-string csv-path)
                 " (format csv, header)")])
          (is (= [{:id 1 :name "alice"}
                  {:id 2 :name "bob"}]
                 (duckdb/read-csv con csv-path {:header true}))))))))

(deftest attaches-and-detaches-file-databases
  (with-temp-dir
    (fn [dir]
      (let [db-path (path-str dir "attached.duckdb")]
        (with-open [file-con (jdbc/get-connection (duckdb/file-datasource db-path))]
          (jdbc/execute! file-con ["create table items as select 42 as id, 'answer' as label"]))
        (with-open [con (jdbc/get-connection (duckdb/memory-datasource))]
          (duckdb/attach! con db-path 'attached {:read-only true})
          (is (= [{:id 42 :label "answer"}]
                 (jdbc/execute! con ["select * from attached.items"])))
          (duckdb/detach! con 'attached)
          (is (thrown? SQLException
                       (jdbc/execute! con ["select * from attached.items"])))
          (let [e (is (thrown? clojure.lang.ExceptionInfo
                               (duckdb/attach! con db-path "bad-alias")))]
            (is (= :invalid-alias (:duckdb/error (ex-data e))))))))))

(deftest extension-names-are-validated-before-sql-executes
  (with-open [con (jdbc/get-connection (duckdb/memory-datasource))]
    (let [install-error (is (thrown? clojure.lang.ExceptionInfo
                                     (duckdb/install-extension! con "bad-name")))
          load-error (is (thrown? clojure.lang.ExceptionInfo
                                  (duckdb/load-extension! con "bad-name")))]
      (is (= :invalid-alias (:duckdb/error (ex-data install-error))))
      (is (= :invalid-alias (:duckdb/error (ex-data load-error)))))
    (is (thrown? SQLException
                 (duckdb/load-extension! con "garbage_valid_name")))))

(deftest returns-duckdb-version
  (with-open [con (jdbc/get-connection (duckdb/memory-datasource))]
    (let [version (duckdb/duckdb-version con)]
      (is (string? version))
      (is (str/starts-with? version "v"))
      (is (not (str/blank? version))))))
