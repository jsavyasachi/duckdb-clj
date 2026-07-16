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

(deftest configurable-datasource-properties
  (with-temp-dir
    (fn [dir]
      (let [db-path (path-str dir "read-only.duckdb")]
        (with-open [con (jdbc/get-connection (duckdb/file-datasource db-path))]
          (jdbc/execute! con ["create table existing (id integer)"]))
        (with-open [con (jdbc/get-connection
                         (duckdb/file-datasource db-path {:read-only true}))]
          (is (.isReadOnly con))
          (is (thrown? SQLException
                       (jdbc/execute! con ["create table forbidden (id integer)"])))))))
  (with-open [con (jdbc/get-connection
                   (duckdb/memory-datasource
                    {:settings {:threads 2}
                     :session-init-sql "set TimeZone = 'UTC'"
                     :auto-commit false
                     :user-agent "duckdb-clj-test"}))]
    (is (false? (.getAutoCommit con)))
    (is (= [{:threads 2 :timezone "UTC" :user_agent "duckdb-clj-test"}]
           (jdbc/execute!
            con
            ["select current_setting('threads')::integer as threads,
                     current_setting('TimeZone') as timezone,
                     current_setting('custom_user_agent') as user_agent"])))))

(deftest duplicates-connections-to-the-same-database
  (with-open [con (jdbc/get-connection
                   (duckdb/memory-datasource
                    {:session-init-sql "set TimeZone = 'Asia/Tokyo'"
                     :auto-commit false}))]
    (jdbc/execute! con ["create table shared (id integer)"])
    (jdbc/execute! con ["insert into shared values (42)"])
    (.commit con)
    (with-open [^java.sql.Connection duplicate (duckdb/duplicate con)]
      (is (false? (.getAutoCommit duplicate)))
      (is (= [{:timezone "Asia/Tokyo"}]
             (jdbc/execute! duplicate ["select current_setting('TimeZone') as timezone"])))
      (is (= [{:id 42}] (jdbc/execute! duplicate ["select * from shared"]))))
    (is (= [{:id 42}] (jdbc/execute! con ["select * from shared"])))))

(deftest duplicate-inherits-read-only-mode
  (with-temp-dir
    (fn [dir]
      (let [db-path (path-str dir "duplicate-read-only.duckdb")]
        (with-open [con (jdbc/get-connection (duckdb/file-datasource db-path))]
          (jdbc/execute! con ["create table existing (id integer)"]))
        (with-open [con (jdbc/get-connection
                         (duckdb/file-datasource db-path {:read-only true}))
                    ^java.sql.Connection duplicate (duckdb/duplicate con)]
          (is (.isReadOnly duplicate))
          (is (thrown? SQLException
                       (jdbc/execute! duplicate ["insert into existing values (1)"]))))))))

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

(deftest exposes-explicit-driver-global-lifecycle
  (with-temp-dir
    (fn [dir]
      (let [db-path (path-str dir "pinned.duckdb")
            jdbc-url (str "jdbc:duckdb:" db-path)]
        (with-open [con (jdbc/get-connection
                         (duckdb/file-datasource db-path {:pin-db true}))]
          (jdbc/execute! con ["create table pinned_value (id integer)"]))
        (is (true? (duckdb/release-db! jdbc-url)))
        (is (false? (duckdb/release-db! jdbc-url))))))
  (duckdb/clear-functions-registry!)
  (is (= [] (duckdb/registered-functions)))
  (is (boolean? (duckdb/shutdown-cancel-scheduler!))))

(deftest appends-map-rows-in-table-column-order
  (with-open [con (jdbc/get-connection (duckdb/memory-datasource))]
    (jdbc/execute! con ["create table append_values (id int, name varchar, score double)"])
    (is (= 4
           (duckdb/append!
            con
            :append_values
            [{:score 1.5 :name "alice" :id 1}
             {:name "bob" :id 2 :score 2.25}
             {:id 3 :score 3.75 :name "carol"}
             {:id 4 :name "dave" :score nil}])))
    (is (= [{:id 1 :name "alice" :score 1.5}
            {:id 2 :name "bob" :score 2.25}
            {:id 3 :name "carol" :score 3.75}
            {:id 4 :name "dave" :score nil}]
           (jdbc/execute! con ["select * from append_values order by id"])))))

(deftest appends-to-catalog-and-schema-qualified-tables
  (with-open [con (jdbc/get-connection (duckdb/memory-datasource))]
    (jdbc/execute! con ["create schema analytics"])
    (jdbc/execute! con ["create table analytics.events (id integer, label varchar)"])
    (let [catalog (:catalog (first (jdbc/execute! con ["select current_catalog() as catalog"])))]
      (is (= 2
             (duckdb/append!
              con
              {:catalog catalog :schema :analytics :table :events}
              [{:id 1 :label "one"} {:id 2 :label "two"}]))))
    (is (= [{:id 1 :label "one"} {:id 2 :label "two"}]
           (jdbc/execute! con ["select * from analytics.events order by id"])))))

(deftest appends-blob-byte-arrays
  (with-open [con (jdbc/get-connection (duckdb/memory-datasource))]
    (jdbc/execute! con ["create table blobs (id integer, payload blob)"])
    (is (= 1 (duckdb/append! con :blobs [{:id 1 :payload (byte-array [0 1 -1])}])))
    (let [^java.sql.Blob payload (:payload (first (jdbc/execute! con ["select payload from blobs"])))]
      (is (= [0 1 -1]
             (vec (.getBytes payload 1 (int (.length payload)))))))))

(deftest appends-explicit-and-composite-values
  (with-open [con (jdbc/get-connection (duckdb/memory-datasource))]
    (jdbc/execute!
     con
     ["create table advanced
       (defaulted integer default 99,
        choice union(i integer, s varchar),
        masked integer[],
        fixed integer[3],
        huge hugeint,
        uid uuid,
        day date,
        nanos timestamp_ns)"])
    (is (= 1
           (duckdb/append!
            con
            :advanced
            [{:defaulted (duckdb/default-value)
              :choice (duckdb/union-value :i 7)
              :masked (duckdb/array-value
                       (int-array [1 2 3])
                       (boolean-array [true false true]))
              :fixed (duckdb/fixed-size-value [4 5 6] 3)
              :huge (duckdb/hugeint-value 42 0)
              :uid (duckdb/uuid-value 0 1)
              :day (duckdb/epoch-days 1)
              :nanos (duckdb/epoch-nanos 1001)}])))
    (is (= [{:defaulted 99
             :choice 7
             :masked [1 nil 3]
             :fixed [4 5 6]
             :huge "42"
             :uid "00000000-0000-0000-0000-000000000001"
             :day "1970-01-02"
             :nanos 1001}]
           (jdbc/execute!
            con
            ["select defaulted,
                     union_extract(choice, 'i') as choice,
                     masked,
                     fixed,
                     huge::varchar as huge,
                     uid::varchar as uid,
                     day::varchar as day,
                     epoch_ns(nanos) as nanos
                from advanced"])))))

(deftest appends-single-column-values
  (with-open [con (jdbc/get-connection (duckdb/memory-datasource))]
    (jdbc/execute! con ["create table single_int (value integer)"])
    (jdbc/execute! con ["create table single_timestamp (value timestamp)"])
    (jdbc/execute! con ["create table single_decimal (value decimal(10, 2))"])
    (jdbc/execute! con ["create table single_string (value varchar)"])
    (jdbc/execute! con ["create table single_blob (value blob)"])
    (is (= 3 (duckdb/append-single! con :main :single_int [1 2 3])))
    (let [timestamp (java.time.LocalDateTime/of 2026 7 16 12 34 56)
          decimal (bigdec "1234.50")]
      (is (= 1 (duckdb/append-single! con :main :single_timestamp [timestamp])))
      (is (= 1 (duckdb/append-single! con :main :single_decimal [decimal])))
      (is (= 2 (duckdb/append-single! con :main :single_string ["duck" nil])))
      (is (= 1 (duckdb/append-single! con :main :single_blob [(byte-array [0 1 -1])])))
      (is (= [{:value 1} {:value 2} {:value 3}]
             (jdbc/execute! con ["select * from single_int order by value"])))
      (is (= [{:value "2026-07-16 12:34:56"}]
             (jdbc/execute! con ["select value::varchar as value from single_timestamp"])))
      (is (= [{:value decimal}]
             (jdbc/execute! con ["select * from single_decimal"])))
      (is (= [{:value "duck"} {:value nil}]
             (jdbc/execute! con ["select * from single_string order by value nulls last"])))
      (is (= [{:value "0001FF"}]
             (jdbc/execute! con ["select hex(value) as value from single_blob"]))))))

(deftest appends-nested-map-rows
  (with-open [con (jdbc/get-connection (duckdb/memory-datasource))]
    (jdbc/execute!
     con
     ["create table append_nested
       (id int,
        tags varchar[],
        info struct(name varchar, age int))"])
    (is (= 2
           (duckdb/append!
            con
            :append_nested
            [{:id 1 :tags ["a" "b"] :info {:name "alice" :age 30}}
             {:id 2 :tags [] :info {:name "bob" :age 41}}])))
    (is (= [{:id 1 :tags ["a" "b"] :info {:name "alice" :age 30}}
            {:id 2 :tags [] :info {:name "bob" :age 41}}]
           (jdbc/execute! con ["select * from append_nested order by id"])))))

(deftest wraps-appender-failures-with-row-context
  (with-open [con (jdbc/get-connection (duckdb/memory-datasource))]
    (jdbc/execute! con ["create table append_failures (id int, name varchar)"])
    (let [e (is (thrown? clojure.lang.ExceptionInfo
                         (duckdb/append!
                          con
                          :append_failures
                          [{:id 1 :name "ok"}
                           {:id "not-an-int" :name "bad"}])))]
      (is (= :append-failed (:duckdb/error (ex-data e))))
      (is (= 1 (:row-index (ex-data e))))
      (is (= "java.sql.SQLException" (:cause-class (ex-data e))))
      (is (string? (:cause-message (ex-data e)))))))
