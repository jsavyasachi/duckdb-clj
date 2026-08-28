(ns duckdb.arrow-test
  (:require [clojure.test :refer [deftest is]]
            [duckdb.arrow :as arrow]
            [duckdb.core :as duckdb]
            [next.jdbc :as jdbc])
  (:import (org.apache.arrow.memory RootAllocator)))

(set! *warn-on-reflection* true)

(deftest exports-and-registers-an-arrow-stream-without-leaking
  (let [expected [{:id 1 :name "alice" :score 1.5}
                  {:id 2 :name "bob" :score nil}
                  {:id 3 :name "carol" :score 3.75}]
        allocator (RootAllocator.)]
    (try
      (with-open [con (jdbc/get-connection (duckdb/memory-datasource))]
        (jdbc/execute!
         con
         ["create table source (id integer, name varchar, score double)"])
        (jdbc/execute!
         con
         ["insert into source values
           (1, 'alice', 1.5),
           (2, 'bob', null),
           (3, 'carol', 3.75)"])
        (is (= expected
               (arrow/with-arrow-reader
                 con
                 ["select * from source order by id"]
                 allocator
                 2
                 (fn [reader]
                   (with-open [stream (arrow/register-arrow!
                                       con "arrow_copy" allocator reader)]
                     (jdbc/execute!
                      con
                      ["select * from arrow_copy order by id"])))))))
      (finally
        (is (zero? (.getAllocatedMemory allocator)))
        (.close allocator)))))

(deftest arrow-reader-closes-after-consumer-failure
  (let [allocator (RootAllocator.)]
    (try
      (with-open [con (jdbc/get-connection (duckdb/memory-datasource))]
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo
             #"consumer failed"
             (arrow/with-arrow-reader
               con ["select 1 as value"] allocator
               (fn [_] (throw (ex-info "consumer failed" {})))))))
      (finally
        (is (zero? (.getAllocatedMemory allocator)))
        (.close allocator)))))

(deftest arrow-export-rejects-invalid-batch-size-without-leaking
  (let [allocator (RootAllocator.)]
    (try
      (with-open [con (jdbc/get-connection (duckdb/memory-datasource))]
        (is (thrown? clojure.lang.ExceptionInfo
                     (arrow/arrow-export con ["select 1"] allocator 0))))
      (finally
        (is (zero? (.getAllocatedMemory allocator)))
        (.close allocator)))))
