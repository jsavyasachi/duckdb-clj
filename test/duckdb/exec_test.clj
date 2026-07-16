(ns duckdb.exec-test
  (:require [clojure.test :refer [deftest is]]
            [duckdb.core :as duckdb]
            [duckdb.exec :as exec]
            [next.jdbc :as jdbc])
  (:import (org.duckdb DuckDBConnection DuckDBPreparedStatement)))

(set! *warn-on-reflection* true)

(deftest reduces-large-native-results-in-streaming-mode
  (let [ds (duckdb/memory-datasource)]
    (is (= 4999950000
           (exec/reduce-streaming
            ds
            ["select i from range(100000) t(i)"]
            (map :i)
            +
            0)))))

(deftest reads-prepared-results-as-columnar-chunks
  (with-open [con (jdbc/get-connection (duckdb/memory-datasource))]
    (let [^DuckDBConnection duck-con (.unwrap con DuckDBConnection)]
      (with-open [^DuckDBPreparedStatement stmt
                  (.prepare duck-con
                            "select i::integer as id, 'v' || i as label
                             from range(4097) t(i) order by i")]
        (let [chunks (exec/read-chunks stmt)]
          (is (= [2048 2048 1] (mapv (comp count :id) chunks)))
          (is (= [0 1 2] (subvec (:id (first chunks)) 0 3)))
          (is (= ["v0" "v1" "v2"] (subvec (:label (first chunks)) 0 3)))
          (is (= 4096 (last (:id (last chunks))))))))))
