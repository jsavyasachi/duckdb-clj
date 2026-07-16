(ns duckdb.exec-test
  (:require [clojure.test :refer [deftest is]]
            [duckdb.core :as duckdb]
            [duckdb.exec :as exec]))

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
