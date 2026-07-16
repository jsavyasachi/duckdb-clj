(ns duckdb.exec-test
  (:require [clojure.test :refer [deftest is]]
            [duckdb.core :as duckdb]
            [duckdb.exec :as exec]
            [next.jdbc :as jdbc])
  (:import (java.math BigInteger)
           (org.duckdb DuckDBConnection DuckDBPreparedStatement)))

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

(deftest controls-reusable-native-prepared-statements
  (with-open [con (jdbc/get-connection (duckdb/memory-datasource))
              ^DuckDBPreparedStatement stmt
              (exec/prepare con "select ?::hugeint as n, ?::varchar as label")]
    (is (= ["HUGEINT" "VARCHAR"]
           (mapv :type-name (exec/parameter-metadata stmt))))
    (is (= {:return-type :query-result
            :columns [{:index 1 :name "n" :type-name "HUGEINT"}
                      {:index 2 :name "label" :type-name "VARCHAR"}]}
           (exec/return-metadata stmt)))
    (is (identical? stmt (exec/set-fetch-size! stmt 512)))
    (exec/bind-hugeint! stmt 1 (BigInteger. "170141183460469231731687303715884105726"))
    (exec/bind-parameters! stmt [nil "first"])
    (exec/bind-hugeint! stmt 1 (BigInteger. "170141183460469231731687303715884105726"))
    (is (= {:n (BigInteger. "170141183460469231731687303715884105726")
            :label "first"}
           (jdbc/execute-one! stmt)))
    (exec/bind-parameters! stmt [nil "second"])
    (exec/bind-hugeint! stmt 1 (BigInteger. "-170141183460469231731687303715884105728"))
    (is (= {:n (BigInteger. "-170141183460469231731687303715884105728")
            :label "second"}
           (jdbc/execute-one! stmt)))))
