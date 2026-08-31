(ns duckdb.exec-test
  (:require [clojure.test :refer [deftest is]]
            [duckdb.core :as duckdb]
            [duckdb.exec :as exec]
            [next.jdbc :as jdbc])
  (:import (java.math BigInteger)
           (java.time LocalTime)
           (java.util UUID)
           (java.nio.file Files Path)
           (java.sql SQLException)
           (org.duckdb DuckDBConnection DuckDBPreparedStatement)))

(set! *warn-on-reflection* true)

(defn- read-single-value [con sql]
  (with-open [^DuckDBPreparedStatement stmt (exec/prepare con sql)]
    (-> (exec/read-chunks stmt) first vals first first)))

(deftest reads-uuid-values-from-native-chunks
  (with-open [con (jdbc/get-connection (duckdb/memory-datasource))]
    (is (= (UUID/fromString "550e8400-e29b-41d4-a716-446655440000")
           (read-single-value con
                              "select '550e8400-e29b-41d4-a716-446655440000'::UUID as value")))))

(deftest reads-json-values-from-native-chunks
  (with-open [con (jdbc/get-connection (duckdb/memory-datasource))]
    (is (= "[1,2,3]"
           (read-single-value con "select '[1,2,3]'::JSON as value")))))

(deftest reads-blob-values-from-native-chunks
  (with-open [con (jdbc/get-connection (duckdb/memory-datasource))]
    (is (= [97 98 99]
           (seq (read-single-value con "select 'abc'::BLOB as value"))))))

(deftest reads-time-values-from-native-chunks
  (with-open [con (jdbc/get-connection (duckdb/memory-datasource))]
    (is (= (LocalTime/of 12 34 56 123456000)
           (read-single-value con "select '12:34:56.123456'::TIME as value")))))

(deftest reads-remaining-native-scalar-types-from-native-chunks
  (with-open [con (jdbc/get-connection (duckdb/memory-datasource))]
    (with-open [^DuckDBPreparedStatement stmt
                (exec/prepare con
                              "select true as bool, 1::utinyint as ub,
                                      1.5::float as real, 12.34::decimal(10,2) as amount,
                                      '2024-01-02'::date as day,
                                      '2024-01-02 03:04:05'::timestamp as happened,
                                      '2024-01-02 03:04:05+00'::timestamptz as zoned")]
      (let [row (first (exec/read-chunks stmt))]
        (is (= true (first (:bool row))))
        (is (= 1 (first (:ub row))))
        (is (= 1.5 (first (:real row))))
        (is (= (bigdec "12.34") (first (:amount row))))
        (is (= (java.time.LocalDate/of 2024 1 2) (first (:day row))))
        (is (instance? java.sql.Timestamp (first (:happened row))))
        (is (instance? java.time.OffsetDateTime (first (:zoned row))))))))

(deftest reads-enum-values-from-native-chunks
  (with-open [con (jdbc/get-connection (duckdb/memory-datasource))]
    (jdbc/execute! con ["create type mood as enum ('sad', 'ok', 'happy')"])
    (is (= "happy"
           (read-single-value con "select 'happy'::mood as value")))))

(deftest reads-list-values-from-native-chunks
  (with-open [con (jdbc/get-connection (duckdb/memory-datasource))]
    (is (= [1 2 3]
           (read-single-value con "select [1,2,3]::INT[] as value")))))

(deftest reads-struct-values-from-native-chunks
  (with-open [con (jdbc/get-connection (duckdb/memory-datasource))]
    (is (= {:a 1 :b "x"}
           (read-single-value con "select {'a': 1, 'b': 'x'} as value")))))

(deftest reads-map-values-from-native-chunks
  (with-open [con (jdbc/get-connection (duckdb/memory-datasource))]
    (is (= {"k" 1}
           (read-single-value con "select MAP {'k': 1} as value")))))

(deftest reduces-jdbc-fallback-chunks-incrementally
  (with-open [con (jdbc/get-connection (duckdb/memory-datasource))
              ^DuckDBPreparedStatement stmt
              (exec/prepare con "select repeat('x', 1)::BLOB as payload from range(5000)")]
    (let [rows-read (atom 0)
          jdbc-row (var-get (ns-resolve 'duckdb.exec 'jdbc-row))]
      (with-redefs [duckdb.exec/jdbc-row
                    (fn [rs metadata column-count]
                      (swap! rows-read inc)
                      (jdbc-row rs metadata column-count))]
        (is (= 2048
               (exec/reduce-chunks stmt
                                    (fn [_ chunk]
                                      (reduced (count (:payload chunk))))
                                    nil)))
        (is (= 2048 @rows-read))))))

(deftest reduces-large-native-results-in-streaming-mode
  (let [ds (duckdb/memory-datasource)]
    (is (= 4999950000
           (exec/reduce-streaming
            ds
            ["select i from range(100000) t(i)"]
            (map :i)
            +
            0)))))

(deftest streaming-reduction-preserves-read-only-datasource-mode
  (let [^Path db-path (Files/createTempFile "duckdb-clj-streaming-" ".duckdb"
                                            (make-array java.nio.file.attribute.FileAttribute 0))]
    (try
      (Files/deleteIfExists db-path)
      (with-open [con (jdbc/get-connection (duckdb/file-datasource (str db-path)))]
        (jdbc/execute! con ["create table protected (id integer)"]))
      (is (thrown? SQLException
                   (exec/reduce-streaming
                    (duckdb/file-datasource (str db-path) {:read-only true})
                    ["insert into protected values (1) returning id"]
                    (map :id)
                    conj
                    [])))
      (finally
        (Files/deleteIfExists db-path)))))

(deftest streaming-reduction-runs-datasource-session-initialization
  (is (= ["Asia/Tokyo"]
         (exec/reduce-streaming
          (duckdb/memory-datasource {:session-init-sql "set TimeZone = 'Asia/Tokyo'"})
          ["select current_setting('TimeZone') as timezone"]
          (map :timezone)
          conj
          []))))

(deftest streaming-reduction-refuses-foreign-datasources
  (let [source (duckdb/memory-datasource)
        foreign-source (proxy [javax.sql.DataSource] []
                         (getConnection [] (.getConnection ^javax.sql.DataSource source)))]
    (try
      (exec/reduce-streaming foreign-source ["select 1"] (map :v) conj [])
      (is false "foreign datasources must not be downgraded to URL-only connections")
      (catch clojure.lang.ExceptionInfo error
        (is (= :unsupported-streaming-datasource
               (:duckdb/error (ex-data error))))))))

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

(deftest binds-duckdb-specific-values-on-reusable-statements
  (with-open [con (jdbc/get-connection (duckdb/memory-datasource))
              ^DuckDBPreparedStatement stmt
              (exec/prepare con "select ?::uuid as id, ?::decimal(10,2) as amount, (?::date)::varchar as day, (?::time)::varchar as at, (?::timestamp)::varchar as happened, hex(?::blob) as payload")]
    (let [bindings [['duckdb.exec/bind-uuid! (java.util.UUID/fromString "550e8400-e29b-41d4-a716-446655440000")]
                    ['duckdb.exec/bind-decimal! (bigdec "12.34")]
                    ['duckdb.exec/bind-date! (java.time.LocalDate/of 2024 1 2)]
                    ['duckdb.exec/bind-time! (java.time.LocalTime/of 3 4 5)]
                    ['duckdb.exec/bind-timestamp! (java.time.LocalDateTime/of 2024 1 2 3 4 5)]
                    ['duckdb.exec/bind-bytes! (byte-array [0 1 -1])]]]
      (doseq [[symbol _] bindings]
        (is (fn? (some-> (resolve symbol) var-get))))
      (when (every? (comp fn? #(some-> (resolve (first %)) var-get)) bindings)
        (doseq [[index [symbol value]] (map-indexed vector bindings)]
          (@(resolve symbol) stmt (inc index) value))
        (is (= {:id (java.util.UUID/fromString "550e8400-e29b-41d4-a716-446655440000")
                :amount (bigdec "12.34")
                :day "2024-01-02"
                :at "03:04:05"
                :happened "2024-01-02 03:04:05"
                :payload "0001FF"}
               (jdbc/execute-one! stmt)))))))

(deftest cancels-long-running-queries-from-another-thread
  (with-open [con (jdbc/get-connection (duckdb/memory-datasource))
              ^DuckDBPreparedStatement stmt
              (exec/prepare con
                            "select sum(a.i * b.i)
                             from range(1000000) a(i), range(1000000) b(i)")]
    (is (identical? stmt (exec/set-query-timeout! stmt 3)))
    (is (= 3 (.getQueryTimeout stmt)))
    (let [executing-thread (.getName (Thread/currentThread))
          timeout-observation (promise)
          error (try
                  (exec/execute-with-timeout!
                   stmt 50
                   (fn [running-stmt]
                     (deliver timeout-observation
                              {:thread (.getName (Thread/currentThread))
                               :progress (exec/query-progress running-stmt)})))
                  nil
                  (catch SQLException e e))
          observation (deref timeout-observation 2000 ::timeout)]
      (is (instance? SQLException error))
      (is (re-find #"(?i)interrupt|cancel" (.getMessage ^SQLException error)))
      (is (not= ::timeout observation))
      (is (not= executing-thread (:thread observation)))
      (is (= #{:processed :total :percentage}
             (set (keys (:progress observation)))))
      (is (every? number? (vals (:progress observation)))))))

(deftest returns-parseable-native-profiling-json
  (let [ds (duckdb/memory-datasource)]
    (with-open [con (jdbc/get-connection ds)]
      (jdbc/execute! con ["set enable_profiling = 'no_output'"])
      (jdbc/execute! con ["select sum(i) as total from range(10000) t(i)"])
      (let [profile (exec/profiling-information con :json)]
        (is (string? profile))
        (is (not-empty profile))
        (with-open [parser-con (jdbc/get-connection ds)]
          (is (true? (:valid (jdbc/execute-one!
                              parser-con
                              ["select json_valid(?) as valid" profile])))))))))
