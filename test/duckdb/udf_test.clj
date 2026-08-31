(ns duckdb.udf-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [duckdb.core :as duckdb]
            [duckdb.udf :as udf]
            [next.jdbc :as jdbc])
  (:import (java.sql SQLException)
           (java.util UUID)
           (org.duckdb DuckDBColumnType DuckDBLogicalType DuckDBWritableVector)))

(set! *warn-on-reflection* true)

(defn- unique-name [prefix]
  (str prefix "_" (str/replace (str (UUID/randomUUID)) "-" "_")))

(deftest registers-scalar-functions
  (with-open [con (jdbc/get-connection (duckdb/memory-datasource))]
    (testing "typed integer function propagates NULL"
      (let [function-name (unique-name "double_int")
            calls (atom 0)]
        (udf/register-scalar!
         con function-name
         (fn [x]
           (swap! calls inc)
           (* 2 x))
         {:parameters [DuckDBColumnType/INTEGER]
          :return-type DuckDBColumnType/INTEGER
          :null-handling :null-in-null-out
          :volatility :immutable})
        (is (= [{:v 42 :missing nil}]
               (jdbc/execute!
                con
                [(format "select %s(21) as v, %s(NULL::integer) as missing"
                         function-name function-name)])))
        (is (= 1 @calls))))
    (testing "typed string function"
      (let [function-name (unique-name "upper_text")]
        (udf/register-scalar!
         con function-name str/upper-case
         {:parameters [DuckDBColumnType/VARCHAR]
          :return-type DuckDBColumnType/VARCHAR
          :null-handling :special
          :volatility :volatile})
        (is (= [{:v "DUCKDB"}]
               (jdbc/execute! con [(format "select %s('DuckDB') as v" function-name)])))))))

(deftest scalar-exceptions-surface-as-sql-errors
  (with-open [con (jdbc/get-connection (duckdb/memory-datasource))]
    (let [function-name (unique-name "throws_cleanly")]
      (udf/register-scalar!
       con function-name
       (fn [_] (throw (ex-info "intentional scalar failure" {:private :context})))
       {:parameters [DuckDBColumnType/INTEGER]
        :return-type DuckDBColumnType/INTEGER})
      (is (thrown-with-msg?
           SQLException
           #"intentional scalar failure"
           (jdbc/execute! con [(format "select %s(1)" function-name)]))))))

(deftest rejects-unsupported-scalar-udf-types-at-registration
  (with-open [con (jdbc/get-connection (duckdb/memory-datasource))]
    (try
      (udf/register-scalar!
       con
       (unique-name "time_parameter")
       identity
       {:parameters [DuckDBColumnType/TIME]
        :return-type DuckDBColumnType/VARCHAR})
      (is false "TIME must be rejected during registration")
      (catch Throwable error
        (is (instance? clojure.lang.ExceptionInfo error))
        (is (re-find #"TIME" (.getMessage error)))))))

(deftest registers-functional-and-varargs-scalar-functions
  (with-open [con (jdbc/get-connection (duckdb/memory-datasource))]
    (with-open [integer-type (DuckDBLogicalType/of DuckDBColumnType/INTEGER)]
      (let [one (unique-name "functional")
          two (unique-name "bifunctional")
          zero (unique-name "supplier")
          many (unique-name "varargs")]
      (udf/register-scalar! con one #(str "v" %) {:parameters [Integer]
                                                    :return-type String
                                                    :mode :function})
      (udf/register-scalar! con two (fn [a b] (int (+ a b))) {:parameters [Integer Integer]
                                                        :return-type Integer
                                                        :mode :bi-function})
      (udf/register-scalar! con zero (constantly (int 42)) {:return-type Integer
                                                      :mode :supplier})
      (udf/register-scalar! con many (fn [values] (int (reduce + values)))
                             {:return-type Integer
                              :mode :varargs
                              :varargs-type integer-type})
      (is (= [{:one "v7" :two 5 :zero 42 :many 6}]
             (jdbc/execute! con
                            [(format "select %s(7) one, %s(2, 3) two, %s() zero, %s(1, 2, 3) many"
                                     one two zero many)])))))))

(deftest registers-table-functions-with-parameters-state-and-vectors
  (with-open [con (jdbc/get-connection (duckdb/memory-datasource))]
    (let [function-name (unique-name "numbered_labels")]
      (udf/register-table!
       con function-name
       {:parameters [DuckDBColumnType/INTEGER]
        :named-parameters (array-map :prefix DuckDBColumnType/VARCHAR)
        :columns [[:id DuckDBColumnType/INTEGER]
                  [:label DuckDBColumnType/VARCHAR]]
        :cardinality {:rows 3 :exact? true}
        :bind (fn [{:keys [parameters named-parameters]}]
                {:row-count (first parameters)
                 :prefix (:prefix named-parameters)})
        :init (fn [_bind-data] (atom 0))
        :apply (fn [{:keys [bind-data state capacity vectors]}]
                 (let [{:keys [row-count prefix]} bind-data
                       start @state
                       n (min capacity (- row-count start))
                       ^DuckDBWritableVector ids (nth vectors 0)
                       ^DuckDBWritableVector labels (nth vectors 1)]
                   (dotimes [offset n]
                     (let [id (+ start offset)]
                       (.setInt ids offset id)
                       (.setString labels offset (str prefix id))))
                   (swap! state + n)
                   n))})
      (is (= [{:id 0 :label "row-0"}
              {:id 1 :label "row-1"}
              {:id 2 :label "row-2"}]
             (jdbc/execute!
              con
              [(format "select * from %s(3, prefix = 'row-') order by id"
                       function-name)]))))))

(deftest table-functions-support-timestamp-with-time-zone
  (with-open [con (jdbc/get-connection (duckdb/memory-datasource))]
    (let [function-name (unique-name "echo_timestamptz")]
      (udf/register-table!
       con function-name
       {:columns [[:value DuckDBColumnType/TIMESTAMP_WITH_TIME_ZONE]]
        :init (fn [_] (atom false))
        :apply (fn [{:keys [state vectors]}]
                 (if (compare-and-set! state false true)
                   (do
                     (let [^DuckDBWritableVector vector (first vectors)]
                       (.setOffsetDateTime vector 0
                                           (java.time.OffsetDateTime/parse "2024-01-02T03:04:05Z")))
                     1)
                   0))})
      (is (= 1
             (count
              (jdbc/execute!
               con
               [(format "select * from %s()" function-name)])))))))

(deftest public-docstrings-warn-that-registration-cannot-be-removed
  (doseq [v [#'udf/register-scalar! #'udf/register-table!]]
    (let [doc (:doc (meta v))]
      (is (str/includes? doc "PROCESS-GLOBAL"))
      (is (str/includes? doc "CANNOT be removed"))
      (is (str/includes? doc "duckdb_jdbc 1.5.5.1"))
      (is (str/includes? doc "stable, unique names")))))
