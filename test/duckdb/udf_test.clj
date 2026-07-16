(ns duckdb.udf-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [duckdb.core :as duckdb]
            [duckdb.udf :as udf]
            [next.jdbc :as jdbc])
  (:import (java.sql SQLException)
           (java.util UUID)
           (org.duckdb DuckDBColumnType DuckDBWritableVector)))

(set! *warn-on-reflection* true)

(defn- unique-name [prefix]
  (str prefix "_" (str/replace (str (UUID/randomUUID)) "-" "_")))

(deftest registers-scalar-functions
  (with-open [con (jdbc/get-connection (duckdb/memory-datasource))]
    (testing "typed integer function with NULL propagation"
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

(deftest public-docstrings-warn-that-registration-cannot-be-removed
  (doseq [v [#'udf/register-scalar! #'udf/register-table!]]
    (let [doc (:doc (meta v))]
      (is (str/includes? doc "PROCESS-GLOBAL"))
      (is (str/includes? doc "CANNOT be removed"))
      (is (str/includes? doc "duckdb_jdbc 1.5.4.0"))
      (is (str/includes? doc "stable, unique names")))))
