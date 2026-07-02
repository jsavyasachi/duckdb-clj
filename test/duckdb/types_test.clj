(ns duckdb.types-test
  (:require [clojure.test :refer [deftest is testing]]
            [duckdb.types]
            [next.jdbc :as jdbc]))

(set! *warn-on-reflection* true)

(defn- with-connection [f]
  (let [ds (jdbc/get-datasource "jdbc:duckdb:")]
    (with-open [con (jdbc/get-connection ds)]
      (f con))))

(deftest reads-duckdb-nested-types-as-clojure-data
  (with-connection
    (fn [con]
      (jdbc/execute!
       con
       ["create table t
         (id int,
          tags varchar[],
          info struct(name varchar, age int),
          meta map(varchar, int),
          int_meta map(int, varchar),
          nested struct(xs int[], m map(varchar, int)),
          people struct(name varchar, age int)[],
          no_tags varchar[],
          no_info struct(name varchar, age int),
          no_meta map(varchar, int),
          empty_tags varchar[],
          label varchar,
          happened_at timestamp)"])
      (jdbc/execute!
       con
       ["insert into t values
         (1,
          ['a', 'b'],
          {'name': 'alice', 'age': 30},
          MAP {'k': 1},
          MAP {1: 'x'},
          {'xs': [1, 2], 'm': MAP {'k': 7}},
          [{'name': 'bob', 'age': 40}, {'name': 'carol', 'age': 41}],
          null,
          null,
          null,
          [],
          'plain',
          TIMESTAMP '2024-01-02 03:04:05')"])
      (let [row (first (jdbc/execute! con ["select * from t"]))]
        (testing "next.jdbc default keys for DuckDB are unqualified"
          (is (= #{:id :tags :info :meta :int_meta :nested :people :no_tags
                   :no_info :no_meta :empty_tags :label :happened_at}
                 (set (keys row)))))
        (testing "top-level DuckDB LIST/STRUCT/MAP values"
          (is (= ["a" "b"] (:tags row)))
          (is (= {:name "alice" :age 30} (:info row)))
          (is (= {"k" 1} (:meta row)))
          (is (= {1 "x"} (:int_meta row))))
        (testing "nested DuckDB values are converted recursively"
          (is (= {:xs [1 2] :m {"k" 7}} (:nested row)))
          (is (= [{:name "bob" :age 40} {:name "carol" :age 41}]
                 (:people row))))
        (testing "NULL and empty values"
          (is (nil? (:no_tags row)))
          (is (nil? (:no_info row)))
          (is (nil? (:no_meta row)))
          (is (= [] (:empty_tags row))))
        (testing "scalars are unchanged by duckdb.types"
          (is (= 1 (:id row)))
          (is (= "plain" (:label row)))
          (is (inst? (:happened_at row))))))))

