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

(deftest writes-clojure-data-as-duckdb-nested-types
  (with-connection
    (fn [con]
      (jdbc/execute!
       con
       ["create table write_values
         (tags varchar[],
          seq_tags varchar[],
          info struct(name varchar, age int),
          nil_info struct(name varchar, age int),
          meta map(varchar, int),
          int_meta map(int, varchar),
          people struct(name varchar, age int)[],
          nested struct(xs int[], m map(varchar, int)),
          quoted struct(\"name\" varchar, \"order\" int),
          empty_tags varchar[])"])
      (testing "missing struct fields fail before binding"
        (let [e (is (thrown-with-msg?
                     clojure.lang.ExceptionInfo
                     #"Missing STRUCT field"
                     (jdbc/execute!
                      con
                      ["insert into write_values(info) values (?)" {:age 1}])))]
          (is (= {:duckdb/error :missing-struct-field
                  :field "name"
                  :type "STRUCT(\"name\" VARCHAR, age INTEGER)"}
                 (ex-data e)))))
      (jdbc/execute!
       con
       ["insert into write_values values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        ["a" "b"]
        (list "c" "d")
        {:name "alice" :age 30}
        {:name nil :age 1}
        {"k" 1}
        {1 "x"}
        [{:name "a" :age 1} {:name "b" :age 2}]
        {:xs [1 2] :m {"k" 9}}
        {:name "reserved" :order 4}
        []])
      (let [row (first (jdbc/execute! con ["select * from write_values"]))]
        (testing "LIST values"
          (is (= ["a" "b"] (:tags row)))
          (is (= ["c" "d"] (:seq_tags row)))
          (is (= [] (:empty_tags row))))
        (testing "STRUCT values"
          (is (= {:name "alice" :age 30} (:info row)))
          (is (= {:name nil :age 1} (:nil_info row)))
          (is (= {:name "reserved" :order 4} (:quoted row))))
        (testing "MAP values"
          (is (= {"k" 1} (:meta row)))
          (is (= {1 "x"} (:int_meta row))))
        (testing "nested LIST/STRUCT/MAP values"
          (is (= [{:name "a" :age 1} {:name "b" :age 2}]
                 (:people row)))
          (is (= {:xs [1 2] :m {"k" 9}} (:nested row)))))
      (testing "untyped parameters fall back to raw next.jdbc binding"
        (is (= {:v "plain"} (first (jdbc/execute! con ["select ? as v" "plain"]))))))))
