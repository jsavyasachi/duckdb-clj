(ns parameter-metadata-probe
  (:require [next.jdbc :as jdbc]))

(set! *warn-on-reflection* true)

(defn -main []
  (let [ds (jdbc/get-datasource "jdbc:duckdb:")]
    (with-open [con (jdbc/get-connection ds)]
      (jdbc/execute!
       con
       ["create table t
         (tags varchar[],
          info struct(name varchar, age int),
          meta map(varchar, int))"])
      (try
        (with-open [ps (.prepareStatement
                        ^java.sql.Connection con
                        "insert into t values (?, ?, ?)")]
          (let [pmd (.getParameterMetaData ps)]
            (doseq [i (range 1 (inc (.getParameterCount pmd)))]
              (println i (.getParameterTypeName pmd i)))))
        (catch Throwable t
          (println (.getName (class t)))
          (println (.getMessage t)))))))

