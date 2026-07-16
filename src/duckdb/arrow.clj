(ns duckdb.arrow
  "Optional Apache Arrow C Data interface helpers for DuckDB JDBC."
  (:require [next.jdbc :as jdbc])
  (:import (java.sql Connection ResultSet)
           (java.util.concurrent.atomic AtomicBoolean)
           (org.apache.arrow.c ArrowArrayStream Data)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector.ipc ArrowReader)
           (org.duckdb DuckDBConnection DuckDBResultSet)))

(set! *warn-on-reflection* true)

(def ^:private default-batch-size 2048)
(def ^:private identifier-pattern #"^[A-Za-z_][A-Za-z0-9_]*$")

(defn- sql-params [query]
  (if (string? query) [query] query))

(defn- validated-batch-size [value]
  (when-not (and (integer? value) (pos? value))
    (throw (ex-info (str "Arrow batch size must be a positive integer: " value)
                    {:duckdb/error :invalid-arrow-batch-size
                     :batch-size value})))
  (long value))

(defn- table-name [value]
  (let [value (if (or (keyword? value) (symbol? value))
                (name value)
                (str value))]
    (when-not (re-matches identifier-pattern value)
      (throw (ex-info (str "Invalid DuckDB Arrow table name: " value)
                      {:duckdb/error :invalid-alias
                       :table value})))
    value))

(defn- duckdb-connection [^Connection con]
  (.unwrap con DuckDBConnection))

(defn- close-once-reader
  ^ArrowReader [^ArrowReader reader ^BufferAllocator allocator]
  (let [closed? (AtomicBoolean. false)]
    (proxy [ArrowReader] [allocator]
      (getVectorSchemaRoot []
        (.getVectorSchemaRoot reader))
      (getDictionaryVectors []
        (.getDictionaryVectors reader))
      (lookup [id]
        (.lookup reader (long id)))
      (getDictionaryIds []
        (.getDictionaryIds reader))
      (loadNextBatch []
        (.loadNextBatch reader))
      (bytesRead []
        (.bytesRead reader))
      (readSchema []
        (.getSchema (.getVectorSchemaRoot reader)))
      (closeReadSource [] nil)
      (close
        ([]
         (when (.compareAndSet closed? false true)
           (.close reader)))
        ([close-source?]
         (when (.compareAndSet closed? false true)
           (.close reader (boolean close-source?))))))))

(defn arrow-export
  "Runs query and returns a closeable ArrowReader backed by allocator.

  query is a next.jdbc SQL/parameter vector or a SQL string. JDBC statement and
  result-set resources are closed before this function returns. The caller owns
  both the returned reader and allocator and must close the reader first."
  (^ArrowReader [^Connection con query ^BufferAllocator allocator]
   (arrow-export con query allocator default-batch-size))
  (^ArrowReader [^Connection con query ^BufferAllocator allocator batch-size]
   (with-open [stmt (jdbc/prepare con (sql-params query))
               ^ResultSet rs (.executeQuery stmt)]
     (let [^DuckDBResultSet duck-rs (.unwrap rs DuckDBResultSet)]
       (close-once-reader
        ^ArrowReader (.arrowExportStream duck-rs allocator
                                         (validated-batch-size batch-size))
        allocator)))))

(defn with-arrow-reader
  "Calls f with an ArrowReader for query and always closes the reader afterward.

  The caller owns allocator and must keep it open for the duration of f."
  ([^Connection con query ^BufferAllocator allocator f]
   (with-arrow-reader con query allocator default-batch-size f))
  ([^Connection con query ^BufferAllocator allocator batch-size f]
   (with-open [reader (arrow-export con query allocator batch-size)]
     (f reader))))

(defn register-arrow!
  "Registers an Arrow C Data stream as table-name and returns its lifetime handle.

  Keep the returned ArrowArrayStream open, along with its allocator and source
  ArrowReader, until DuckDB has finished the query that consumes the replacement
  scan. Closing any of them early invalidates the zero-copy C Data buffers.

  The four-argument form creates and exports an ArrowArrayStream from reader. It
  closes the stream if export or registration fails. The three-argument form
  registers an existing stream whose lifetime remains the caller's responsibility."
  (^ArrowArrayStream [^Connection con table ^ArrowArrayStream stream]
   (.registerArrowStream ^DuckDBConnection (duckdb-connection con)
                         (table-name table)
                         stream)
   stream)
  (^ArrowArrayStream [^Connection con table ^BufferAllocator allocator
                      ^ArrowReader reader]
   (let [stream (ArrowArrayStream/allocateNew allocator)]
     (try
       (Data/exportArrayStream allocator reader stream)
       (register-arrow! con table stream)
       (catch Throwable t
         (.close stream)
         (throw t))))))
