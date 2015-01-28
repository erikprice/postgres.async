(ns postgres.async-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async :refer [<!! >!! chan close! go put! thread timeout]]
            [postgres.async :refer :all]))

(def ^:private ^:dynamic *db*)
(def table "clj_pg_test")
(def table-2 "clj_pg_test_2")

(defn- wait [channel]
  (let [r (<!! channel)]
    (if (instance? Throwable r)
      (throw r))
    r))

(defn- create-tables [db]
  (wait (<execute! db [(str "drop table if exists " table)]))
  (wait (<execute! db [(str "drop table if exists " table-2)]))
  (wait (<execute! db [(str "create table " table " (
                           id serial, t varchar(10))")]))
  (wait (<execute! db [(str "create table " table-2 " (
                           id serial, t varchar(10))")])))

(defn- db-fixture [f]
  (letfn [(env [name default]
            (or (System/getenv name) default))]
    (binding [*db* (open-db {:hostname (env "PG_HOST" "local.zensight.co")
                             :port     (env "PG_PORT" 5432)
                             :database (env "PG_DB" "postgres")
                             :username (env "PG_USER" "postgres")
                             :password (env "PG_PASSWORD" "postgres")
                             :pool-size 1})]
      (try
        (create-tables *db*)
        (f)
        (finally (close-db! *db*))))))

(use-fixtures :each db-fixture)

(defn- make-worker
  [done-ch]
  (let [work-ch (chan)]
    (thread
      (loop [blockers []]
        (wait (timeout (-> 5 rand-int inc)))
        (if-let [[work expectation] (<!! work-ch)]
          (let [blocker (chan)]
            (work (fn [r e]
                    ; (println "worker: " work-ch ", r: " r ", e: " e)
                    (expectation r)
                    (close! blocker)))
            (recur (conj blockers blocker)))
          (do
            (wait (async/merge blockers))
            (put! done-ch :done)))))
    work-ch))

(deftest queries
  (testing "query returns rows as map"
    (let [rs (wait (<query! *db* ["select 1 as x"]))]
      (is (= 1 (get-in rs [0 :x])))))

  ;; `works` is a vector of 2-tuples. Each 2-tuple's first element is a fn that
  ;; performs a postgres.async database operation, and its second element is a
  ;; fn that is passed the result of that database operation and verifies that
  ;; the result meets some expectation.
  ;; 
  ;; A `worker` constructs an input channel and a thread that repeatedly loops
  ;; until that input channel is closed. The "work" in `works` are randomly
  ;; sent into the worker on this channel so that it can perform the "work"
  ;; (database operation and expectation verification).
  ;;
  ;; `done-ch` is a single channel used by each worker to indicate that it is
  ;; no longer waiting on any database operations, so that the test code can
  ;; safely exit.

  (testing "concurrent queries are not mixed up"
    (let [rs      (wait (<insert! *db*
                                  {:table table :returning "id"}
                                  [{:t "a"} {:t "b"}]))
          [a b]   (map :id (:rows rs))
          stmt    (str "select t from " table " where id = $1")
          done-ch (chan)
          works   [[#(insert! *db* {:table table :returning "t"} [{:t "value"}] %)
                    #(assert (= "value" (get-in % [:rows 0 :t])))]
                   [#(insert! *db* {:table table :returning "t"} [{:t "value-a"}] %)
                    #(assert (= "value-a" (get-in % [:rows 0 :t])))]
                   [#(insert! *db* {:table table-2 :returning "t"} [{:t "value-2"}] %)
                    #(assert (= "value-2" (get-in % [:rows 0 :t])))]
                   [#(insert! *db* {:table table-2 :returning "t"} [{:t "value-b"}] %)
                    #(assert (= "value-b" (get-in % [:rows 0 :t])))]
                   [#(query! *db* [stmt a] %)
                    #(assert (= "a" (get-in % [0 :t])))]
                   [#(query! *db* [stmt b] %)
                    #(assert (= "b" (get-in % [0 :t])))]]
          workers (repeatedly 5 #(make-worker done-ch))
          total-i 500]

      (loop [i 0]
        (let [worker (rand-nth workers)
              work   (rand-nth works)]
          (put! worker work))
        (if (< i total-i)
          (recur (inc i))
          (do
            (doseq [worker workers]
              (close! worker))
            (wait (async/into [] (async/take (count workers) done-ch)))
            (close! done-ch)))))))

(deftest inserts
  (testing "insert returns row count"
    (let [rs (wait (<insert! *db* {:table table} {:t "x"}))]
      (is (= 1 (:updated rs)))))
  (testing "insert with returning returns generated keys"
    (let [rs (wait (<insert! *db* {:table table :returning "id"} {:t "y"}))]
      (is (get-in rs [:rows 0 :id]))))
  (testing "multiple rows can be inserted"
    (let [rs (wait (<insert! *db*
                             {:table table :returning "id"}
                             [{:t "foo"} {:t "bar"}]))]
      (is (= 2 (:updated rs)))
      (is (= 2 (count (:rows rs)))))))

(deftest updates
  (testing "update returns row count"
    (let [rs (wait (<insert! *db*
                             {:table table :returning "id"}
                             [{:t "update0"} {:t "update1"}]))
          rs (wait (<update! *db*
                             {:table table :where ["id in ($1, $2)"
                                                   (get-in rs [:rows 0 :id])
                                                   (get-in rs [:rows 1 :id])]}
                             {:t "update2"}))]
      (is (= 2 (:updated rs))))))

(deftest sql-macro
  (testing "dosql returns last form"
    (is (= "123" (wait (go (dosql
                            [tx (<begin! *db*)
                             rs (<query! tx ["select 123 as x"])
                             rs (<query! tx ["select $1::text as t" (:x (first rs))])
                             _  (<commit! tx)]
                            (:t (first rs))))))))
  (testing "dosql short-circuits on errors"
    (let [e (Exception. "Oops!")
          executed (atom 0)]
      (is (= (try
               (wait (go (dosql
                          [_ (<query! *db* ["select 123 as t"])
                           _ (go e)
                           _ (swap! executed inc)]
                          "Just hanging out")))
               (catch Exception caught
                 {:caught caught}))
             {:caught e}))
      (is (= @executed 0)))))
