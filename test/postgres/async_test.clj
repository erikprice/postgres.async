(ns postgres.async-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [<!! go]]
            [postgres.async :refer :all]))

(def ^:private ^:dynamic *db*)
(def table "clj_pg_test")

(defn- wait [channel]
  (let [r (<!! channel)]
    (if (instance? Throwable r)
      (throw r))
    r))

(defn- create-tables [db]
  (wait (<execute! db [(str "drop table if exists " table)]))
  (wait (<execute! db [(str "create table " table " (
                           id serial, t varchar(10))")])))

(defn- db-fixture [f]
  (letfn [(env [name default]
            (or (System/getenv name) default))]
    (binding [*db* (open-db {:hostname (env "PG_HOST" "localhost")
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

(deftest queries
  (testing "query returns rows as map"
    (let [rs (wait (<query! *db* ["select 1 as x"]))]
      (is (= 1 (get-in rs [0 :x]))))))

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
