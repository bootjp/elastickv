(ns elastickv.jepsen-test
  (:gen-class)
  (:require [jepsen
             [core :as jepsen]
             [cli :as cli]
             [db :as db]
             [client :as client]
             [checker :as checker]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.tests.linearizable-register :as register]
            [jepsen.nemesis :as nemesis])
  (:import (redis.clients.jedis Jedis)))

(defrecord RedisClient [port]
  client/Client
  (open! [this test node]
    (assoc this :conn (Jedis. (name node) port)))
  (close! [this test]
    (.close (:conn this))
    this)
  (setup! [this test])
  (teardown! [this test])
  (invoke! [this test op]
    (let [conn (:conn this)
          value (:value op)]
      (case (:f op)
        :write (do (.set conn "k" (pr-str value))
                   (assoc op :type :ok))
        :read (let [v (.get conn "k")]
                (assoc op :type :ok
                       :value (when v (read-string v))))
        (assoc op :type :fail :error :unknown-op)))))

(defn elastickv-test []
  (register/test
    {:name "elastickv-register"
     :nodes ["n1" "n2" "n3"]
     :db db/noop
     :client (->RedisClient 63791)
     :concurrency 5
     :nemesis (nemesis/partition-random-halves)}))

(defn -main
  [& args]
  (cli/run! (cli/single-test-cmd {:test-fn elastickv-test}) args))
