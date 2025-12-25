(ns elastickv.redis-workload
  (:gen-class)
  (:require [clojure.string :as str]
            [clojure.tools.cli :as tools.cli]
            [jepsen [client :as client]
                    [core :as jepsen]
                    [db :as db]
                    [generator :as gen]]
            [jepsen.os :as os]
            [jepsen.control.core :as control]
            [jepsen.nemesis :as nemesis]
            [jepsen.redis.client :as rc]
            [jepsen.redis.append :as redis-append]
            [jepsen.tests.cycle.append :as append]))

(def default-node->port
  {:n1 63791
   :n2 63792
   :n3 63793})

(defn parse-ports
  "Turns a comma separated ports string into a vector of ints."
  [s]
  (->> (str/split s #",")
       (remove str/blank?)
       (map #(Integer/parseInt %))
       vec))

(defrecord DummyRemote []
  control/Remote
  (connect [this conn-spec] this)
  (disconnect! [this])
  (execute! [this ctx action]
    (assoc action :exit 0 :out "" :err ""))
  (upload! [this ctx local-paths remote-path opts] {:exit 0})
  (download! [this ctx remote-paths local-path opts] {:exit 0}))

(defrecord NoopNemesis []
  nemesis/Nemesis
  (setup! [this test] this)
  (invoke! [this test op] (assoc op :type :info, :value :noop))
  (teardown! [this test] this))

(defrecord ElastickvRedisClient [host node->port conn]
  client/Client
  (open! [this test node]
    (let [p (get node->port node 6379)
          h (or (:redis-host test) host)
          c (rc/open h {:port p :timeout-ms 10000})]
      (assoc this :conn c)))

  (close! [this test]
    (when-let [c (:conn this)]
      (rc/close! c))
    this)

  (setup! [this test])
  (teardown! [this test])

  (invoke! [this test op]
    (let [conn (:conn this)]
      (rc/with-exceptions op #{}
        (rc/with-conn conn
          (->> (if (< 1 (count (:value op)))
                 (->> (:value op)
                      (mapv (partial redis-append/apply-mop! conn))
                      (rc/with-txn conn)
                      (mapv (fn [[f k v] r]
                              [f k (if (= f :r) r v)])
                            (:value op)))
                 (->> (:value op)
                      (mapv (partial redis-append/apply-mop! conn))))
               (mapv (partial redis-append/parse-read conn))
               (assoc op :type :ok, :value)))))))

(defn elastickv-append-workload
  "Append workload that reuses jepsen-io/redis logic but targets elastickv."
  [opts]
  (let [workload (append/test {:key-count (or (:key-count opts) 12)
                               :min-txn-length 1
                               :max-txn-length (or (:max-txn-length opts) 4)
                               :max-writes-per-key (or (:max-writes-per-key opts) 128)
                               :consistency-models [:strict-serializable]})
        client (->ElastickvRedisClient (or (:redis-host opts) "127.0.0.1")
                                       (or (:node->port opts) default-node->port)
                                       nil)]
    (assoc workload :client client)))

(defn ports->node-map
  [ports]
  (into {}
        (map-indexed (fn [idx port]
                       [(keyword (str "n" (inc idx))) port])
                     ports)))

(defn elastickv-redis-test
  "Builds a Jepsen test map that drives elastickv's Redis protocol with the
   jepsen-io/redis append workload."
  ([] (elastickv-redis-test {}))
  ([opts]
   (let [ports      (or (:redis-ports opts) (vals default-node->port))
         node->port (or (:node->port opts) (ports->node-map ports))
         nodes      (vec (keys node->port))
         rate       (double (or (:rate opts) 5))
         time-limit (or (:time-limit opts) 30)
         workload   (elastickv-append-workload (assoc opts :node->port node->port))]
     (merge workload
            {:name         (or (:name opts) "elastickv-redis-append")
             :nodes        nodes
             :db           db/noop
             :os           os/noop
             :ssh          {:dummy? true}
             :remote       (->DummyRemote)
             :nemesis      (->NoopNemesis)
             :concurrency  (or (:concurrency opts) 5)
             :generator    (->> (:generator workload)
                                (gen/stagger (/ rate))
                                (gen/time-limit time-limit))}))))

(def cli-opts
  [[nil "--ports PORTS" "Comma separated Redis ports (leader first)."
    :default "63791,63792,63793"]
   [nil "--host HOST" "Hostname elastickv is listening on."
    :default "127.0.0.1"]
   [nil "--time-limit SECONDS" "How long to run the workload."
    :default 30
    :parse-fn #(Integer/parseInt %)]
   [nil "--rate HZ" "Approx ops/sec per worker."
    :default 5
    :parse-fn #(Double/parseDouble %)]
   [nil "--concurrency N" "Number of worker threads."
    :default 5
    :parse-fn #(Integer/parseInt %)]
   ["-h" "--help"]])

(defn -main
  [& args]
  (let [{:keys [options errors summary]} (tools.cli/parse-opts args cli-opts)
        parsed  (update options :ports parse-ports)
        options (assoc parsed
                  :redis-ports (:ports parsed)
                  :redis-host  (:host parsed))]
    (cond
      (:help options) (println summary)
      (seq errors) (binding [*out* *err*]
                     (println "Error parsing options:" (str/join "; " errors)))
      :else (jepsen/run! (elastickv-redis-test options)))))
