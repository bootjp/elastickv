(ns elastickv.redis-workload
  (:gen-class)
  (:require [clojure.string :as str]
            [elastickv.cli :as cli]
            [elastickv.db :as ekdb]
            [jepsen.db :as jdb]
            [jepsen [client :as client]
                    [generator :as gen]
                    [net :as net]]
            [jepsen.control :as control]
            [jepsen.os :as os]
            [jepsen.nemesis :as nemesis]
            [jepsen.nemesis.combined :as combined]
            [jepsen.os.debian :as debian]
            [jepsen.redis.client :as rc]
            [jepsen.redis.append :as redis-append]
            [jepsen.tests.cycle.append :as append]))

(def default-nodes ["n1" "n2" "n3" "n4" "n5"])

(defrecord ElastickvRedisClient [node->port conn]
  client/Client
  (open! [this test node]
    (let [p (get node->port node 6379)
          h (or (:redis-host test) (name node))
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
        client (->ElastickvRedisClient (or (:node->port opts)
                                           (zipmap default-nodes (repeat 6379)))
                                       nil)]
    (assoc workload :client client)))

(defn elastickv-redis-test
  "Builds a Jepsen test map that drives elastickv's Redis protocol with the
   jepsen-io/redis append workload."
  ([] (elastickv-redis-test {}))
  ([opts]
   (let [nodes      (or (:nodes opts) default-nodes)
         redis-ports (or (:redis-ports opts) (repeat (count nodes) (or (:redis-port opts) 6379)))
         node->port (or (:node->port opts) (cli/ports->node-map redis-ports nodes))
         local?     (:local opts)
         db         (if local?
                      jdb/noop
                      (ekdb/db {:grpc-port (or (:grpc-port opts) 50051)
                                :redis-port node->port
                                :raft-groups (:raft-groups opts)
                                :shard-ranges (:shard-ranges opts)}))
         rate       (double (or (:rate opts) 5))
         time-limit (or (:time-limit opts) 30)
         faults     (if local?
                      []
                      (cli/normalize-faults (or (:faults opts) [:partition :kill])))
         nemesis-p  (when-not local?
                      (combined/nemesis-package {:db db
                                                 :faults faults
                                                 :interval (or (:fault-interval opts) 40)}))
         nemesis-gen (if nemesis-p
                       (:generator nemesis-p)
                       (gen/once {:type :info :f :noop}))
         workload   (elastickv-append-workload (assoc opts :node->port node->port))]
     (merge workload
            {:name            (or (:name opts) "elastickv-redis-append")
             :nodes           nodes
             :db              db
             :redis-host      (:redis-host opts)
             :os              (if local? os/noop debian/os)
             :net             (if local? net/noop net/iptables)
             :ssh             (merge {:username "vagrant"
                                      :private-key-path "/home/vagrant/.ssh/id_rsa"
                                      :strict-host-key-checking false}
                                     (when local? {:dummy true})
                                     (:ssh opts))
             :remote          control/ssh
             :nemesis         (if nemesis-p
                                (:nemesis nemesis-p)
                                nemesis/noop)
             ; Jepsen 0.3.x can't fressian-serialize some combined final gens; skip.
             :final-generator nil
             :concurrency     (or (:concurrency opts) 5)
             :generator       (->> (:generator workload)
                                   (gen/nemesis nemesis-gen)
                                   (gen/stagger (/ rate))
                                   (gen/time-limit time-limit))}))))

;; ---------------------------------------------------------------------------
;; CLI
;; ---------------------------------------------------------------------------

(def redis-cli-opts
  "Redis-specific CLI options, appended to common opts."
  [[nil "--ports PORTS" "Comma separated Redis ports (per node)."
    :default nil
    :parse-fn (fn [s]
                (->> (str/split s #",")
                     (remove str/blank?)
                     (mapv #(Integer/parseInt %))))]
   [nil "--redis-port PORT" "Redis port (applied to all nodes)."
    :default 6379
    :parse-fn #(Integer/parseInt %)]])

(defn- prepare-redis-opts
  "Transform parsed CLI options into the map expected by elastickv-redis-test."
  [options]
  (let [ports (:ports options)
        options (cli/parse-common-opts options ports)]
    (assoc options
      :redis-host (:host options)
      :redis-ports ports
      :redis-port (:redis-port options))))

(defn -main
  [& args]
  (cli/run-workload! args
                     (into cli/common-cli-opts redis-cli-opts)
                     prepare-redis-opts
                     elastickv-redis-test))
