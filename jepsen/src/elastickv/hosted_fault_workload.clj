(ns elastickv.hosted-fault-workload
  "Three managed Docker nodes, deterministic faults, and checked recovery reads."
  (:gen-class)
  (:require [clojure.string :as str]
            [elastickv.cli :as cli]
            [elastickv.db :as db]
            [elastickv.dynamodb-workload :as dynamodb]
            [elastickv.redis-workload :as redis]
            [elastickv.s3-workload :as s3]
            [elastickv.sqs-workload :as sqs]
            [elastickv.sqs-htfifo-workload :as fifo]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.independent :as independent]
            [jepsen.nemesis.combined :as combined]
            [jepsen.os :as os]))

(def fault-ops
  {:partition [:start-partition :stop-partition]
   :kill [:kill :start]
   :pause [:pause :resume]})

(def adapters
  {:redis {:workload redis/elastickv-append-workload :port 6379}
   :dynamodb {:workload dynamodb/dynamodb-append-workload :port 8000 :db-port :dynamo-port}
   :s3 {:workload s3/s3-register-workload :port 9000 :db-port :s3-port}
   :sqs-standard {:workload sqs/sqs-workload :port 9324 :db-port :sqs-port}
   :sqs-htfifo {:workload fifo/sqs-htfifo-workload :port 9324 :db-port :sqs-port}})

(defn queue-adapter? [adapter] (contains? #{:sqs-standard :sqs-htfifo} adapter))

(defn operation-keys [adapter op]
  (case adapter
    (:redis :dynamodb) (when (= :txn (:f op)) (map second (:value op)))
    :s3 (when (#{:read :write} (:f op)) [(first (:value op))])
    []))

(defn recovery-keys [adapter op]
  (case adapter
    (:sqs-standard :sqs-htfifo) (when (= :recv (:f op)) [:queue])
    :s3 (when (= :read (:f op)) [(first (:value op))])
    (when (= :txn (:f op)) (map second (filter #(= :r (first %)) (:value op))))))

(defrecord ObservedClient [delegate node]
  client/Client
  (open! [this test node]
    (assoc this :node node :delegate (client/open! delegate test node)))
  (setup! [_ test] (client/setup! delegate test))
  (invoke! [_ test op]
    (assoc (client/invoke! delegate test op) :node node))
  (teardown! [_ test] (client/teardown! delegate test))
  (close! [_ test] (client/close! delegate test)))

(defn fault-cycle
  "Isolate or stop one node at a time, retaining the two-node quorum."
  [nodes fault interval]
  (let [[start stop] (fault-ops fault)]
    (cons (gen/sleep interval)
          (mapcat (fn [node]
                    [{:type :info :f start :target node
                      :value (if (= :partition fault)
                               (into {} (for [peer nodes]
                                          [peer (if (= peer node)
                                                  (vec (remove #{node} nodes))
                                                  [node])]))
                               [node])}
                     (gen/sleep interval)
                     {:type :info :f stop :target node :value :all}
                     (gen/sleep interval)])
                  nodes))))

(defn final-read-phase
  ([nodes touched-keys] (final-read-phase nodes touched-keys :redis))
  ([nodes touched-keys adapter]
  (apply gen/any
         (map-indexed
           (fn [i _node]
             (gen/on-threads
               #(and (integer? %) (= i (mod % (count nodes))))
               (delay
                 (if (= :redis adapter)
                   [{:type :invoke :f :txn :final true
                     :value (mapv (fn [k] [:r k nil]) (sort @touched-keys))}]
                   ;; One-key DynamoDB reads avoid TransactGetItems' 100-key
                   ;; limit after Elle has rotated through many key IDs.
                   (mapv (fn [k]
                           (if (= :s3 adapter)
                             {:f :read :final true :value (independent/tuple k nil)}
                             {:f :txn :final true :value [[:r k nil]]}))
                         (sort @touched-keys))))))
           nodes))))

(defn fault-schedule [nodes fault interval repeat?]
  (let [round (fault-cycle nodes fault interval)]
    (if repeat? (cycle round) round)))

(defn evidence-checker
  "Reject missing injections, failed healing, or unobserved recovery reads."
  ([nodes fault key-count] (evidence-checker nodes fault key-count 1))
  ([nodes fault key-count min-cycles] (evidence-checker nodes fault key-count min-cycles :redis))
  ([nodes fault key-count min-cycles adapter]
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [[start stop] (fault-ops fault)
            indexed (map-indexed vector history)
            ;; The serial nemesis worker records both invocation and response
            ;; as :info. Only a matching, successful response proves completion.
            completed (keep (fn [pair]
                              (when (= 2 (count pair))
                                (let [[[_ invoked] [i response]] pair]
                                  (when (and (= (select-keys invoked [:f :target :recovery])
                                                (select-keys response [:f :target :recovery]))
                                             (= :info (:type response))
                                             (not (:error response)))
                                    [i response]))))
                            (partition-all 2 (filter #(= :nemesis (:process (second %))) indexed)))
            healed-at (some (fn [[i op]]
                              (when (and (:recovery op) (= stop (:f op))
                                         (= :nemesis (:process op))
                                         (= :info (:type op)) (not (:error op))) i))
                            completed)
            events (keep (fn [[i op]]
                           (when (and healed-at (< i healed-at)
                                      (= :nemesis (:process op))
                                      (= :info (:type op)) (not (:error op))) op))
                         completed)
            counts (frequencies (map (juxt :target :f) events))
            cycles (into {} (for [node nodes]
                              [node (min (get counts [node start] 0)
                                         (get counts [node stop] 0))]))
            missing-faults (for [node nodes f [start stop]
                                 :when (< (get counts [node f] 0) min-cycles)]
                             [node f])
            finals (keep (fn [[i op]]
                           (when (and healed-at (> i healed-at)
                                      (:final op) (= :ok (:type op))) op))
                         indexed)
            observed (set (for [op finals
                                k (recovery-keys adapter op)]
                            [(:node op) k]))
            keys (if (queue-adapter? adapter)
                   #{:queue}
                   (into (set (range key-count))
                         (mapcat #(operation-keys adapter %)
                                 (filter #(and (= :invoke (:type %)) (not (:final %))
                                               (not= :nemesis (:process %))) history))))
            missing-reads (for [node nodes k (sort keys)
                                :when (not (observed [node k]))] [node k])
            writes (filter #(and (= :ok (:type %))
                                 (case adapter
                                   (:sqs-standard :sqs-htfifo) (= :send (:f %))
                                   :s3 (= :write (:f %))
                                   (and (= :txn (:f %))
                                        (some (fn [[f]] (= :append f)) (:value %)))))
                           (remove #(= :nemesis (:process %)) history))]
        {:valid? (boolean (and healed-at (empty? missing-faults)
                              (empty? missing-reads) (seq writes)))
         :missing-faults (vec missing-faults)
         :missing-final-reads (vec missing-reads)
         :recovery-completed? (some? healed-at)
         :completed-fault-cycles cycles
         :minimum-fault-cycles min-cycles
         :adapter adapter
         :successful-writes (count writes)})))))

(defn hosted-fault-test [opts]
  (let [nodes (:nodes opts)
        adapter (or (:adapter opts) :redis)
        fault (first (:faults opts))
        interval (:fault-interval opts)
        key-count (or (:key-count opts) 8)
        recovery-time (:recovery-time opts)
        min-cycles (or (:min-fault-cycles opts) 1)
        minimum-time (* interval (inc (* 2 (count nodes) min-cycles)))]
    (when-not (and (contains? adapters adapter)
                   (= 3 (count (set nodes))) (= 3 (count nodes))
                   (= 1 (count (:faults opts))) (contains? fault-ops fault)
                   (not (:local opts)) (not (:host opts))
                   (not (seq (:raft-groups opts))) (not (:shard-ranges opts))
                   (pos? interval) (pos? recovery-time) (pos? key-count)
                   (pos? (:rate opts))
                   (pos-int? min-cycles)
                   (or (:repeat-faults opts) (= 1 min-cycles))
                   (pos? (:concurrency opts))
                   (zero? (mod (:concurrency opts) 3))
                   (or (not= :s3 adapter) (even? (:concurrency opts)))
                   (or (not (queue-adapter? adapter)) (>= (or (:drain-time opts) 40) 40))
                   (> (:time-limit opts) minimum-time))
      (throw (ex-info "Require 3 managed nodes, one of partition/kill/pause, positive intervals and keys, concurrency divisible by 3, repeat-faults for multiple cycles, and time-limit > (1 + 6 * min-fault-cycles) * fault-interval"
                      {:options opts})))
    (let [{:keys [workload port db-port]} (adapters adapter)
          members (str/join "," (map #(str % "=" % ":" (or (:grpc-port opts) 50051)) nodes))
          base (redis/elastickv-redis-test (assoc opts :key-count key-count
                                                     :bootstrap-members members))
          managed-db (db/db (cond-> {:grpc-port (or (:grpc-port opts) 50051)
                                    :redis-port 6379 :bootstrap-members members
                                    :encryption (:encryption opts)
                                    :sqs-region "us-east-1"}
                             db-port (assoc db-port port)))
          workload (workload (assoc opts :key-count key-count :sqs-region "us-east-1"
                                         :node->port (zipmap nodes (repeat port))))
          touched-keys (atom (set (range key-count)))
          [_ stop] (fault-ops fault)]
      (assoc (merge base workload)
             :name (str "elastickv-hosted-" (name adapter) "-" (name fault))
             :final-generator nil
             :concurrency (:concurrency opts)
             :db managed-db
             ;; Images contain all dependencies. Debian OS setup manages system
             ;; services and /etc/hosts, which Docker owns on these nodes.
             :os os/noop
             ;; combined/nemesis-package initializes clock and file-corruption
             ;; nemeses even when their generators are disabled. Containers
             ;; must never initialize those host-level failure mechanisms.
             :nemesis (if (= :partition fault)
                        (combined/partition-nemesis managed-db)
                        (combined/db-nemesis managed-db))
             :client (->ObservedClient (:client workload) nil)
             :checker (checker/compose
                        {:consistency (:checker workload)
                         :fault-and-recovery (evidence-checker nodes fault key-count min-cycles adapter)})
             :generator
             (gen/phases
               (->> (:generator workload)
                    ;; Elle rotates its active key window; key-count does not
                    ;; bound key IDs. Track emitted keys before dispatch so
                    ;; even indeterminate operations receive final reads.
                    (gen/map (fn [op]
                               (swap! touched-keys into (operation-keys adapter op))
                               op))
                    (gen/stagger (/ (double (:rate opts))))
                    (gen/nemesis (fault-schedule nodes fault interval (:repeat-faults opts)))
                    (gen/time-limit (:time-limit opts)))
               ;; Keep recovery inside :generator: combined final generators
               ;; cannot be serialized as :final-generator in Jepsen 0.3.x.
               (gen/nemesis (gen/once {:type :info :f stop :value :all :recovery true}))
               (gen/sleep recovery-time)
               (gen/clients
                 (if (queue-adapter? adapter)
                   (->> (gen/repeat {:f :recv :final true})
                        (gen/stagger (/ (double (:rate opts))))
                        (gen/time-limit (or (:drain-time opts) 40)))
                   (final-read-phase nodes touched-keys adapter))))))))

(def cli-opts
  (into (into cli/common-cli-opts redis/redis-cli-opts)
        [[nil "--adapter NAME" "redis, dynamodb, s3, sqs-standard, or sqs-htfifo."
          :default :redis :parse-fn keyword]
         [nil "--drain-time SECONDS" "SQS receive/delete-only recovery phase."
          :default 40 :parse-fn #(Integer/parseInt %)]
         [nil "--repeat-faults" "Repeat fault cycles throughout the main phase."
          :default false]
         [nil "--min-fault-cycles N" "Minimum completed fault/heal cycles per node."
          :default 1 :parse-fn #(Integer/parseInt %)]
         [nil "--recovery-time SECONDS" "Wait after healing before final reads."
          :default 10 :parse-fn #(Double/parseDouble %)]]))

(defn -main [& args]
  (cli/run-workload! args cli-opts
                     #(cli/parse-common-opts % nil)
                     hosted-fault-test))
