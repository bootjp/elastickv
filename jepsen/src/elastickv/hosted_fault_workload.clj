(ns elastickv.hosted-fault-workload
  "Three managed Docker nodes, deterministic faults, and checked recovery reads."
  (:gen-class)
  (:require [clojure.string :as str]
            [elastickv.cli :as cli]
            [elastickv.redis-workload :as redis]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.nemesis.combined :as combined]
            [jepsen.os :as os]))

(def fault-ops
  {:partition [:start-partition :stop-partition]
   :kill [:kill :start]
   :pause [:pause :resume]})

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

(defn final-read-phase [nodes touched-keys]
  (apply gen/any
         (map-indexed
           (fn [i _node]
             (gen/on-threads
               #(and (integer? %) (= i (mod % (count nodes))))
               (gen/once
                 (fn [_test _ctx]
                   {:type :invoke :f :txn :final true
                    :value (mapv (fn [k] [:r k nil]) (sort @touched-keys))}))))
           nodes)))

(defn evidence-checker
  "Reject missing injections, failed healing, or unobserved recovery reads."
  [nodes fault key-count]
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
            missing-faults (for [node nodes f [start stop]
                                 :when (not-any? #(and (= f (:f %))
                                                       (= node (:target %))) events)]
                             [node f])
            finals (keep (fn [[i op]]
                           (when (and healed-at (> i healed-at)
                                      (:final op) (= :ok (:type op))) op))
                         indexed)
            observed (set (for [op finals [f k _v] (:value op) :when (= :r f)]
                            [(:node op) k]))
            keys (into (set (range key-count))
                       (for [op history
                             :when (and (= :invoke (:type op)) (= :txn (:f op))
                                        (not (:final op)))
                             [_f k _v] (:value op)] k))
            missing-reads (for [node nodes k (sort keys)
                                :when (not (observed [node k]))] [node k])
            writes (filter #(and (= :ok (:type %))
                                 (some (fn [[f]] (= :append f)) (:value %)))
                           (remove #(= :nemesis (:process %)) history))]
        {:valid? (boolean (and healed-at (empty? missing-faults)
                              (empty? missing-reads) (seq writes)))
         :missing-faults (vec missing-faults)
         :missing-final-reads (vec missing-reads)
         :recovery-completed? (some? healed-at)
         :successful-write-transactions (count writes)}))))

(defn hosted-fault-test [opts]
  (let [nodes (:nodes opts)
        fault (first (:faults opts))
        interval (:fault-interval opts)
        key-count (or (:key-count opts) 8)
        recovery-time (:recovery-time opts)
        minimum-time (* interval (inc (* 2 (count nodes))))]
    (when-not (and (= 3 (count (set nodes))) (= 3 (count nodes))
                   (= 1 (count (:faults opts))) (contains? fault-ops fault)
                   (not (:local opts)) (not (:host opts))
                   (not (seq (:raft-groups opts))) (not (:shard-ranges opts))
                   (pos? interval) (pos? recovery-time) (pos? key-count)
                   (pos? (:rate opts))
                   (pos? (:concurrency opts))
                   (zero? (mod (:concurrency opts) 3))
                   (> (:time-limit opts) minimum-time))
      (throw (ex-info "Require 3 managed nodes, one of partition/kill/pause, positive intervals and keys, concurrency divisible by 3, and time-limit > 7 * fault-interval"
                      {:options opts})))
    (let [members (str/join "," (map #(str % "=" % ":" (or (:grpc-port opts) 50051)) nodes))
          base (redis/elastickv-redis-test (assoc opts :key-count key-count
                                                     :bootstrap-members members))
          workload (redis/elastickv-append-workload (assoc opts :key-count key-count))
          touched-keys (atom (set (range key-count)))
          [_ stop] (fault-ops fault)]
      (assoc base
             :name (str "elastickv-hosted-" (name fault))
             ;; Images contain all dependencies. Debian OS setup manages system
             ;; services and /etc/hosts, which Docker owns on these nodes.
             :os os/noop
             ;; combined/nemesis-package initializes clock and file-corruption
             ;; nemeses even when their generators are disabled. Containers
             ;; must never initialize those host-level failure mechanisms.
             :nemesis (if (= :partition fault)
                        (combined/partition-nemesis (:db base))
                        (combined/db-nemesis (:db base)))
             :client (->ObservedClient (:client base) nil)
             :checker (checker/compose
                        {:consistency (:checker base)
                         :fault-and-recovery (evidence-checker nodes fault key-count)})
             :generator
             (gen/phases
               (->> (:generator workload)
                    ;; Elle rotates its active key window; key-count does not
                    ;; bound key IDs. Track emitted keys before dispatch so
                    ;; even indeterminate operations receive final reads.
                    (gen/map (fn [op]
                               (swap! touched-keys into (map second (:value op)))
                               op))
                    (gen/stagger (/ (double (:rate opts))))
                    (gen/nemesis (fault-cycle nodes fault interval))
                    (gen/time-limit (:time-limit opts)))
               ;; Keep recovery inside :generator: combined final generators
               ;; cannot be serialized as :final-generator in Jepsen 0.3.x.
               (gen/nemesis (gen/once {:type :info :f stop :value :all :recovery true}))
               (gen/sleep recovery-time)
               (gen/clients (final-read-phase nodes touched-keys)))))))

(def cli-opts
  (into (into cli/common-cli-opts redis/redis-cli-opts)
        [[nil "--recovery-time SECONDS" "Wait after healing before final reads."
          :default 10 :parse-fn #(Double/parseDouble %)]]))

(defn -main [& args]
  (cli/run-workload! args cli-opts
                     #(cli/parse-common-opts % nil)
                     hosted-fault-test))
