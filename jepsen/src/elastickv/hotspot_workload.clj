(ns elastickv.hotspot-workload
  "Bounded M4 automatic hotspot split test over linearizable Redis registers."
  (:gen-class)
  (:require [clojure.java.shell :as shell]
            [clojure.string :as str]
            [clojure.tools.logging :refer [warn]]
            [elastickv.cli :as cli]
            [elastickv.composed1-nemesis :as routes]
            [elastickv.db :as ekdb]
            [elastickv.split-workload :as split]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [independent :as independent]
                    [net :as net]]
            [jepsen.control :as control]
            [jepsen.db :as jdb]
            [jepsen.nemesis :as nemesis]
            [jepsen.nemesis.combined :as combined]
            [jepsen.os :as os]
            [jepsen.os.debian :as debian]))

(def register-keys ["a-hot" "m-hot" "z-hot"])

(def fault-ops
  {:partition [:start-partition :stop-partition]
   :kill [:kill :start]})

(def auto-split-args
  ["--autoSplit" "--keyvizStep=5s" "--autoSplitEvalInterval=1s"
   "--autoSplitCooldown=5s" "--autoSplitThreshold=60"
   "--autoSplitCandidateWindows=3" "--autoSplitMaxRoutes=2"
   "--autoSplitMaxPerCycle=1" "--autoSplitDefaultBuckets=16"])

(defn- grpc-address [opts test]
  (or (:grpc-host-port opts)
      (str (or (:grpc-host opts) (name (first (:nodes test)))) ":"
           (or (:grpc-port opts) 50051))))

(defn route-snapshot! [opts test]
  (let [bin (or (:list-routes-bin opts)
                (str (System/getProperty "user.dir")
                     "/target/elastickv-jepsen/elastickv-list-routes"))
        result (shell/sh bin "--address" (grpc-address opts test))]
    (when-not (zero? (:exit result))
      (throw (ex-info "list-routes failed" result)))
    (routes/parse-routes-json (:out result))))

(defn- active-routes [snapshot]
  (filter #(= "ROUTE_STATE_ACTIVE" (:state %)) (:routes snapshot)))

(defn split-observation [before after]
  (let [old (vec (active-routes before))
        new (vec (active-routes after))
        parent (first old)
        children (when parent
                   (filter #(and (= (:raft-group-id parent) (:raft-group-id %))
                                 (not (neg? (compare (:start %) (:start parent))))
                                 (or (nil? (:end parent))
                                     (and (:end %) (not (pos? (compare (:end %) (:end parent))))))) new))
        ordered (sort-by :start children)]
    (when (and (= 1 (count old)) (= 2 (count new)) (= 2 (count ordered))
               (> (:catalog-version after) (:catalog-version before))
               (= (:start parent) (:start (first ordered)))
               (neg? (compare (:start (first ordered)) (:end (first ordered))))
               (= (:end (first ordered)) (:start (second ordered)))
               (or (nil? (:end (second ordered)))
                   (neg? (compare (:start (second ordered)) (:end (second ordered)))))
               (= (:end parent) (:end (second ordered)))
               (every? #(= 1 (count (filter (fn [r] (routes/route-covers? r %)) new)))
                       register-keys)
               (every? #(some (fn [key] (routes/route-covers? % key)) register-keys)
                       ordered))
      {:before before :after after :split-key (:end (first ordered))})))

(defn controller-nemesis [opts]
  (let [baseline (atom nil)
        deadline (atom nil)
        observed (atom nil)]
    (reify
      nemesis/Reflection
      (fs [_] #{:capture-hotspot-baseline :observe-hotspot-split})
      nemesis/Nemesis
      (setup! [this _] this)
      (invoke! [_ test op]
        (try
          (case (:f op)
            :capture-hotspot-baseline
            (let [snapshot (route-snapshot! opts test)]
              (when-not (= 1 (count (active-routes snapshot)))
                (throw (ex-info "M4 requires one initial active route" {:snapshot snapshot})))
              (reset! baseline snapshot)
              (reset! deadline (+ (System/currentTimeMillis)
                                  (or (:observe-timeout-ms opts) 90000)))
              (assoc op :type :info :value snapshot))
            :observe-hotspot-split
            (do
              (when-not @baseline
                (throw (ex-info "missing hotspot baseline" {})))
              (if @observed
                (assoc op :type :info :value @observed)
                (let [after (try
                              (route-snapshot! opts test)
                              (catch Throwable t
                                (warn t "M4 route observation temporarily unavailable")
                                nil))
                      observation (when after (split-observation @baseline after))]
                  (cond
                    observation (do (reset! observed observation)
                                    (assoc op :type :info :value observation))
                    (nil? after)
                    (if (>= (System/currentTimeMillis) @deadline)
                      (throw (ex-info "automatic hotspot split not observed before deadline" {}))
                      (assoc op :type :info))
                    (>= (System/currentTimeMillis) @deadline)
                    (throw (ex-info "automatic hotspot split has not reached valid route geometry before deadline"
                                    {:before @baseline :after after}))
                    :else (assoc op :type :info)))))
            op)
          (catch Throwable t
            (warn t "M4 controller observation failed")
            (assoc op :type :info :error (str (or (.getMessage t) (str t))
                                              (when-let [after (:after (ex-data t))]
                                                (str ": " (pr-str after))))))))
      (teardown! [this _] this))))

(defn controller-package [opts]
  {:generator (gen/phases
                (gen/once {:type :info :f :capture-hotspot-baseline})
                (gen/sleep (or (:observe-after-seconds opts) 15))
                (gen/stagger 5 (gen/repeat {:type :info :f :observe-hotspot-split})))
   :final-generator nil
   :nemesis (controller-nemesis opts)
   :perf #{{:name "automatic-hotspot-split"
            :fs #{:capture-hotspot-baseline :observe-hotspot-split}
            :start #{:capture-hotspot-baseline}
            :stop #{:observe-hotspot-split}
            :color "#4A90E2"}}})

(defn controller-observation-generator [opts]
  (gen/phases
    (gen/sleep (or (:observe-after-seconds opts) 15))
    (gen/stagger 5 (gen/repeat {:type :info :f :observe-hotspot-split}))))

(defn controller-checker []
  (reify checker/Checker
    (check [_ _ history _]
      (let [ops (filter #(and (= :nemesis (:process %))
                              (contains? #{:capture-hotspot-baseline :observe-hotspot-split} (:f %))
                              (contains? #{:info :ok :fail} (:type %))) history)
            captures (vec (filter #(and (= :capture-hotspot-baseline (:f %))
                                        (map? (:value %))) ops))
            observations (filter #(and (= :observe-hotspot-split (:f %))
                                       (map? (:value %))) ops)
            capture (first captures)
            observe (first observations)]
        {:valid? (and (= 1 (count captures))
                      (some? observe)
                      (not-any? #(or (:error %) (= :fail (:type %))) ops)
                      (< (:index capture) (:index observe))
                      (= (:value capture) (get-in observe [:value :before]))
                      (some? (split-observation (:value capture)
                                                (get-in observe [:value :after]))))
         :completions (vec ops)}))))

(defn fault-cycle [nodes fault interval]
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

(defn deterministic-fault-package [db nodes fault interval]
  (let [[_ stop] (fault-ops fault)]
    {:generator (fault-cycle nodes fault interval)
     :final-generator (gen/once {:type :info :f stop :value :all :recovery true})
     :nemesis (if (= :partition fault)
                (combined/partition-nemesis db)
                (combined/db-nemesis db))
     :perf #{}}))

(defn fault-cycle-checker [nodes fault]
  (let [[start stop] (fault-ops fault)]
    (reify checker/Checker
      (check [_ _ history _]
        (let [ops (filter #(and (= :nemesis (:process %))
                                (#{start stop} (:f %))) history)
              pairs (partition-all 2 ops)
              completed (keep (fn [pair]
                                (when (= 2 (count pair))
                                  (let [[invoke response] pair]
                                    (when (and (= :info (:type invoke))
                                               (not (:error invoke))
                                               (= :info (:type response))
                                               (not (:error response))
                                               (= (select-keys invoke [:f :target :recovery])
                                                  (select-keys response [:f :target :recovery])))
                                      response)))) pairs)
              recovery (last (filter :recovery completed))
              cycles (remove :recovery completed)
              state (reduce (fn [{:keys [active counts valid?] :as acc} op]
                              (let [node (:target op)]
                                (cond
                                  (or (not valid?) (not (some #{node} nodes)))
                                  (assoc acc :valid? false)

                                  (= start (:f op))
                                  (if active (assoc acc :valid? false)
                                      (assoc acc :active node))

                                  (= node active)
                                  (assoc acc :active nil :counts (update counts node (fnil inc 0)))

                                  :else (assoc acc :valid? false))))
                            {:active nil :counts {} :valid? true} cycles)
              valid? (and (= (count ops) (* 2 (count completed)))
                          (= 1 (count (filter :recovery completed)))
                          (= stop (:f recovery))
                          (every? #(< (:index %) (:index recovery)) cycles)
                          (:valid? state) (nil? (:active state))
                          (every? #(pos? (get (:counts state) % 0)) nodes))]
          {:valid? (boolean valid?)
           :completed-fault-cycles (:counts state)
           :recovery-completed? (some? recovery)})))))

(defn final-reads [node-count]
  (apply gen/any
         (for [node-index (range node-count)]
           (gen/on-threads #(and (integer? %) (= node-index (mod % node-count)))
                           (map (fn [key-index]
                                  {:f :read :value (independent/tuple key-index nil) :final true})
                                (range (count register-keys)))))))

(defn final-read-checker
  ([] (final-read-checker nil))
  ([recovery-f]
  (reify checker/Checker
    (check [_ test history _]
      (let [writes (set (keep #(when (and (= :ok (:type %)) (= :write (:f %)))
                                  (first (:value %))) history))
            reads (filter #(and (:final %) (= :ok (:type %)) (= :read (:f %))) history)
            observed (set (map #(vector (:node %) (first (:value %))) reads))
            expected (set (for [node (:nodes test), key-index (range (count register-keys))]
                            [node key-index]))
            observed-split (last (filter #(and (= :observe-hotspot-split (:f %))
                                              (map? (:value %))) history))
            recovery (when recovery-f
                       (last (filter #(and (= :nemesis (:process %))
                                           (= recovery-f (:f %)) (:recovery %)
                                           (= :info (:type %)) (not (:error %))) history)))]
        {:valid? (boolean (and (= (set (range (count register-keys))) writes)
                              (= expected observed)
                              (some? observed-split)
                              (every? #(< (:index observed-split) (:index %)) reads)
                              (or (nil? recovery-f)
                                  (and recovery
                                       (every? #(< (:index recovery) (:index %)) reads)))))
         :missing-final-reads (vec (sort (remove observed expected)))
         :written-keys writes})))))

(defrecord StartupRetryClient [delegate]
  client/Client
  (open! [this test node]
    (assoc this :delegate (client/open! delegate test node)))
  (setup! [this test]
    (loop [delays [100 250 500 1000 2000 3000 5000 5000 5000]]
      (let [[ok result] (try [true (client/setup! delegate test)]
                             (catch Exception e [false e]))]
        (cond
          ok this
          (seq delays) (do (Thread/sleep (first delays)) (recur (rest delays)))
          :else (throw result)))))
  (invoke! [_ test op] (client/invoke! delegate test op))
  (teardown! [this test] (client/teardown! delegate test) this)
  (close! [this test]
    (assoc this :delegate (client/close! delegate test))))

(defn elastickv-hotspot-test
  ([] (elastickv-hotspot-test {}))
  ([opts]
   (let [nodes (or (:nodes opts) split/default-nodes)
         local? (:local opts)
         node->port (or (:node->port opts)
                        (cli/ports->node-map
                          (or (:redis-ports opts) (repeat (count nodes) (or (:redis-port opts) 6379))) nodes))
         requested-faults (cli/normalize-faults (or (:faults opts) [:partition :kill]))
         deterministic-fault (when (and (not local?) (= 1 (count requested-faults))
                                        (#{:partition :kill} (first requested-faults)))
                               (first requested-faults))
         bootstrap-members (when deterministic-fault
                             (str/join "," (map #(str % "=" % ":" (or (:grpc-port opts) 50051)) nodes)))
         db (if local? jdb/noop
              (ekdb/db (cond-> {:grpc-port (or (:grpc-port opts) 50051)
                                :redis-port node->port
                                :encryption (:encryption opts)
                                :auto-split-args auto-split-args}
                         deterministic-fault (assoc :bootstrap-members bootstrap-members))))
         interval (or (:fault-interval opts) 15)
         _ (when (and deterministic-fault
                      (or (not= #{"n1" "n2" "n3"} (set nodes))
                          (not= 3 (count nodes))
                          (not (pos? interval))
                          (not (pos? (or (:recovery-time opts) 10)))
                          (not (> (or (:time-limit opts) 120) (* 7 interval)))))
             (throw (ex-info "single-fault hotspot requires n1,n2,n3 and time-limit > 7 * fault-interval"
                             {:nodes nodes :fault deterministic-fault :interval interval})))
         faults (when-not local?
                  (if deterministic-fault
                    (deterministic-fault-package db nodes deterministic-fault interval)
                    (combined/nemesis-package
                      {:db db :faults requested-faults :interval interval})))
         controller (controller-package opts)
         package (combined/compose-packages
                   (cond-> [(if deterministic-fault
                              (assoc controller :generator (controller-observation-generator opts))
                              controller)]
                     faults (conj faults)))
         concurrency (or (:concurrency opts) 6)
         _ (when (or (< concurrency (max (* 2 (count register-keys)) (count nodes)))
                     (odd? concurrency))
             (throw (ex-info "hotspot workload requires even concurrency covering all nodes and registers"
                             {:concurrency concurrency :nodes (count nodes)})))
         workload (split/split-register-workload
                    (assoc opts :node->port node->port :register-keys register-keys
                           :independent-concurrency 2
                           :max-writes-per-key (or (:max-writes-per-key opts) 10000)))]
     (merge workload
            {:name (or (:name opts) "elastickv-m4-automatic-hotspot")
             :nodes nodes :db db :redis-host (:redis-host opts)
             :os (if (or local? deterministic-fault) os/noop debian/os)
             :net (if local? net/noop net/iptables)
             :ssh (merge {:username "vagrant" :private-key-path "/home/vagrant/.ssh/id_rsa"
                          :strict-host-key-checking false}
                         (when local? {:dummy true}) (:ssh opts))
             :remote control/ssh :nemesis (:nemesis package)
             :client (if deterministic-fault
                       (->StartupRetryClient (:client workload))
                       (:client workload))
             :checker (checker/compose (cond-> {:register (:checker workload)
                                                :controller (controller-checker)
                                                :final-reads (final-read-checker
                                                               (when deterministic-fault
                                                                 (second (fault-ops deterministic-fault))))}
                                         deterministic-fault
                                         (assoc :fault-cycles (fault-cycle-checker nodes deterministic-fault))))
             :concurrency concurrency
             :generator (apply gen/phases
                               (cond-> []
                                 deterministic-fault
                                 (conj (gen/nemesis
                                         (gen/once {:type :info :f :capture-hotspot-baseline})))
                                 true
                                 (conj (->> (:generator workload)
                                            (gen/nemesis (:generator package))
                                            (gen/stagger (/ (double (or (:rate opts) 20))))
                                            (gen/time-limit (or (:time-limit opts) 120)))
                                       (gen/nemesis (:final-generator package)))
                                 deterministic-fault
                                 (conj (gen/sleep (or (:recovery-time opts) 10)))
                                 true (conj (gen/clients (final-reads (count nodes))))))
             :final-generator nil}))))

(def hotspot-cli-opts
  [[nil "--redis-port PORT" "Redis port." :default 6379 :parse-fn #(Integer/parseInt %)]
   [nil "--grpc-host-port HOST:PORT" "Distribution gRPC address."]
   [nil "--list-routes-bin PATH" "Path to elastickv-list-routes."]
   [nil "--observe-after-seconds SECONDS" "Delay before route observation."
    :default 15 :parse-fn #(Integer/parseInt %)]
   [nil "--observe-timeout-ms MS" "Maximum route observation duration."
    :default 90000 :parse-fn #(Long/parseLong %)]
   [nil "--recovery-time SECONDS" "Wait after the final heal before final reads."
    :default 10 :parse-fn #(Double/parseDouble %)]])

(defn -main [& args]
  (cli/run-workload! args
                     (into (mapv (fn [option]
                                   (case (second option)
                                     "--time-limit SECONDS" (assoc option 4 120)
                                     "--concurrency N" (assoc option 4 6)
                                     "--faults LIST" (assoc option 4 "partition,kill")
                                     "--fault-interval SECONDS" (assoc option 4 10)
                                     option)) cli/common-cli-opts)
                           hotspot-cli-opts)
                     #(cli/parse-common-opts % nil) elastickv-hotspot-test))
