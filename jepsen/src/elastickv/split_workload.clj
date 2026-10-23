(ns elastickv.split-workload
  "Bounded M2 split workloads over Redis registers."
  (:gen-class)
  (:require [clojure.java.shell :as shell]
            [clojure.tools.logging :refer [info warn]]
            [elastickv.cli :as cli]
            [elastickv.composed1-nemesis :as routes]
            [elastickv.db :as ekdb]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [independent :as independent]
                    [net :as net]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control :as control]
            [jepsen.db :as jdb]
            [jepsen.nemesis :as nemesis]
            [jepsen.nemesis.combined :as combined]
            [jepsen.os :as os]
            [jepsen.os.debian :as debian]
            [knossos.model :as model]
            [taoensso.carmine :as car]))

(def default-nodes ["n1" "n2" "n3" "n4" "n5"])
(def ^:private split-key "m2-split")
(def ^:private register-keys ["m2-left" "m2-target"])
(def ^:private repeated-register-keys ["m2-left" "m2-stay" "m2-target" "m2-z"])
(def ^:private repeated-splits
  [{:split-key "m2-split" :same-owner? true}
   {:split-key "m2-t"}
   {:split-key "m2-u"}])

(defn- helper-path [name]
  (str (System/getProperty "user.dir") "/target/elastickv-jepsen/" name))

(defrecord SplitRegisterClient [node->port conn-spec]
  client/Client
  (open! [this test node]
    (assoc this :node node :conn-spec
           {:pool {}
            :spec {:host (or (:redis-host test) (name node))
                   :port (get node->port node 6379)
                   :timeout-ms 10000}}))
  (close! [this _test] (assoc this :conn-spec nil))
  (setup! [this _test]
    (doseq [k (or (:register-keys this) register-keys)]
      (car/wcar conn-spec (car/del k)))
    this)
  (teardown! [this _test] this)
  (invoke! [this _test op]
    (let [op (cond-> op (:final op) (assoc :node (:node this)))]
      (try
        (let [[k value] (:value op)
              redis-key (nth (or (:register-keys this) register-keys) k)]
          (case (:f op)
            :write (do (car/wcar conn-spec (car/set redis-key (str value)))
                       (assoc op :type :ok))
            :read  (let [raw (car/wcar conn-spec (car/get redis-key))
                         value (when raw (Long/parseLong (str raw)))]
                     (assoc op :type :ok :value (independent/tuple k value)))))
        (catch java.net.ConnectException _
          (assoc op :type :info :error :connection-refused))
        (catch java.net.SocketTimeoutException _
          (assoc op :type :info :error :socket-timeout))
        (catch Throwable t
          (assoc op :type :info :error (or (.getMessage t) (str t))))))))

(defn split-register-workload [opts]
  (let [keys (if (:repeat-splits opts) repeated-register-keys register-keys)
        client (assoc (->SplitRegisterClient
                        (or (:node->port opts) (zipmap default-nodes (repeat 6379)))
                        nil)
                      :register-keys keys)
        max-writes (or (:max-writes-per-key opts) 100)]
    {:client client
     :generator (independent/concurrent-generator
                  2
                  (range (count keys))
                  (fn [_]
                    (gen/mix [(map (fn [v] {:f :write :value v}) (range max-writes))
                              (repeat max-writes {:f :read})])))
     :checker (independent/checker
                (checker/compose
                  {:linear (checker/linearizable {:model (model/register)
                                                  :algorithm :competition})
                   :timeline (timeline/html)}))}))

(defn- grpc-address [opts test]
  (or (:grpc-host-port opts)
      (:grpc-host-port test)
      (let [node (name (first (:nodes test)))
            groups (:raft-groups test)
            port (if (seq groups)
                   (get groups (first (sort (keys groups))))
                   (or (:grpc-port test) 50051))]
        (str node ":" port))))

(defn- run-helper! [bin & args]
  (let [result (apply shell/sh bin args)]
    (when-not (zero? (:exit result))
      (throw (ex-info (str bin " failed") result)))
    (:out result)))

(defn- current-plan [opts test]
  (let [address (grpc-address opts test)
        snapshot (routes/parse-routes-json
                   (run-helper! (or (:list-routes-bin opts) (helper-path "elastickv-list-routes"))
                                "--address" address))
        source (routes/route-containing-key (:routes snapshot) split-key)
        groups (sort (keys (:raft-groups test)))
        target (or (:target-group-id opts)
                   (first (remove #{(:raft-group-id source)} groups)))]
    (when-not (and source target)
      (throw (ex-info "split workload requires an active source route and a different target group"
                      {:source source :groups groups})))
    {:address address
     :catalog-version (:catalog-version snapshot)
     :route-id (:route-id source)
     :target-group-id target}))

(defn- start-split! [opts test]
  (let [{:keys [address catalog-version route-id target-group-id] :as plan}
        (current-plan opts test)
        output (run-helper! (or (:split-bin opts) (helper-path "elastickv-split"))
                            "--address" address
                            "--route-id" (str route-id)
                            "--split-key" split-key
                            "--expected-version" (str catalog-version)
                            "--target-group-id" (str target-group-id))
        job-id (some->> (re-find #"job_id:\s*(\d+)" output) second Long/parseLong)]
    (when-not job-id
      (throw (ex-info "split helper did not return job_id" {:output output :plan plan})))
    (assoc plan :job-id job-id :output output)))

(defn- job-phase [opts address job-id]
  (let [output (run-helper! (or (:split-bin opts) (helper-path "elastickv-split"))
                            "--address" address "--get-job-id" (str job-id))]
    (some->> (re-find #"phase:\s*(\S+)" output) second)))

(defn- await-phase! [opts address job-id wanted timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (let [phase (job-phase opts address job-id)]
        (cond
          (contains? wanted phase) phase
          (> (System/currentTimeMillis) deadline)
          (throw (ex-info "timed out waiting for split job phase"
                          {:job-id job-id :phase phase :wanted wanted}))
          :else (do (Thread/sleep 100) (recur)))))))

(defn split-nemesis [opts]
  (let [active (atom nil)]
    (reify
      nemesis/Reflection
      (fs [_] #{:start-cross-group-split :verify-cross-group-split})
      nemesis/Nemesis
      (setup! [this _test] this)
      (invoke! [_ test op]
        (try
          (case (:f op)
            :start-cross-group-split
            (let [plan (start-split! opts test)]
              (reset! active plan)
              (if (:abandon-at-fence opts)
                (do
                  (await-phase! opts (:address plan) (:job-id plan)
                                #{"SPLIT_JOB_PHASE_FENCE"} 60000)
                  (run-helper! (or (:split-bin opts) (helper-path "elastickv-split"))
                               "--address" (:address plan)
                               "--abandon-job-id" (str (:job-id plan))))
                (info "started M2 split job" (:job-id plan)))
              (assoc op :value plan))

            :verify-cross-group-split
            (let [{:keys [address job-id] :as plan} @active
                  wanted (if (:abandon-at-fence opts)
                           #{"SPLIT_JOB_PHASE_ABANDONED"}
                           #{"SPLIT_JOB_PHASE_DONE"})
                  phase (await-phase! opts address job-id wanted 120000)]
              (assoc op :value (assoc plan :phase phase)))

            op)
          (catch Throwable t
            (warn t "M2 split nemesis failed")
            (assoc op :type :fail :error (or (.getMessage t) (str t))))))
      (teardown! [this _test] this))))

(defn split-package [opts]
  {:generator (gen/phases
                (gen/sleep (or (:split-at-seconds opts) 5))
                (gen/once {:type :info :f :start-cross-group-split})
                (gen/sleep (or (:split-verify-after-seconds opts) 20))
                (gen/once {:type :info :f :verify-cross-group-split}))
   :final-generator nil
   :nemesis (split-nemesis opts)
   :perf #{{:name "cross-group-split"
            :fs #{:start-cross-group-split :verify-cross-group-split}
            :start #{:start-cross-group-split}
            :stop #{:verify-cross-group-split}
            :color "#4A90E2"}}})

(defn split-lifecycle-checker [opts]
  (reify checker/Checker
    (check [_ _test history _checker-opts]
      (let [operations (filter #(and (= :nemesis (:process %))
                                     (contains? #{:start-cross-group-split :verify-cross-group-split} (:f %)))
                               history)
            failures (filter #(or (= :fail (:type %)) (:error %)) operations)
            ;; Jepsen also logs invocation placeholders with no result value.
            completions (vec (filter #(map? (:value %)) operations))
            [start verify] completions
            job-id (get-in start [:value :job-id])
            expected (if (:abandon-at-fence opts)
                       "SPLIT_JOB_PHASE_ABANDONED"
                       "SPLIT_JOB_PHASE_DONE")]
        {:valid? (and (empty? failures)
                      (= 2 (count completions))
                      (= :start-cross-group-split (:f start))
                      (= :verify-cross-group-split (:f verify))
                      (every? #(contains? #{:info :ok} (:type %)) completions)
                      (integer? job-id) (pos? job-id)
                      (= job-id (get-in verify [:value :job-id]))
                      (= expected (get-in verify [:value :phase])))
         :expected-phase expected
         :completions completions
         :failures (vec failures)}))))

(defn- route-snapshot! [opts test]
  (update (routes/parse-routes-json
            (run-helper! (or (:list-routes-bin opts) (helper-path "elastickv-list-routes"))
                         "--address" (grpc-address opts test)))
          :routes #(vec (sort-by :route-id %))))

(defn- exact-range [snapshot start end]
  (filter #(and (= start (:start %)) (= end (:end %))) (:routes snapshot)))

(defn- repeated-plan! [opts test step previous]
  (let [{:keys [split-key same-owner?]} (nth repeated-splits step)
        before (route-snapshot! opts test)
        sources (filter #(routes/route-covers? % split-key) (:routes before))
        source (first sources)
        target (if same-owner? (:raft-group-id source)
                 (if (= 1 step)
                   (or (:target-group-id opts)
                       (first (remove #{(:raft-group-id source)} (sort (keys (:raft-groups test))))))
                   (:raft-group-id (:source previous))))]
    (when-not (and (= 1 (count sources))
                   (neg? (compare (:start source) split-key))
                   target (or same-owner? (not= target (:raft-group-id source)))
                   (or (zero? step) (= source (:right previous))))
      (throw (ex-info "repeated split requires the previous moving child and a valid owner"
                      {:step step :source source :target target :previous previous})))
    {:step step :split-key split-key :same-owner? (boolean same-owner?)
     :address (grpc-address opts test) :before before :source source
     :route-id (:route-id source) :target-group-id target}))

(defn- begin-repeated-split! [opts plan]
  (let [{:keys [address route-id split-key before same-owner? target-group-id]} plan
        args (cond-> ["--address" address "--route-id" (str route-id)
                      "--split-key" split-key "--expected-version" (str (:catalog-version before))]
               (not same-owner?) (into ["--target-group-id" (str target-group-id)]))
        output (apply run-helper! (or (:split-bin opts) (helper-path "elastickv-split")) args)
        job-id (some->> (re-find #"job_id:\s*(\d+)" output) second Long/parseLong)]
    (when (and (not same-owner?) (not (and job-id (pos? job-id))))
      (throw (ex-info "split helper did not return a positive job_id" {:output output :plan plan})))
    (assoc plan :job-id job-id)))

(declare partition-valid?)

(defn- finish-repeated-split! [opts test plan]
  (let [deadline (+ (System/currentTimeMillis) (or (:split-timeout-ms opts) 120000))]
    (loop [observations []]
      (let [phase (if (:same-owner? plan) "SPLIT_JOB_PHASE_DONE"
                    (job-phase opts (:address plan) (:job-id plan)))
            snapshot (route-snapshot! opts test)
            observation {:phase phase :snapshot snapshot}
            observations (if (= observation (peek observations)) observations
                             (conj observations observation))]
        (when (> (count observations) 64)
          (throw (ex-info "too many distinct split observations" {:plan plan})))
        (cond
          (= "SPLIT_JOB_PHASE_DONE" phase)
          (let [result (assoc plan :phase phase :after snapshot :observations observations
                              :right (first (exact-range snapshot (:split-key plan) (:end (:source plan)))))]
            (when-not (partition-valid? result)
              (throw (ex-info "split completed with invalid partition geometry" result)))
            result)
          (or (contains? #{"SPLIT_JOB_PHASE_FAILED" "SPLIT_JOB_PHASE_ABANDONING"
                           "SPLIT_JOB_PHASE_ABANDONED"} phase)
              (> (System/currentTimeMillis) deadline))
          (throw (ex-info "repeated split did not complete" {:plan plan :observations observations}))
          :else (do (Thread/sleep 250) (recur observations)))))))

(defn repeated-split-nemesis [opts]
  (let [state (atom {:next-step 0})]
    (reify
      nemesis/Reflection
      (fs [_] #{:start-repeated-split :verify-repeated-split})
      nemesis/Nemesis
      (setup! [this _test] this)
      (invoke! [_ test op]
        (try
          (let [{:keys [next-step active previous failed?]} @state
                step (get-in op [:value :step])]
            (when (or failed? (not= step next-step) (>= next-step (count repeated-splits)))
              (throw (ex-info "out-of-order repeated split" {:state @state :op op})))
            (case (:f op)
              :start-repeated-split
              (do
                (when active (throw (ex-info "split already active" {:active active})))
                (let [plan (begin-repeated-split! opts (repeated-plan! opts test step previous))]
                  (swap! state assoc :active plan)
                  (assoc op :value plan)))
              :verify-repeated-split
              (do
                (when-not active (throw (ex-info "no active repeated split" {:step step})))
                (let [result (finish-repeated-split! opts test active)]
                  (reset! state {:next-step (inc next-step) :previous result})
                  (assoc op :value result)))
              op))
          (catch Throwable t
            (swap! state assoc :failed? true)
            (assoc op :type :fail :error (or (.getMessage t) (str t))
                   :details (ex-data t)))))
      (teardown! [this _test] this))))

(defn repeated-split-package [opts]
  {:generator (apply gen/phases
                     (gen/sleep (or (:split-at-seconds opts) 5))
                     (mapcat (fn [step]
                               [(gen/once {:type :info :f :start-repeated-split :value {:step step}})
                                (gen/once {:type :info :f :verify-repeated-split :value {:step step}})])
                             (range (count repeated-splits))))
   :final-generator nil
   :nemesis (repeated-split-nemesis opts)
   :perf #{}})

(defn- partition-valid? [{:keys [before after source split-key target-group-id observations route-id right]}]
  (let [lefts (exact-range after (:start source) split-key)
        rights (exact-range after split-key (:end source))
        left (first lefts)
        positive-id? #(and (integer? %) (pos? %))
        without-source (remove #{source} (:routes before))]
    (and (= source (first (filter #(= route-id (:route-id %)) (:routes before))))
         (= route-id (:route-id source))
         (routes/route-covers? source split-key)
         (neg? (compare (:start source) split-key))
         (= 1 (count lefts)) (= 1 (count rights)) (= right (first rights))
         (every? positive-id? [route-id (:route-id left) (:route-id right)])
         (= 3 (count (set [route-id (:route-id left) (:route-id right)])))
         (= "ROUTE_STATE_ACTIVE" (:state left) (:state right))
         (= (:raft-group-id source) (:raft-group-id left))
         (= target-group-id (:raft-group-id right))
         (< (:catalog-version before) (:catalog-version after))
         (= (set (:routes after)) (set (concat without-source [left right])))
         (= (inc (count (:routes before))) (count (:routes after)))
         (= (count (:routes after)) (count (set (map :route-id (:routes after)))))
         (seq observations)
         ;; SourceRouteID is lineage. Only the moving child's observed ID is
         ;; stable across FENCE/cutover; it must not be replaced at cutover.
         (= after (:snapshot (peek observations)))
         (= "SPLIT_JOB_PHASE_DONE" (:phase (peek observations)))
         (apply <= (:catalog-version before) (map #(get-in % [:snapshot :catalog-version]) observations))
         (every? (fn [{:keys [snapshot]}]
                   (let [moving (first (exact-range snapshot split-key (:end source)))]
                     (or (= (set (:routes before)) (set (:routes snapshot)))
                         (and (= (:route-id right) (:route-id moving))
                              (contains? (set [(:raft-group-id source) target-group-id]) (:raft-group-id moving))
                              (contains? #{"ROUTE_STATE_ACTIVE" "ROUTE_STATE_WRITE_FENCED"} (:state moving))
                              (= (set (:routes snapshot))
                                 (set (concat without-source [left moving])))
                              (= (count (:routes after)) (count (:routes snapshot)))))))
                 observations))))

(defn repeated-split-checker []
  (reify checker/Checker
    (check [_ _test history _checker-opts]
      (let [ops (filter #(and (= :nemesis (:process %))
                              (contains? #{:start-repeated-split :verify-repeated-split} (:f %))) history)
            failures (vec (filter #(or (= :fail (:type %)) (:error %)) ops))
            completions (vec (filter #(and (contains? #{:info :ok} (:type %))
                                           (:source (:value %))) ops))
            pairs (partition 2 completions)
            results (mapv (comp :value second) pairs)]
        {:valid? (boolean
                   (and (empty? failures) (= 6 (count completions))
                        (every? true?
                          (map-indexed
                            (fn [step [start verify]]
                              (let [plan (:value start) result (:value verify)
                                    same? (zero? step)]
                                (and (= :start-repeated-split (:f start))
                                     (= :verify-repeated-split (:f verify))
                                     (= step (:step plan) (:step result))
                                     (= (:split-key (nth repeated-splits step)) (:split-key plan))
                                     (= same? (:same-owner? plan))
                                     (= plan (select-keys result (keys plan)))
                                     (= "SPLIT_JOB_PHASE_DONE" (:phase result))
                                     (if same? (and (nil? (:job-id plan))
                                                    (= (:target-group-id plan) (:raft-group-id (:source plan))))
                                       (and (integer? (:job-id plan)) (pos? (:job-id plan))
                                            (not= (:target-group-id plan) (:raft-group-id (:source plan)))))
                                     (partition-valid? result)))) pairs))
                        (= 2 (count (set (keep :job-id results))))
                        (every? (fn [[previous result]]
                                  (and (= (:right previous) (:source result))
                                       (= (:after previous) (:before result))))
                                (partition 2 1 results))
                        (= (:raft-group-id (:source (first results)))
                           (:target-group-id (last results)))))
         :completions completions :failures failures}))))

(defn repeated-final-reads [node-count]
  (apply gen/any
         (for [node-index (range node-count)]
           (gen/on-threads #(and (integer? %) (= node-index (mod % node-count)))
                           (map (fn [k] {:f :read :value (independent/tuple k nil) :final true})
                                (range (count repeated-register-keys)))))))

(defn repeated-read-checker []
  (reify checker/Checker
    (check [_ test history _opts]
      (let [writes (set (keep #(when (and (= :ok (:type %)) (= :write (:f %)))
                                  (first (:value %))) history))
            reads (filter #(and (:final %) (= :ok (:type %)) (= :read (:f %))) history)
            last-verify (last (filter #(and (= :verify-repeated-split (:f %))
                                            (= "SPLIT_JOB_PHASE_DONE" (get-in % [:value :phase]))) history))
            observed (set (map #(vector (:node %) (first (:value %))) reads))
            expected (set (for [n (:nodes test), k (range (count repeated-register-keys))] [n k]))]
        {:valid? (and (= (set (range (count repeated-register-keys))) writes)
                       (= expected observed)
                       (some? last-verify)
                       (every? #(< (:index last-verify) (:index %)) reads))
         :missing-final-reads (vec (sort (remove observed expected)))
         :written-keys writes}))))

(defn elastickv-split-test
  ([] (elastickv-split-test {}))
  ([opts]
   (when (and (:repeat-splits opts) (:abandon-at-fence opts))
     (throw (ex-info "repeated splits require completed migrations" {})))
   (let [nodes (or (:nodes opts) default-nodes)
         concurrency (or (:concurrency opts) (if (:repeat-splits opts) 8 6))
         _ (when (and (:repeat-splits opts)
                      (or (< concurrency (max (count nodes) 8)) (odd? concurrency)))
             (throw (ex-info "repeated splits require even concurrency >= max(node count, 8)" {})))
         ports (or (:redis-ports opts) (repeat (count nodes) (or (:redis-port opts) 6379)))
         node->port (or (:node->port opts) (cli/ports->node-map ports nodes))
         local? (:local opts)
         db (if local? jdb/noop
              (ekdb/db {:grpc-port (or (:grpc-port opts) 50051)
                        :redis-port node->port
                        :raft-groups (:raft-groups opts)
                        :shard-ranges (:shard-ranges opts)
                        :migration-enabled true}))
         fault-package (when-not local?
                         (combined/nemesis-package
                           {:db db
                            :faults (cli/normalize-faults (or (:faults opts) [:partition :kill]))
                            :interval (or (:fault-interval opts) 10)}))
         package (combined/compose-packages
                   (cond-> [(if (:repeat-splits opts) (repeated-split-package opts) (split-package opts))]
                     fault-package (conj fault-package)))
         workload (split-register-workload (assoc opts :node->port node->port))
         main-gen (->> (:generator workload)
                        (gen/nemesis (:generator package))
                        (gen/stagger (/ (double (or (:rate opts) 10))))
                        (gen/time-limit (or (:time-limit opts) 60)))
         checkers (cond-> {:register (:checker workload)
                           :split-lifecycle (if (:repeat-splits opts)
                                              (repeated-split-checker)
                                              (split-lifecycle-checker opts))}
                    (:repeat-splits opts) (assoc :final-reads (repeated-read-checker)))
         generator (if (:repeat-splits opts)
                     (gen/phases main-gen
                                 (gen/nemesis (:final-generator package))
                                 (gen/clients (repeated-final-reads (count nodes))))
                     main-gen)]
     (merge workload
            {:name (or (:name opts) (if (:repeat-splits opts) "elastickv-m2-repeated-split"
                                       "elastickv-m2-cross-group-split"))
             :nodes nodes
             :db db
             :raft-groups (:raft-groups opts)
             :grpc-port (:grpc-port opts)
             :grpc-host-port (:grpc-host-port opts)
             :redis-host (:redis-host opts)
             :os (if local? os/noop debian/os)
             :net (if local? net/noop net/iptables)
             :ssh (merge {:username "vagrant"
                          :private-key-path "/home/vagrant/.ssh/id_rsa"
                          :strict-host-key-checking false}
                         (when local? {:dummy true}) (:ssh opts))
             :remote control/ssh
             :nemesis (:nemesis package)
             :checker (checker/compose checkers)
             :final-generator nil
             :concurrency concurrency
             :generator generator}))))

(def split-cli-opts
  [[nil "--redis-port PORT" "Redis port." :default 6379 :parse-fn #(Integer/parseInt %)]
   [nil "--target-group-id ID" "Target Raft group." :parse-fn #(Long/parseLong %)]
   [nil "--grpc-host-port HOST:PORT" "Distribution gRPC address."]
   [nil "--split-bin PATH" "Path to elastickv-split."]
   [nil "--list-routes-bin PATH" "Path to elastickv-list-routes."]
   [nil "--split-at-seconds SECONDS" "Workload time to start migration." :default 5 :parse-fn #(Integer/parseInt %)]
   [nil "--split-verify-after-seconds SECONDS" "Delay before verifying terminal state." :default 20 :parse-fn #(Integer/parseInt %)]
   [nil "--repeat-splits" "Run one same-owner and two successive cross-group splits." :default false]
   [nil "--abandon-at-fence" "Abandon after observing FENCE." :default false]])

(defn -main [& args]
  (cli/run-workload! args
                     (into cli/common-cli-opts split-cli-opts)
                     #(cli/parse-common-opts % nil)
                     elastickv-split-test))
