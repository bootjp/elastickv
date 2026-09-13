(ns elastickv.split-workload
  "Deterministic M2 cross-group split workload over two Redis registers."
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

(defn- helper-path [name]
  (str (System/getProperty "user.dir") "/target/elastickv-jepsen/" name))

(defrecord SplitRegisterClient [node->port conn-spec]
  client/Client
  (open! [this test node]
    (assoc this :conn-spec
           {:pool {}
            :spec {:host (or (:redis-host test) (name node))
                   :port (get node->port node 6379)
                   :timeout-ms 10000}}))
  (close! [this _test] (assoc this :conn-spec nil))
  (setup! [this _test]
    (doseq [k register-keys]
      (car/wcar conn-spec (car/del k)))
    this)
  (teardown! [this _test] this)
  (invoke! [_this _test op]
    (try
      (let [[k value] (:value op)
            redis-key (nth register-keys k)]
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
        (assoc op :type :info :error (or (.getMessage t) (str t)))))))

(defn split-register-workload [opts]
  (let [client (->SplitRegisterClient
                 (or (:node->port opts) (zipmap default-nodes (repeat 6379)))
                 nil)
        max-writes (or (:max-writes-per-key opts) 100)]
    {:client client
     :generator (independent/concurrent-generator
                  2
                  (range (count register-keys))
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

(defn elastickv-split-test
  ([] (elastickv-split-test {}))
  ([opts]
   (let [nodes (or (:nodes opts) default-nodes)
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
                   (cond-> [(split-package opts)] fault-package (conj fault-package)))
         workload (split-register-workload (assoc opts :node->port node->port))]
     (merge workload
            {:name (or (:name opts) "elastickv-m2-cross-group-split")
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
             :final-generator nil
             :concurrency (or (:concurrency opts) 6)
             :generator (->> (:generator workload)
                             (gen/nemesis (:generator package))
                             (gen/stagger (/ (double (or (:rate opts) 10))))
                             (gen/time-limit (or (:time-limit opts) 60)))}))))

(def split-cli-opts
  [[nil "--redis-port PORT" "Redis port." :default 6379 :parse-fn #(Integer/parseInt %)]
   [nil "--target-group-id ID" "Target Raft group." :parse-fn #(Long/parseLong %)]
   [nil "--grpc-host-port HOST:PORT" "Distribution gRPC address."]
   [nil "--split-bin PATH" "Path to elastickv-split."]
   [nil "--list-routes-bin PATH" "Path to elastickv-list-routes."]
   [nil "--split-at-seconds SECONDS" "Workload time to start migration." :default 5 :parse-fn #(Integer/parseInt %)]
   [nil "--split-verify-after-seconds SECONDS" "Delay before verifying terminal state." :default 20 :parse-fn #(Integer/parseInt %)]
   [nil "--abandon-at-fence" "Abandon after observing FENCE." :default false]])

(defn -main [& args]
  (cli/run-workload! args
                     (into cli/common-cli-opts split-cli-opts)
                     #(cli/parse-common-opts % nil)
                     elastickv-split-test))
