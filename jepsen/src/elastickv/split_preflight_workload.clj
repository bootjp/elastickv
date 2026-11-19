(ns elastickv.split-preflight-workload
  "Pause one voting process only after the catalog leader reports a partial preflight."
  (:refer-clojure :exclude [run!])
  (:require [clojure.data.json :as json]
            [clojure.java.shell :as sh]
            [clojure.set :as set]
            [clojure.string :as str]
            [elastickv.cli :as cli]
            [elastickv.db :as db]
            [elastickv.grpc-workload :as grpc]
            [elastickv.hosted-fault-workload :as hosted]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.independent :as independent]
            [jepsen.nemesis :as nemesis]
            [jepsen.tests :as tests]))

(def nodes ["n1" "n2" "n3"])
(def ports (zipmap nodes [25101 25102 25103]))
(def split-key "jepsen-grpc/raw/1")
(def seeds {0 4101, 1 4202})

(defn- run! [& args]
  (let [{:keys [exit out err]} (apply sh/sh args)]
    (when-not (zero? exit)
      (throw (ex-info "Preflight helper failed" {:command (first args) :stderr err :exit exit})))
    (str/trim out)))

(defn- path [file] (str (System/getenv "PREFLIGHT_DIR") "/" file))
(defn- address [node] (str "127.0.0.1:" (ports node)))
(defn- status [node] (db/parse-raft-status (run! (path "raftadmin") (address node) "status")))
(defn- routes [node]
  (json/read-str (run! (path "elastickv-list-routes") "--address" (address node)
                       "--tls-ca-file" (path "cert.pem")) :key-fn keyword))

(defn- split [node & args]
  (apply run! (path "elastickv-split") "--address" (address node)
         "--tls-ca-file" (path "cert.pem") "--token-file" (path "token") args))

(defn- sample [node job]
  (let [j (json/read-str (split node "--get-job-id" job "--job-json") :key-fn keyword)
        progress (:transfer_preflight_progress j)
        ids (fn [xs] (set (map (juxt :family :bracket_id) xs)))]
    {:job (:job_id j) :phase (:phase j) :scan-ts (:transfer_preflight_ts j)
     :plan (ids progress) :completed (ids (filter :done progress))
     :done (:transfer_preflight_done j) :fence (:fence_ts j)
     :source (:source_group_id j) :target (:target_group_id j) :error (:last_error j)}))

(defn partial-scan? [s]
  (and (= "SPLIT_JOB_PHASE_FENCE" (:phase s)) (false? (:done s))
       (= "0" (:fence s)) (some-> (:scan-ts s) parse-long pos?)
       (< 0 (count (:completed s)) (count (:plan s)))))

(defn- await-sample [node job seconds poll-ms accept?]
  (let [deadline (+ (System/nanoTime) (* seconds 1000000000))]
    (loop []
      (let [s (sample node job)]
        (cond (accept? s) s
              (or (contains? #{"SPLIT_JOB_PHASE_FAILED" "SPLIT_JOB_PHASE_ABANDONING"
                               "SPLIT_JOB_PHASE_ABANDONED" "SPLIT_JOB_PHASE_DONE"} (:phase s))
                  (>= (System/nanoTime) deadline))
              (throw (ex-info "Required split checkpoint was not observed" s))
              :else (do (Thread/sleep poll-ms) (recur)))))))

(defn- process-state [pid] (run! "ps" "-o" "stat=" "-p" pid))
(defn- stopped? [state] (str/starts-with? state "T"))

(defn- pause! [active]
  (let [leader (:leader_id (status "n1"))
        address-status (status leader)
        victim (first (remove #{leader} nodes))
        pid (str/trim (slurp (path (str victim ".pid"))))
        catalog (routes leader)
        [route] (:routes catalog)]
    (when-not (and (= "LEADER" (:state address-status)) (= 1 (count (:routes catalog)))
                   (= 1 (:raft_group_id route)) (= "" (:start route) (:end route)))
      (throw (ex-info "Expected the initial single group-1 route on the catalog leader" catalog)))
    (let [out (split leader "--route-id" (str (:route_id route)) "--expected-version"
                     (str (:catalog_version catalog)) "--split-key" split-key "--target-group-id" "2")
          job (second (re-find #"job_id:\s*(\d+)" out))
          _ (when-not job (throw (ex-info "No migration job ID" {:output out})))
          before (await-sample leader job 180 150 partial-scan?)]
      (reset! active {:leader leader :pid pid :victim victim :job job})
      (run! "kill" "-STOP" pid)
      (let [state (process-state pid)
            during (sample leader job)
            current (status leader)]
        (when-not (and (stopped? state) (= "LEADER" (:state current))
                       (= (:term address-status) (:term current)) (= leader (:leader_id current)))
          (throw (ex-info "Leader changed or voter did not stop" {:state state :leader current})))
        {:leader leader :victim victim :state state :term (:term current)
         :before before :during during}))))

(defn- resume! [{:keys [victim pid]}]
  (when-not (stopped? (process-state pid))
    (throw (ex-info "Voter resumed before the healing operation" {:victim victim})))
  (run! "kill" "-CONT" pid)
  (let [state (process-state pid)]
    (when (stopped? state) (throw (ex-info "Voter did not resume" {:state state})))
    {:victim victim :state state}))

(defn- finish! [{:keys [leader job]}]
  (let [final (await-sample leader job 360 500 #(= "SPLIT_JOB_PHASE_DONE" (:phase %)))
        catalog (routes leader)]
    {:final final :routes (:routes catalog)}))

(defn- preflight-nemesis []
  (let [active (atom nil)]
    (reify nemesis/Nemesis
      (setup! [this _] this)
      (invoke! [_ _ op]
        (try
          (assoc op :confirmed true :value
                 (case (:f op)
                   :pause-preflight (pause! active)
                   :resume-voter (resume! @active)
                   :finish-split (finish! @active)))
          (catch Exception e
            (assoc op :error (.getMessage e) :details (ex-data e)))))
      (teardown! [_ _]
        (when-let [pid (:pid @active)] (run! "kill" "-CONT" pid))))))

(defn valid-checkpoints? [before during final]
  (and (partial-scan? before) (partial-scan? during)
       (= "SPLIT_JOB_PHASE_DONE" (:phase final)) (true? (:done final))
       (= (:job before) (:job during) (:job final))
       (= (:scan-ts before) (:scan-ts during) (:scan-ts final))
       (= (:plan before) (:plan during) (:plan final) (:completed final))
       (set/subset? (:completed before) (:completed during))
       (= "1" (:source final)) (= "2" (:target final))))

(defn- correct-routes? [routes]
  (let [boundary (.encodeToString (java.util.Base64/getEncoder) (.getBytes split-key "UTF-8"))]
    (and (= 2 (count routes))
         (= #{["" boundary 1] [boundary "" 2]}
            (set (map (juxt :start :end :raft_group_id) routes)))
         (every? #(= "ROUTE_STATE_ACTIVE" (:state %)) routes))))

(defn recovery-checker []
  (reify checker/Checker
    (check [_ _ history _]
      (let [[pause resume finish :as events] (filter :confirmed history)
            {:keys [leader victim state before during]} (:value pause)
            {:keys [final routes]} (:value finish)
            ok (filter #(= :ok (:type %)) history)
            reads (filter #(and (= :read (:f %)) (:final %)) ok)
            coverage (set (map (juxt :node (comp first :value)) reads))
            expected (set (for [n nodes k (keys seeds)] [n k]))
            during-reads (filter #(and (:during %) (= :read (:f %))
                                       (< (:index pause) (:index %) (:index resume))) ok)
            written (into {} (map :value (filter #(= :write (:f %)) ok)))]
        {:valid? (boolean
                   (and (= [:pause-preflight :resume-voter :finish-split] (mapv :f events))
                        (= seeds written) (not= victim leader) (stopped? state)
                        (= victim (get-in resume [:value :victim]))
                        (valid-checkpoints? before during final) (correct-routes? routes)
                        (= (set (keys seeds)) (set (map (comp first :value) during-reads)))
                        (= expected coverage) (every? #(< (:index finish) (:index %)) reads)
                        (every? (fn [{[k v] :value}] (= (seeds k) v))
                                (filter #(= :read (:f %)) ok))))
         :leader leader :paused-voter victim :stopped-state state
         :job (:job before) :scan-ts (:scan-ts before) :before (count (:completed before))
         :while-stopped (count (:completed during)) :after (count (:completed final))
         :acknowledged-seeds written :successful-final-reads (count reads)
         :missing-final-reads (set/difference expected coverage)
         :errors (vec (keep #(when (and (= :nemesis (:process %)) (:error %))
                               (select-keys % [:f :error :details])) history))}))))

(defn- probe-reads [tag]
  (apply gen/any
         (for [i (range (count nodes))]
           (gen/on-threads #{i} (mapv (fn [k] {:f :read :value (independent/tuple k nil) tag true})
                                      (keys seeds))))))

(defn- workload [_]
  (let [client (grpc/->GRPCClient ports :raw (path "jepsen-grpc-client") nil nil nil)
        event #(gen/nemesis (gen/once {:type :info :f %}))]
    (merge tests/noop-test
           {:name "elastickv-split-preflight-pause" :nodes nodes :concurrency 3
            :grpc-host "127.0.0.1" :grpc-tls-ca-file (path "cert.pem")
            :client (hosted/->ObservedClient client nil) :nemesis (preflight-nemesis)
            :checker (recovery-checker)
            :generator (gen/phases
                         (gen/clients (map (fn [[k v]] {:f :write :value (independent/tuple k v)}) seeds))
                         (event :pause-preflight) (gen/clients (probe-reads :during))
                         (event :resume-voter) (event :finish-split) (gen/clients (probe-reads :final)))})))

(defn -main [& args]
  (cli/run-workload! args cli/common-cli-opts #(cli/parse-common-opts % nil) workload))
