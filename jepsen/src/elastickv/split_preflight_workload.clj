(ns elastickv.split-preflight-workload
  "Exercise catalog preflight and online split recovery with a stopped voter."
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
            [jepsen.net :as net]
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
     :source (:source_group_id j) :target (:target_group_id j)
     :online-version (:online_capture_version j) :online-sequence (:online_sequence j)
     :seal-sequence (:online_seal_sequence j) :online-manifest (:online_manifest j)
     :capability-regressed (:capability_regressed j) :error (:last_error j)}))

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
(defn- stopped? [state] (and (string? state) (str/starts-with? state "T")))

(defn- submit-split! [leader]
  (let [catalog (routes leader)
        [route] (:routes catalog)]
    (when-not (and (= 1 (count (:routes catalog)))
                   (= 1 (:raft_group_id route)) (= "" (:start route) (:end route)))
      (throw (ex-info "Expected the initial single group-1 route on the catalog leader" catalog)))
    (let [out (split leader "--route-id" (str (:route_id route)) "--expected-version"
                     (str (:catalog_version catalog)) "--split-key" split-key "--target-group-id" "2")
          job (second (re-find #"job_id:\s*(\d+)" out))]
      (or job (throw (ex-info "No migration job ID" {:output out}))))))

(defn- pause! [active]
  (let [leader (:leader_id (status "n1"))
        address-status (status leader)
        victim (first (remove #{leader} nodes))
        pid (str/trim (slurp (path (str victim ".pid"))))]
    (when-not (= "LEADER" (:state address-status))
      (throw (ex-info "Expected a catalog leader" address-status)))
    (let [job (submit-split! leader)
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

(defn- online-checkpoint? [s]
  (and (partial-scan? s) (= 1 (:online-version s))
       (not (str/blank? (:online-manifest s))) (= "0" (:seal-sequence s))))

(defn- start-online! [active]
  (let [leader (:leader_id (status "n1"))
        current (status leader)]
    (when-not (and (= "LEADER" (:state current)) (= leader (:leader_id current))
                   (integer? (:term current)))
      (throw (ex-info "Expected a catalog leader with a term" current)))
    (let [job (submit-split! leader)
          checkpoint (await-sample leader job 180 25 online-checkpoint?)]
      (reset! active {:leader leader :term (:term current) :job job})
      {:leader leader :term (:term current) :sample checkpoint})))

(defn- replacement-leader! [victim term]
  (let [deadline (+ (System/nanoTime) 25000000000)]
    (loop []
      (or (some (fn [node]
                  (when-let [s (try (status node) (catch Exception _ nil))]
                    (when (and (= "LEADER" (:state s)) (= node (:leader_id s))
                               (integer? (:term s)) (> (:term s) term))
                      {:leader node :term (:term s)})))
                (remove #{victim} nodes))
          (if (< (System/nanoTime) deadline)
            (do (Thread/sleep 100) (recur))
            (throw (ex-info "No new catalog leader while the original was stopped"
                            {:victim victim :term term})))))))

(defn- failover! [active]
  (let [{old :leader old-term :term job :job} @active
        before (sample old job)
        current (status old)
        pid (str/trim (slurp (path (str old ".pid"))))]
    (when-not (and (online-checkpoint? before) (= "LEADER" (:state current))
                   (= old (:leader_id current)) (= old-term (:term current)))
      (throw (ex-info "Online checkpoint or original leader was lost before injection"
                      {:before before :status current})))
    (swap! active assoc :pid pid :victim old)
    (run! "kill" "-STOP" pid)
    (let [{:keys [leader term]} (replacement-leader! old old-term)
          state (process-state pid)
          during (sample leader job)]
      (when-not (and (stopped? state) (online-checkpoint? during))
        (throw (ex-info "The new leader did not observe the unfinished online checkpoint"
                        {:state state :term term :sample during})))
      (swap! active assoc :leader leader)
      {:victim old :leader leader :term term :state state :before before :during during})))

(defn- preflight-nemesis []
  (let [active (atom nil)]
    (reify nemesis/Nemesis
      (setup! [this _] this)
      (invoke! [_ _ op]
        (try
          (assoc op :confirmed true :value
                 (case (:f op)
                   :pause-preflight (pause! active)
                   :start-online (start-online! active)
                   :failover (failover! active)
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
            last-sample (or final (:details (last (filter #(and (= :finish-split (:f %)) (:error %)) history))))
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
                        ;; A capability regression may close the moving range.
                        (some #(= 0 (first (:value %))) during-reads)
                        (= expected coverage) (every? #(< (:index finish) (:index %)) reads)
                        (every? (fn [{[k v] :value}] (= (seeds k) v))
                                (filter #(= :read (:f %)) ok))))
         :leader leader :paused-voter victim :stopped-state state
         :job (:job before) :scan-ts (:scan-ts before) :before (count (:completed before))
         :while-stopped (count (:completed during))
         :last-checkpoint (when (:plan last-sample)
                            {:phase (:phase last-sample) :completed (count (:completed last-sample))
                             :total (count (:plan last-sample)) :preflight-done (:done last-sample)
                             :capability-regressed (:capability-regressed last-sample)})
         :acknowledged-seeds written :successful-final-reads (count reads)
         :missing-final-reads (set/difference expected coverage)
         :errors (vec (keep #(when (and (= :nemesis (:process %)) (:error %))
                               (select-keys % [:f :error :details])) history))}))))

(def online-seeds {0 4101, 1 4202, 2 4303})
(def captured-mutations #{[:write [1 5101]] [:delete [2 nil]]})
(def post-cutover-mutations #{[:write [3 5303]]})
(def online-final-values {0 4101, 1 5101, 2 nil, 3 5303})

(defn- acknowledged-between? [history tag expected lower upper]
  (let [completed (filter #(and (= tag (:tag %)) (not= :invoke (:type %))) history)]
    (and (= (count expected) (count completed))
         (= expected (set (map (juxt :f :value) completed)))
         (every? #(and (= :ok (:type %)) (< lower (:index %) upper)) completed))))

(defn online-failover-checker []
  (reify checker/Checker
    (check [_ _ history _]
      (let [[start fault heal finish :as events] (filter :confirmed history)
            {initial :sample old :leader old-term :term} (:value start)
            {:keys [before during leader victim state term]} (:value fault)
            {:keys [final routes]} (:value finish)
            final-reads (filter #(and (= :read (:f %)) (:final %)
                                      (not= :invoke (:type %))) history)
            expected (set (for [n nodes [k v] online-final-values] [n [k v]]))
            actual (set (map (juxt :node :value) final-reads))
            post-writes (filter #(and (= :post-cutover (:tag %)) (= :ok (:type %))) history)
            errors (vec (filter #(and (= :nemesis (:process %)) (:error %)) history))]
        {:valid? (boolean
                   (and (empty? errors)
                        (= [:start-online :failover :resume-voter :finish-split] (mapv :f events))
                        (apply < (map :index events))
                        (every? online-checkpoint? [initial before during])
                        (= (:job initial) (:job before) (:job during) (:job final))
                        (= (:scan-ts initial) (:scan-ts before))
                        (= (:plan initial) (:plan before))
                        (= (:online-manifest initial) (:online-manifest before) (:online-manifest during))
                        (set/subset? (:completed initial) (:completed before))
                        (= old victim (get-in heal [:value :victim]))
                        (contains? (set nodes) old) (contains? (set nodes) leader) (not= old leader)
                        (integer? old-term) (integer? term) (> term old-term)
                        (stopped? state) (not (str/blank? (get-in heal [:value :state])))
                        (not (stopped? (get-in heal [:value :state])))
                        (valid-checkpoints? before during final) (correct-routes? routes)
                        (= 1 (:online-version final))
                        (= (:online-sequence final) (:seal-sequence final))
                        (some-> (:seal-sequence final) parse-long pos?)
                        (acknowledged-between? history :seed
                                               (set (map (fn [kv] [:write (vec kv)]) online-seeds))
                                               -1 (:index start))
                        (acknowledged-between? history :captured captured-mutations
                                               (:index start) (:index fault))
                        (acknowledged-between? history :post-cutover post-cutover-mutations
                                               (:index finish) Long/MAX_VALUE)
                        (= expected actual) (= (count expected) (count final-reads))
                        (every? #(and (= :ok (:type %))
                                      (< (:index (first post-writes)) (:index %))) final-reads)))
         :old-leader old :new-leader leader :old-term old-term :new-term term
         :job (:job initial) :sealed-sequence (:seal-sequence final)
         :completed-before (count (:completed before)) :completed-after (count (:completed during))
         :final-reads (count final-reads)
         :missing-or-incorrect-final-values (set/difference expected actual)
         :errors errors}))))

(defn- probe-reads [tag keys-to-read]
  (apply gen/any
         (for [i (range (count nodes))]
           (gen/on-threads #{i} (mapv (fn [k] {:f :read :value (independent/tuple k nil) tag true})
                                      keys-to-read)))))

(defn- mutation-phase [tag entries]
  (gen/clients
    (map (fn [[f [k v]]] {:f f :value (independent/tuple k v) :tag tag}) entries)))

(defn- workload [{:keys [online-leader-failover]}]
  (let [client (grpc/->GRPCClient ports :raw (path "jepsen-grpc-client") nil nil nil)
        event #(gen/nemesis (gen/once {:type :info :f %}))]
    (merge tests/noop-test
           {:name (if online-leader-failover "elastickv-split-online-failover"
                                             "elastickv-split-preflight-pause")
            :nodes nodes :concurrency 3 :net net/noop
            :grpc-host "127.0.0.1" :grpc-tls-ca-file (path "cert.pem")
            :client (hosted/->ObservedClient client nil) :nemesis (preflight-nemesis)
            :checker (if online-leader-failover (online-failover-checker) (recovery-checker))
            :generator (if online-leader-failover
                         (gen/phases
                           (mutation-phase :seed (map #(vector :write %) online-seeds))
                           (event :start-online)
                           (mutation-phase :captured captured-mutations)
                           (event :failover) (event :resume-voter) (event :finish-split)
                           (mutation-phase :post-cutover post-cutover-mutations)
                           (gen/clients (probe-reads :final (keys online-final-values))))
                         (gen/phases
                           (mutation-phase :seed (map #(vector :write %) seeds))
                           (event :pause-preflight) (gen/clients (probe-reads :during (keys seeds)))
                           (event :resume-voter) (event :finish-split)
                           (gen/clients (probe-reads :final (keys seeds)))))})))

(defn -main [& args]
  (cli/run-workload! args
                     (conj cli/common-cli-opts
                           [nil "--online-leader-failover" "Stop the catalog leader during online capture."])
                     #(cli/parse-common-opts % nil) workload))
