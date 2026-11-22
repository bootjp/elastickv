(ns elastickv.split-preflight-workload
  "Exercise catalog preflight and online split recovery with a stopped voter."
  (:refer-clojure :exclude [run!])
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
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
     :cutover-version (:cutover_version j) :target-promoted (:target_promotion_done j)
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

(defn- follower-or-leader-workload [{:keys [online-leader-failover]}]
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

(def membership-nodes (conj nodes "n4"))
(def membership-ports (assoc ports "n4" 25104))

(defn- group-address [group node]
  (str "127.0.0.1:" (+ (membership-ports node) (* 100 (dec group)))))

(defn- group-admin [group node & args]
  (apply run! (path "raftadmin") (group-address group node) args))

(defn- group-status [group node]
  (db/parse-raft-status (group-admin group node "status")))

(defn- poll! [seconds message f]
  (let [deadline (+ (System/nanoTime) (* seconds 1000000000))]
    (loop []
      (or (f)
          (if (< (System/nanoTime) deadline)
            (do (Thread/sleep 100) (recur))
            (throw (ex-info message {})))))))

(defn- leader! [group]
  (poll! 20 (str "No leader in group " group)
    #(some (fn [node]
             (let [s (group-status group node)]
               (when (= "LEADER" (:state s)) (assoc s :node node))))
           nodes)))

(defn- candidate-role [group]
  (some-> (re-find #"(?s)id: \"n4\"\s+address: \"[^\"]+\"\s+suffrage: \"([^\"]+)\""
                   (group-admin group "n4" "configuration"))
          second str/lower-case))

(defn- wait-candidate! [group min-index role?]
  (poll! 35 (str "Candidate did not apply the expected state for group " group)
    #(let [s (group-status group "n4")]
       (when (and (>= (:applied_index s) min-index)
                  (>= (:configuration_index s) min-index)
                  (role? (candidate-role group)))
         (assoc s :role (candidate-role group))))))

(defn- change-index [output]
  (or (some-> (re-find #"index:\s*(\d+)" output) second parse-long)
      (throw (ex-info "Raft admin did not return an index" {:output output}))))

(defn- add-test-learner! [group leader]
  (change-index (group-admin group (:node leader) "add_learner" "n4"
                             (group-address group "n4") (str (:configuration_index leader)))))

(defn- transfer-test-leader! [group leader node]
  (when-not (= node (:node leader))
    (group-admin group (:node leader) "leadership_transfer_to_server" node
                 (group-address group node)))
  (poll! 25 (str node " did not become leader of group " group)
    #(let [s (group-status group node)]
       (when (and (= "LEADER" (:state s)) (= node (:leader_id s))) s))))

(defn- prepare-membership! []
  (let [leader (leader! 1)
        added (add-test-learner! 1 leader)
        _ (wait-candidate! 1 added #{"learner" "nonvoter"})
        status (transfer-test-leader! 1 (leader! 1) "n1")
        target (transfer-test-leader! 2 (leader! 2) "n3")]
    {:catalog-learner-index added :catalog-leader (:leader_id status)
     :initial-target-leader (:leader_id target)}))

(defn- fixture-ints [file expected]
  (let [f (io/file (path file))]
    (when (.exists f)
      (let [fields (mapv parse-long (str/split (str/trim (slurp f)) #"\s+"))]
        (when (and (= expected (count fields)) (every? number? fields)) fields)))))

(defn- moving-owner [catalog]
  (let [boundary (.encodeToString (java.util.Base64/getEncoder) (.getBytes split-key "UTF-8"))]
    (:raft_group_id (some #(when (= boundary (:start %)) %) (:routes catalog)))))

(defn- add-and-promote! [active]
  (let [{:keys [leader job]} @active
        [gate-job old-version certified seal nonce :as boundary]
        (poll! 150 "No final-confirmation checkpoint" #(fixture-ints "before-publish" 5))
        previous (leader! 2)
        before (sample leader job)
        catalog (routes leader)]
    (when-not (and (= (parse-long job) gate-job) (= leader "n1")
                   (= certified (:configuration_index previous)) (pos? seal)
                   (= "SPLIT_JOB_PHASE_CUTOVER" (:phase before))
                   (= old-version (:catalog_version catalog)) (= 1 (moving-owner catalog)))
      (throw (ex-info "Membership fault missed the intended cutover checkpoint"
                      {:checkpoint [gate-job old-version certified seal] :job before :catalog catalog})))
    (spit (path "hold-n3") (str old-version))
    (let [added (add-test-learner! 2 previous)
          caught (wait-candidate! 2 added #{"learner" "nonvoter"})
          target-leader (leader! 2)
          threshold (:commit_index target-leader)
          caught-up (poll! 35 "Learner did not reach the target leader commit index"
                      #(let [status (group-status 2 "n4")]
                         (when (>= (:applied_index status) threshold) (:applied_index status))))
          promoted (change-index (group-admin 2 (:node target-leader) "promote_learner" "n4"
                                              (str added) (str threshold)))
          voter (wait-candidate! 2 promoted #{"voter"})
          after (sample leader job)
          unchanged (routes leader)]
      (when-not (and (= "SPLIT_JOB_PHASE_CUTOVER" (:phase after))
                     (= old-version (:catalog_version unchanged)) (= 1 (moving-owner unchanged))
                     (< certified added promoted) (>= (:applied_index voter) promoted))
        (throw (ex-info "Target promotion was not applied before publication"
                        {:after after :catalog unchanged :voter voter})))
      (swap! active assoc :old-version old-version :boundary boundary)
      (spit (path "release-publish") "continue")
      {:job job :certified certified :add-index added :promote-index promoted
       :learner-role (:role caught) :voter-role (:role voter)
       :threshold threshold :learner-applied caught-up :voter-applied (:applied_index voter)
       :old-version old-version :seal seal :attempt nonce :before-owner 1 :after-promotion-owner 1})))

(defn- await-membership-publication! [active]
  (let [{:keys [leader job old-version boundary]} @active
        published (await-sample leader job 120 100
                    #(and (contains? #{"SPLIT_JOB_PHASE_CLEANUP" "SPLIT_JOB_PHASE_DONE"} (:phase %))
                          (:target-promoted %)))
        version (parse-long (:cutover-version published))
        catalog (routes leader)
        attempt (poll! 15 "Cutover did not commit on the paused attempt"
                  #(fixture-ints "published-attempt" 5))
        copied (poll! 30 "New member did not apply the published route"
                 #(when-let [[v] (fixture-ints "caught-n4" 1)] (when (>= v version) v)))
        [held old-owner offered] (poll! 30 "Old ingress did not reject the catalog update"
                                  #(fixture-ints "held-n3" 3))]
    (when-not (and (= attempt boundary)
                   (< old-version version) (<= version (:catalog_version catalog))
                   (= 2 (moving-owner catalog)) (<= held old-version)
                   (= old-owner 1) (< old-version offered))
      (throw (ex-info "Did not observe new owner and held old ingress together"
                      {:job published :held held :old-owner old-owner :catalog catalog})))
    (swap! active assoc :published-version version)
    {:version version :old-ingress "n3" :cached-version held
     :old-owner old-owner :new-owner 2 :blocked-version offered :job job
     :attempt (last attempt) :new-member-version copied
     :initial-target-leader (:node (leader! 2))}))

(defn- lead-new-member! []
  (let [leader (leader! 2)]
    (when-not (= "n3" (:node leader))
      (throw (ex-info "The stale ingress no longer leads the target group" leader)))
    (let [target (transfer-test-leader! 2 leader "n4")]
      {:leader (:leader_id target) :target-applied (:applied_index target)})))

(defn- heal-membership! [active]
  (io/delete-file (path "hold-n3"))
  (let [{:keys [published-version]} @active
        finished (finish! @active)
        caught (poll! 35 "Old ingress did not catch up after healing"
                 #(when-let [[v] (fixture-ints "caught-n3" 1)]
                    (when (>= v published-version) v)))
        target (group-status 2 "n4")]
    (when-not (= "LEADER" (:state target))
      (throw (ex-info "New member lost leadership before the final audit" target)))
    (assoc finished :healed-version caught
           :target-leader (:leader_id target) :target-applied (:applied_index target))))

(defn- membership-nemesis []
  (let [active (atom nil)]
    (reify nemesis/Nemesis
      (setup! [this _] this)
      (invoke! [_ _ op]
        (try
          (assoc op :confirmed true :value
                 (case (:f op)
                   :prepare-member (prepare-membership!)
                   :start-online (start-online! active)
                   :promote-before-publish (add-and-promote! active)
                   :await-publication (await-membership-publication! active)
                   :lead-new-member (lead-new-member!)
                   :heal-member-views (heal-membership! active)))
          (catch Exception e
            (assoc op :error (.getMessage e) :details (ex-data e)))))
      (teardown! [_ _]
        (io/delete-file (path "hold-n3") true)
        (spit (path "release-publish") "cleanup")))))

(defn- member-reads [tag threads register-keys]
  (apply gen/any
    (for [thread threads]
      (gen/on-threads #{thread}
        (mapv (fn [k] {:f :read :tag tag :value (independent/tuple k nil)}) register-keys)))))

(defn- safe-stale-read? [{:keys [type value error]}]
  (let [[key got] value]
    (case type
      :ok (= (online-final-values key) got)
      :info (boolean (re-find #"code = Unavailable desc = Route unavailable\b" (str error)))
      false)))

(defn membership-checker []
  (reify checker/Checker
    (check [_ _ history _]
      (let [[prepare start change publish lead heal :as events] (filter :confirmed history)
            m (:value change) p (:value publish) l (:value lead) h (:value heal)
            completed (fn [tag] (filter #(and (= tag (:tag %)) (not= :invoke (:type %))) history))
            stale (completed :stale)
            new-while-stale (completed :new-while-stale)
            joined-leader (completed :joined-leader)
            final (completed :member-read)
            expected (set (for [node ["n3" "n4"] [k v] online-final-values] [node [k v]]))
            actual (set (map (juxt :node :value) final))
            errors (vec (filter #(and (= :nemesis (:process %)) (:error %)) history))]
        {:valid? (boolean
                   (and (empty? errors)
                        (= [:prepare-member :start-online :promote-before-publish
                            :await-publication :lead-new-member :heal-member-views] (mapv :f events))
                        (apply < (map :index events))
                        (= "n1" (get-in prepare [:value :catalog-leader]))
                        (= "n3" (get-in prepare [:value :initial-target-leader]) (:initial-target-leader p))
                        (online-checkpoint? (get-in start [:value :sample]))
                        (= "n1" (get-in start [:value :leader]))
                        (= (:job m) (:job p) (get-in start [:value :sample :job]) (get-in h [:final :job]))
                        (= 1 (:before-owner m) (:after-promotion-owner m) (:old-owner p))
                        (< (:certified m) (:add-index m) (:promote-index m))
                        (contains? #{"learner" "nonvoter"} (:learner-role m))
                        (= "voter" (:voter-role m))
                        (<= (:add-index m) (:threshold m) (:learner-applied m))
                        (<= (:promote-index m) (:voter-applied m) (:target-applied l) (:target-applied h))
                        (= (:attempt m) (:attempt p)) (pos? (:attempt p))
                        (= 2 (:new-owner p)) (= "n3" (:old-ingress p))
                        (>= (:new-member-version p) (:version p))
                        (<= (:cached-version p) (:old-version m))
                        (< (:old-version m) (:blocked-version p))
                        (< (:old-version m) (:version p))
                        (>= (:healed-version h) (:version p))
                        (= "n4" (:leader l) (:target-leader h))
                        (= "SPLIT_JOB_PHASE_DONE" (get-in h [:final :phase]))
                        (= (:seal m) (parse-long (get-in h [:final :seal-sequence]))
                           (parse-long (get-in h [:final :online-sequence])))
                        (correct-routes? (:routes h))
                        (acknowledged-between? history :seed
                                               (set (map (fn [kv] [:write (vec kv)]) online-seeds))
                                               (:index prepare) (:index start))
                        (acknowledged-between? history :captured captured-mutations
                                               (:index start) (:index change))
                        (= 2 (count stale))
                        (= #{["n3" 1] ["n3" 2]} (set (map (juxt :node (comp first :value)) stale)))
                        (every? #(and (< (:index publish) (:index %) (:index lead))
                                      (safe-stale-read? %)) stale)
                        (= 2 (count new-while-stale))
                        (= #{["n4" 1] ["n4" 2]}
                           (set (map (juxt :node (comp first :value)) new-while-stale)))
                        (every? #(and (< (:index publish) (:index %) (:index lead))
                                      (safe-stale-read? %)) new-while-stale)
                        (= 2 (count joined-leader))
                        (= #{["n4" [1 5101]] ["n4" [2 nil]]}
                           (set (map (juxt :node :value) joined-leader)))
                        (every? #(and (= :ok (:type %)) (< (:index lead) (:index %) (:index heal)))
                                joined-leader)
                        (= (count expected) (count final)) (= expected actual)
                        (seq final)
                        (acknowledged-between? history :post-cutover post-cutover-mutations
                                               (:index heal) (apply min (map :index final)))
                        (every? #(= :ok (:type %)) final)))
         :membership m :publication p
         :stale-reads (mapv #(select-keys % [:node :type :value :error]) stale)
         :new-member-reads-while-stale (mapv #(select-keys % [:node :type :value :error]) new-while-stale)
         :joined-leader-reads (mapv #(select-keys % [:node :type :value]) joined-leader)
         :target-leader (:target-leader h)
         :final-reads (count final) :errors errors
         :incorrect-or-missing-final-values (set/difference expected actual)}))))

(defn- membership-workload []
  (let [client (grpc/->GRPCClient membership-ports :raw (path "jepsen-grpc-client") nil nil nil)
        event #(gen/nemesis (gen/once {:type :info :f %}))]
    (merge tests/noop-test
           {:name "elastickv-split-target-membership" :nodes membership-nodes :concurrency 4 :net net/noop
            :grpc-host "127.0.0.1" :grpc-tls-ca-file (path "cert.pem")
            :client (hosted/->ObservedClient client nil) :nemesis (membership-nemesis)
            :checker (membership-checker)
            :generator
            (gen/phases
              (event :prepare-member)
              (gen/on-threads #{0 1 2}
                (mutation-phase :seed (map #(vector :write %) online-seeds)))
              (event :start-online)
              (gen/on-threads #{0 1 2} (mutation-phase :captured captured-mutations))
              (event :promote-before-publish) (event :await-publication)
              (gen/clients (member-reads :stale [2] [1 2]))
              (gen/clients (member-reads :new-while-stale [3] [1 2]))
              (event :lead-new-member)
              (gen/clients (member-reads :joined-leader [3] [1 2]))
              (event :heal-member-views)
              (gen/on-threads #{3} (mutation-phase :post-cutover post-cutover-mutations))
              (gen/clients (member-reads :member-read [2 3] (keys online-final-values))))})))

(defn -main [& args]
  (cli/run-workload! args
                     (into cli/common-cli-opts
                           [[nil "--online-leader-failover" "Stop the catalog leader during online capture."]
                            [nil "--online-member-change" "Promote a target voter before catalog publication."]])
                     #(cli/parse-common-opts % nil)
                     #(if (:online-member-change %) (membership-workload)
                          (follower-or-leader-workload %))))
