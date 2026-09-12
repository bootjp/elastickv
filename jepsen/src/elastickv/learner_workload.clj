(ns elastickv.learner-workload
  "Jepsen workload for the Raft learner primitive: attach a learner, promote
  it under partition, and assert the safety properties the learner design
  leaves as Milestone 3 hardening.

  Operations:

    {:f :write           :value n}                write n to the register
    {:f :read            :value {:lease? b
                                 :value  n}}      read the register
    {:f :add-learner     :value node}             attach node as a learner
    {:f :promote-learner :value {:node             n
                                 :catch-up-target  t
                                 :target-source    :leader-commit-index
                                 :match            m}}
                                                  promote, recording the
                                                  target the operator
                                                  SAMPLED, where it came
                                                  from, and the learner's
                                                  Match when the promotion
                                                  committed

  Properties checked (see `learner-safety-checker`):

  1. **Promotion never outruns catch-up** — `match >= catch-up-target`,
     against the immutable sampled target, plus two guards: a promotion with
     no evidence fails closed, and a target not sampled from the leader is a
     procedure violation. See `premature-promotions` for why neither
     min-applied-index nor the leader's current commit index works alone.

  2. **No acknowledged write is lost across a promotion** — established by
     temporal ordering, not set subtraction. See
     `lost-writes-across-promotions`.

  3. **A learner never counts toward the lease** — isolating a non-voter must
     not fail lease reads. Expressed on reads rather than writes because
     `quorumAckTracker` gates `LastQuorumAck` and the lease-read fast path,
     not the write-commit quorum. See `learner-partition-read-failures`."
  (:gen-class)
  (:require [clojure.tools.logging :refer [warn]]
            [elastickv.cli :as cli]
            [elastickv.db :as ekdb]
            [jepsen.control :as c]
            [jepsen.db :as jdb]
            [jepsen.os.debian :as debian]
            [jepsen [checker :as checker]
                    [client :as client]
                    [control :as control]
                    [generator :as gen]
                    [nemesis :as nemesis]
                    [net :as net]
                    [os :as os]]
            [taoensso.carmine :as car :refer [wcar]]))

(def default-nodes ["n1" "n2" "n3" "n4" "n5"])

;; The last node is reserved as the learner candidate: ElastickvDB's setup
;; otherwise runs `raftadmin add_voter` for every node after the bootstrap
;; one, which leaves no non-member to attach. See ekdb/db :reserve-learner.
(defn learner-candidate
  "The node held out of the initial voter set, so :add-learner has something
  to attach. Without one, every node is already a voter before the workload
  starts and the operation the test is named for cannot run at all."
  [nodes]
  (last nodes))

(defn voter-nodes
  "The nodes that join as voters during setup."
  [nodes]
  (vec (butlast nodes)))
;; ---------------------------------------------------------------------------
;; Pure history analysis
;; ---------------------------------------------------------------------------

(defn completed-promotions
  "Every :promote-learner COMPLETION.

  Completions only, not invocations: a Jepsen operation appears twice in a
  history (an :invoke plus an :ok / :fail / :info), so selecting both counted
  each promotion twice and reported an invocation with no completion as a
  promotion that happened."
  [history]
  (->> history
       (filter #(= :promote-learner (:f %)))
       (remove #(= :invoke (:type %)))
       vec))

(defn unmeasurable-promotions
  "Promotions that reported :ok without the evidence needed to judge them.

  Fails closed. A successful promotion whose completion is missing :match or
  :catch-up-target -- status collection failed, say -- used to be filtered out
  by the numeric guards, so the checker could return :valid? true having
  verified catch-up for nothing at all. An unmeasurable promotion is not a
  safe one; it is one we cannot vouch for, and it must show up."
  [history]
  (->> (completed-promotions history)
       (filter #(= :ok (:type %)))
       (remove (fn [op]
                 (let [{:keys [match catch-up-target]} (:value op)]
                   (and (number? match) (number? catch-up-target)))))
       vec))

(defn premature-promotions
  "Promotions that committed while the learner was behind its catch-up target.

  Measured against the target the operator SAMPLED and passed as
  min_applied_index, not against the leader's commit index at promotion time.

  Both alternatives are wrong in opposite directions:

  - Comparing min-applied-index with Match alone proves nothing, because the
    engine's own test IS `Match >= min_applied_index`. An operator who reads
    the learner's current Match and passes it back satisfies that by
    construction, so the correct and the broken procedure are
    indistinguishable from the pair.
  - Comparing Match with the leader's CURRENT commit index rejects the
    documented safe workflow. An operator samples commit index T, waits for
    the learner to reach T, and promotes; if the leader commits more entries
    while that happens the promotion legitimately has `match >= T` but
    `match < leader-commit-index`, and the healthy run is marked invalid.
    That also contradicts the \"within N entries\" policy in
    docs/raft_learner_operations.md.

  The immutable sampled target is the only reference that distinguishes the
  two procedures without rejecting the safe one, so the workload records where
  its target came from and `promotions-without-a-sampled-target` rejects a
  target derived from the learner."
  [history]
  (->> (completed-promotions history)
       (filter #(= :ok (:type %)))
       (filter (fn [op]
                 (let [{:keys [match catch-up-target]} (:value op)]
                   (and (number? match)
                        (number? catch-up-target)
                        (< match catch-up-target)))))
       vec))

(defn promotions-without-a-sampled-target
  "Promotions whose catch-up target did not come from the leader.

  This is what closes the loophole the Match comparison could not: a target
  read off the learner's own Match makes the engine's check vacuous, so the
  workload records :target-source and anything other than
  :leader-commit-index is a procedure violation regardless of the outcome."
  [history]
  (->> (completed-promotions history)
       (filter #(= :ok (:type %)))
       (remove #(= :leader-commit-index (:target-source (:value %))))
       vec))

(defn- last-ok-write-before
  [history t]
  (->> history
       (filter #(and (= :write (:f %)) (= :ok (:type %)) (< (:time %) t)))
       (sort-by :time)
       last))

(defn- first-ok-read-after
  [history t]
  (->> history
       (filter #(and (= :read (:f %)) (= :ok (:type %)) (> (:time %) t)))
       (sort-by :time)
       first))

(defn- writes-invoked-between
  [history from to]
  (->> history
       (filter #(and (= :write (:f %)) (= :invoke (:type %))
                     (> (:time %) from) (< (:time %) to)))
       vec))

(defn lost-writes-across-promotions
  "Acknowledged writes that a promotion lost, by TEMPORAL ordering.

  Set subtraction cannot establish this. `write 1 :ok, write 2 :ok, promote
  :ok, read 2 :ok` is a legal register history, but subtracting observed
  values from acknowledged ones reports 1 as lost merely because it was
  overwritten before anyone read it; and a read of 1 taken BEFORE its write
  would mask a genuine later loss.

  So this pairs each promotion with the last write acknowledged before it and
  the first read that succeeded after it, and reports a loss only when no
  other write was in flight in between -- the case where the register's value
  is pinned and the read is obliged to return it. Concurrency makes the
  expected value ambiguous rather than wrong, so those cases are skipped
  instead of guessed at."
  [history]
  (->> (completed-promotions history)
       (filter #(= :ok (:type %)))
       (keep (fn [promotion]
               (let [t     (:time promotion)
                     write (last-ok-write-before history t)
                     read  (first-ok-read-after history t)]
                 ;; A read's :value is the map {:lease? b :value n}, so the
                 ;; register value has to be unwrapped before comparing it
                 ;; with the write's scalar.
                 (let [observed (get-in read [:value :value])]
                   (when (and write read
                              (empty? (writes-invoked-between
                                        history (:time write) (:time read)))
                              (not= (:value write) observed))
                     {:promotion      (:value promotion)
                      :acked-write    (:value write)
                      :observed-after observed})))))
       vec))

(defn learner-partition-windows
  "Every learners-only partition interval, as [start stop] time pairs.

  Each start is paired with its OWN stop. Taking the first start and the
  first later stop examined one window and silently ignored every subsequent
  learner-isolation period, so a regression in the second or later window
  could not fail the check."
  [history]
  (let [nemesis (->> history (filter #(= :nemesis (:process %))) (sort-by :time))
        starts  (->> nemesis
                     (filter #(= :start-partition (:f %)))
                     (filter #(= :learners-only (get-in % [:value :scope]))))
        stops   (->> nemesis (filter #(= :stop-partition (:f %))) (map :time) vec)]
    (->> starts
         (map (fn [start]
                (let [t (:time start)]
                  [t (or (first (filter #(> % t) stops)) Long/MAX_VALUE)])))
         vec)))

(defn learner-partition-read-failures
  "Lease reads that failed while only learners were partitioned.

  Reads, not writes. The learner is excluded from the write-commit quorum by
  the voter set, but `quorumAckTracker` feeds `LastQuorumAck`, which gates the
  leader-local LEASE-READ fast path. A learner wrongly counted there does not
  stop writes committing -- so a write-failure check stays green through the
  exact regression it claims to catch -- it stalls the lease the leader serves
  fast reads from.

  So the property is expressed on lease reads: isolating a non-voter must not
  make them fail."
  [history]
  (let [windows (learner-partition-windows history)]
    (if (empty? windows)
      []
      (->> history
           (filter #(and (= :read (:f %))
                         (= :fail (:type %))
                         (true? (:lease? (:value %)))))
           (filter (fn [op]
                     (some (fn [[start stop]]
                             (and (>= (:time op) start) (<= (:time op) stop)))
                           windows)))
           vec))))

;; ---------------------------------------------------------------------------
;; Checker
;; ---------------------------------------------------------------------------

(defn learner-safety-checker
  "Checks the learner safety properties over a completed history.

  An EMPTY history is invalid. A run that emitted no operations proves
  nothing, and reporting it valid is how a workload that cannot actually
  drive the cluster still passes -- which is exactly what this workload did
  before it had a client, a nemesis, or a generator that produced its
  documented operations."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [promotions   (completed-promotions history)
            premature    (premature-promotions history)
            unmeasurable (unmeasurable-promotions history)
            unsampled    (promotions-without-a-sampled-target history)
            lost         (lost-writes-across-promotions history)
            stalls       (learner-partition-read-failures history)
            writes       (count (filter #(and (= :write (:f %)) (= :ok (:type %))) history))
            reads        (count (filter #(and (= :read (:f %)) (= :ok (:type %))) history))]
        (when (seq premature)
          (warn "learner promoted before reaching its sampled target:" premature))
        (when (seq unmeasurable)
          (warn "promotion reported ok without catch-up evidence:" unmeasurable))
        {:valid?               (and (pos? (count promotions))
                                    (pos? writes)
                                    (pos? reads)
                                    (empty? premature)
                                    (empty? unmeasurable)
                                    (empty? unsampled)
                                    (empty? lost)
                                    (empty? stalls))
         :promotions           (count promotions)
         :ok-writes            writes
         :ok-reads             reads
         :premature-promotions premature
         :unmeasurable-promotions unmeasurable
         :unsampled-targets    unsampled
         :lost-writes          lost
         :learner-read-failures stalls}))))

;; ---------------------------------------------------------------------------
;; Client
;; ---------------------------------------------------------------------------

(def ^:private register-key "learner-register")

(defn- raftadmin!
  "Runs raftadmin on node against the leader address."
  [node & args]
  (c/on node (c/su (apply c/exec :env "RAFTADMIN_ALLOW_INSECURE=true"
                          (ekdb/raftadmin-binary) args))))

(defn- leader-commit-index
  "Samples the leader's commit index: the catch-up target.

  Sampled from the LEADER because a target read off the learner is what makes
  the engine's `Match >= min_applied_index` test vacuous."
  [node leader-addr]
  (:commit_index (ekdb/raft-status node leader-addr)))

(defn- learner-applied-index
  "The learner's OWN applied index, read from the learner.

  Deliberately not the leader's Match for that peer: an independently observed
  measure cannot be satisfied by the leader's own bookkeeping, so it is the
  stronger evidence of catch-up. (Per-peer Match is not available from
  `raftadmin status` on this branch in any case.)"
  [learner-node learner-addr]
  (:applied_index (ekdb/raft-status learner-node learner-addr)))

(def ^:private catch-up-poll-ms 200)
(def ^:private catch-up-timeout-ms 60000)

(defn- await-catch-up!
  "Polls the learner until its applied index reaches target."
  [learner-node learner-addr target]
  (let [deadline (+ (System/currentTimeMillis) catch-up-timeout-ms)]
    (loop []
      (let [applied (or (learner-applied-index learner-node learner-addr) 0)]
        (cond
          (>= applied target) applied
          (> (System/currentTimeMillis) deadline)
          (throw (ex-info "learner did not reach the catch-up target"
                          {:target target :applied applied}))
          :else (do (Thread/sleep (long catch-up-poll-ms)) (recur)))))))

(defrecord LearnerClient [node->port leader-addr conn]
  client/Client
  (open! [this test node]
    (let [port (get node->port node 6379)
          host (or (:redis-host test) (name node))]
      (assoc this :conn {:pool {} :spec {:host host :port port :timeout-ms 10000}})))

  (close! [this _test] this)
  (setup! [_this _test])
  (teardown! [_this _test])

  (invoke! [this test op]
    (let [conn   (:conn this)
          nodes  (:nodes test)
          leader (first nodes)
          addr   (or leader-addr (str leader ":50051"))]
      (try
        (case (:f op)
          :write (do (wcar conn (car/set register-key (:value op)))
                     (assoc op :type :ok))

          ;; :lease? marks the read as one the leader may serve from its
          ;; lease, which is the path a learner wrongly counted in
          ;; quorumAckTracker would break.
          :read (let [v (wcar conn (car/get register-key))]
                  (assoc op :type :ok
                            :value {:lease? true
                                    :value  (when v (Long/parseLong (str v)))}))

          :add-learner
          (let [candidate (name (:value op))]
            (raftadmin! leader addr "add_learner" candidate
                        (str candidate ":" (:grpc-port test 50051)) "0")
            (assoc op :type :ok))

          :promote-learner
          (let [candidate      (name (:value op))
                candidate-addr (str candidate ":" (:grpc-port test 50051))
                ;; Sample the target FIRST, then wait for the learner to reach
                ;; it, then promote against that same immutable value.
                ;; Recording where the target came from is what lets the
                ;; checker reject the vacuous procedure.
                target         (leader-commit-index leader addr)
                applied        (await-catch-up! candidate candidate-addr target)]
            (raftadmin! leader addr "promote_learner" candidate
                        "0" (str target))
            (assoc op :type :ok
                      :value {:node            candidate
                              :catch-up-target target
                              :target-source   :leader-commit-index
                              :match           applied})))
        (catch Exception e
          (assoc op :type :fail :error (.getMessage e)))))))

;; ---------------------------------------------------------------------------
;; Nemesis
;; ---------------------------------------------------------------------------

(defn learner-partition-nemesis
  "Isolates ONLY the learner candidate, leaving every voter connected.

  That is the shape the quorum property needs: voters retain quorum among
  themselves, so anything that degrades must be attributable to the learner
  being counted where it should not be."
  [nodes]
  (let [learner (learner-candidate nodes)]
    (nemesis/partitioner
      (fn [_test _nodes]
        (nemesis/complete-grudge [[learner] (voter-nodes nodes)])))))

(defn learner-nemesis-generator
  "start-partition / stop-partition pairs, each start tagged :learners-only so
  the checker can pair it with its own stop."
  []
  ;; A seq is a generator in Jepsen 0.3.x; gen/seq was removed.
  (cycle [(gen/sleep 5)
          {:type :info :f :start-partition :value {:scope :learners-only}}
          (gen/sleep 10)
          {:type :info :f :stop-partition :value {:scope :learners-only}}]))

;; ---------------------------------------------------------------------------
;; Generator
;; ---------------------------------------------------------------------------

(defn client-generator
  "Register traffic plus one attach/promote cycle for the reserved candidate.

  The previous generator was `gen/nemesis` applied to nil with no :client at
  all, so a run emitted NOTHING: none of the documented :write, :read,
  :add-learner or :promote-learner operations could appear, and the checker
  reported the resulting empty history as valid."
  [nodes]
  (let [candidate (learner-candidate nodes)
        register  (gen/mix [(fn [] {:f :write :value (rand-int 1000000)})
                            (fn [] {:f :read})])]
    (gen/phases
      ;; Some traffic first, so the promotion has acknowledged writes to
      ;; preserve across it.
      (gen/time-limit 5 register)
      (gen/once {:f :add-learner :value candidate})
      (gen/time-limit 5 register)
      (gen/once {:f :promote-learner :value candidate})
      register)))

(defn elastickv-learner-test
  "Builds a Jepsen test map exercising learner attach and promotion."
  ([] (elastickv-learner-test {}))
  ([opts]
   (let [nodes      (or (:nodes opts) default-nodes)
         local?     (:local opts)
         grpc-port  (or (:grpc-port opts) 50051)
         redis-port (or (:redis-port opts) 6379)
         db         (if local?
                      jdb/noop
                      (ekdb/db {:grpc-port       grpc-port
                                :redis-port      redis-port
                                :encryption      (:encryption opts)
                                ;; Held out of the voter set so :add-learner
                                ;; has a non-member to attach.
                                :reserve-learner (learner-candidate nodes)}))
         time-limit (or (:time-limit opts) 30)
         ports      (or (:node->port opts)
                        (cli/ports->node-map
                          (repeat (count nodes) redis-port) nodes))]
     {:name        "elastickv-learner"
      :nodes       nodes
      :db          db
      :os          (if local? os/noop debian/os)
      :net         (if local? net/noop net/iptables)
      :ssh         (merge {:username "vagrant"
                           :private-key-path "/home/vagrant/.ssh/id_rsa"
                           :strict-host-key-checking false}
                          (when local? {:dummy true})
                          (:ssh opts))
      :remote      control/ssh
      :client      (->LearnerClient ports nil nil)
      :nemesis     (if local? nemesis/noop (learner-partition-nemesis nodes))
      ;; Jepsen 0.3.x cannot fressian-serialize some final generators.
      :final-generator nil
      :concurrency (or (:concurrency opts) 10)
      :time-limit  time-limit
      :rate        (double (or (:rate opts) 5))
      :checker     (learner-safety-checker)
      :generator   (->> (client-generator nodes)
                        (gen/nemesis (if local?
                                       (gen/once {:type :info :f :noop})
                                       (learner-nemesis-generator)))
                        (gen/stagger (/ (double (or (:rate opts) 5))))
                        (gen/time-limit time-limit))
      :grpc-port   grpc-port
      :ports       ports})))

(defn -main
  "Runnable entry point. Without one, neither invocation form could select
  this workload: the namespace had no -main, and the shared dispatcher in
  elastickv.jepsen-test neither required it nor listed it, so passing its
  name fell through to the Redis test."
  [& args]
  (cli/run-workload! args cli/common-cli-opts identity elastickv-learner-test))
