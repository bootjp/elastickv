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

;; FOUR nodes, not five: three voters plus the reserved candidate.
;;
;; With five, the run starts at four voters and promotion takes it to five --
;; but majority(4) and majority(5) are both 3, and followerQuorumForClusterSize
;; is 2 for both, so the quorum-denominator transition this workload is named
;; for never actually happens and a bug that only appears when promotion raises
;; the threshold cannot surface. Three voters going to four moves the majority
;; from 2 to 3 and the follower quorum from 1 to 2.
(def default-nodes ["n1" "n2" "n3" "n4"])

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

(defn- raft-majority
  "floor(N/2)+1 -- the voters a proposal needs, including the leader."
  [n]
  (inc (quot n 2)))

(defn promotion-changes-quorum?
  "Whether promoting the candidate actually raises the Raft majority.

  This is the workload's reason to exist, and it is not true of every
  topology: four voters going to five needs three either way. A run on such a
  topology exercises the promotion mechanics but not the transition, so the
  constructor warns rather than letting a green result be read as evidence
  about quorum handling."
  [nodes]
  (let [voters (count (voter-nodes nodes))]
    (> (raft-majority (inc voters)) (raft-majority voters))))
;; ---------------------------------------------------------------------------
;; Pure history analysis
;; ---------------------------------------------------------------------------

(defn successful-promotions
  "Promotions that reported :ok.

  The coverage gate counts these, not every completion: a history with a
  :fail or :info promotion had a positive promotion count while every safety
  predicate -- which all filter to :ok -- saw nothing, so a run in which no
  learner was ever promoted was reported valid."
  [history]
  (->> history
       (filter #(and (= :promote-learner (:f %)) (= :ok (:type %))))
       vec))

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

(defn unenforced-min-applied-index
  "Deliberately premature promotions the server ACCEPTED.

  premature-promotions cannot fail on its own. The live workflow samples the
  target, waits for the learner to reach it, and only then promotes, recording
  that already-qualified applied index as :match -- so match >= target holds by
  construction and the property stays empty even if the engine dropped its
  min_applied_index test entirely.

  The only way to show the enforcement exists is to ask for something it must
  refuse. This op promotes with a min_applied_index far above the leader's
  commit index, which no learner can have reached, and expects rejection.
  A success here is the defect premature-promotions was meant to catch."
  [history]
  (->> history
       (filter #(and (= :promote-learner-early (:f %)) (= :ok (:type %))))
       vec))

(defn unattempted-min-applied-index-probe
  "True when the run never issued the premature-promotion probe at all.

  A probe that did not run is not evidence of enforcement, and an empty
  unenforced-min-applied-index would otherwise look identical to a passing
  one."
  [history]
  (empty? (filter #(and (= :promote-learner-early (:f %))
                        (contains? #{:ok :fail :info} (:type %)))
                  history)))

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

(defn- write-intervals
  "Each write as [invoke-time complete-time op], for overlap tests.

  A write still in flight has no completion; it is treated as running to the
  end of the history, because a write that never completed can still have
  taken effect."
  [history]
  (let [writes (->> history (filter #(= :write (:f %))) (sort-by :time))
        end    (or (some->> history (map :time) (reduce max)) 0)]
    (->> writes
         (reduce (fn [{:keys [pending done]} op]
                   (case (:type op)
                     :invoke {:pending (assoc pending (:process op) op)
                              :done    done}
                     (:ok :fail :info)
                     (if-let [inv (get pending (:process op))]
                       {:pending (dissoc pending (:process op))
                        :done    (conj done [(:time inv) (:time op) op])}
                       {:pending pending :done done})
                     {:pending pending :done done}))
                 {:pending {} :done []})
         ((fn [{:keys [pending done]}]
            (into done (map (fn [[_ inv]] [(:time inv) end inv])
                            pending)))))))

(defn- concurrent-writes
  "Writes whose execution interval overlaps [from to], excluding the write
  that defines it.

  Completion order alone does not identify a register's final value. Write 1
  running t=10..40 and write 2 running t=20..30 both linearize either way, so
  a later read of 2 is legal even though 1 completed last and no write was
  INVOKED after 40. Looking only at invocations in (from, to) misses write 2
  entirely and reports a loss that never happened.

  So the ambiguity test is overlap with the selected write's own interval, not
  just the gap between it and the read."
  [history from to selected]
  (->> (write-intervals history)
       (remove (fn [[_ _ op]] (identical? op selected)))
       (filter (fn [[start stop _]] (and (< start to) (> stop from))))
       (mapv (fn [[_ _ op]] op))))

(defn- last-ok-write-interval-before
  "The last write acknowledged before t, as its [invoke complete op] triple.

  The triple, not the op: the caller needs the write's own INVOCATION time to
  test what overlapped it, and it needs the identical op so that write can be
  excluded from its own overlap set. Returning a copy with the invocation time
  attached breaks the second -- the copy is not identical to the entry in the
  interval list, so the selected write counts as overlapping itself and every
  history looks ambiguous."
  [history t]
  (->> (write-intervals history)
       (filter (fn [[_ stop op]] (and (= :ok (:type op)) (< stop t))))
       (sort-by (fn [[_ stop _]] stop))
       last))

(defn- read-invocation-times
  "Each successful read paired with the time it was actually invoked.

  A process set is not enough. If process P invokes a read before t, completes
  it after t, and then invokes another read after t, P is in the set of
  processes that invoked after t -- so the EARLIER, pre-t read is selected as
  though it had been invoked after t. That admits exactly the stale evidence
  the invocation filter exists to exclude.

  Jepsen runs one operation at a time per process, so the invocation a
  completion belongs to is the last invoke on that process before it."
  [history]
  (let [reads (->> history
                   (filter #(= :read (:f %)))
                   (sort-by :time))]
    (->> reads
         (reduce (fn [{:keys [pending done]} op]
                   (case (:type op)
                     :invoke {:pending (assoc pending (:process op) (:time op))
                              :done    done}
                     (:ok :fail :info)
                     {:pending (dissoc pending (:process op))
                      :done    (if (and (= :ok (:type op))
                                        (contains? pending (:process op)))
                                 (conj done (assoc op ::invoked-at
                                                   (get pending (:process op))))
                                 done)}
                     {:pending pending :done done}))
                 {:pending {} :done []})
         :done)))

(defn- first-ok-read-invoked-after
  "The first successful read whose own INVOCATION happened after t.

  Invocation, not completion: with concurrent workers a read invoked before the
  promotion can linearize against the old value and return after it. Selecting
  on completion time picks up exactly that read and reports the acknowledged
  write as lost, so the pairing has to start from the invoke event -- and from
  THIS completion's invoke event, not from any invoke the same process made."
  [history t]
  (->> (read-invocation-times history)
       (filter #(> (::invoked-at %) t))
       (sort-by :time)
       first))

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
               (let [t        (:time promotion)
                     interval (last-ok-write-interval-before history t)
                     [w-start _ write] interval
                     read     (first-ok-read-invoked-after history t)]
                 ;; A read's :value is the map {:lease? b :value n}, so the
                 ;; register value has to be unwrapped before comparing it
                 ;; with the write's scalar.
                 (let [observed (get-in read [:value :value])]
                   (when (and write read
                              ;; From the selected write's own INVOCATION, so a
                              ;; write that overlapped it counts as ambiguity
                              ;; rather than being skipped.
                              (empty? (concurrent-writes
                                        history w-start (:time read) write))
                              (not= (:value write) observed))
                     {:promotion      (:value promotion)
                      :acked-write    (:value write)
                      :observed-after observed})))))
       vec))

(defn promotions-without-post-read-evidence
  "Successful promotions with no unambiguous successful read after them.

  lost-writes-across-promotions SKIPS a promotion it cannot pair with a later
  read, which happens when catch-up finishes near the time limit. Counting
  reads anywhere in the history hid that: the run passed having observed no
  post-promotion state at all."
  [history]
  (->> (successful-promotions history)
       (remove #(some? (first-ok-read-invoked-after history (:time %))))
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

(defn- partition-edge-counters
  "The {:hit n :miss n} samples the nemesis took at each window's edges."
  [history]
  (let [nemesis (->> history (filter #(= :nemesis (:process %))) (sort-by :time))
        starts  (->> nemesis
                     (filter #(and (= :start-partition (:f %))
                                   (= :learners-only (get-in % [:value :scope]))
                                   (= :info (:type %))))
                     vec)
        stops   (->> nemesis
                     (filter #(and (= :stop-partition (:f %)) (= :info (:type %))))
                     vec)]
    (->> starts
         (keep (fn [start]
                 (when-let [stop (->> stops
                                      (filter #(> (:time %) (:time start)))
                                      first)]
                   {:start start
                    :stop  stop
                    :before (get-in start [:value :lease-counters])
                    :after  (get-in stop  [:value :lease-counters])})))
         vec)))

(defn lease-fast-path-losses
  "Windows in which a lease read FELL BACK to the linearizable path.

  Neither failures nor latency can establish this. A Redis GET calls
  LeaseReadForKeyThrough, and when the lease is unavailable kv/raft_engine.go
  transparently falls back to LinearizableRead -- so a failure-only check stays
  empty through exactly the regression it claims to catch. Latency does not
  separate them either: LinearizableRead issues a ReadIndex immediately and,
  with every voter connected on one host, its quorum round trip completes in a
  few milliseconds. Any budget loose enough not to flag ordinary jitter is far
  above the fallback's actual cost, so a run can turn every GET into a
  successful slow-path read and still report valid.

  elastickv_lease_read_total separates them by construction: the metric's own
  help text defines miss as \"fell back to LinearizableRead\". So the property
  is a counter delta across the window, not a time."
  [history]
  (->> (partition-edge-counters history)
       (keep (fn [{:keys [before after start]}]
               (when (and before after)
                 (let [delta (- (:miss after) (:miss before))]
                   (when (pos? delta)
                     {:window-start (:time start)
                      :miss-delta   delta})))))
       vec))

(defn unmeasured-partition-windows
  "Windows that produced no evidence either way.

  Two ways a window proves nothing, and both used to read as a pass:

  - the counters could not be sampled at all (metrics endpoint unreachable),
    so zero misses is indistinguishable from no data;
  - the lease hit counter did not move, meaning no read was served from the
    leader's lease while the candidate was attached and isolated. The read
    coverage gate counted successful reads ANYWHERE in the run, so reads taken
    before attachment or after the heal satisfied it while the quorum-ack
    property was never exercised."
  [history]
  (->> (partition-edge-counters history)
       (keep (fn [{:keys [before after start]}]
               (cond
                 (or (nil? before) (nil? after))
                 {:window-start (:time start) :reason :counters-unavailable}

                 (not (pos? (- (:hit after) (:hit before))))
                 {:window-start (:time start) :reason :no-lease-read-served})))
       vec))

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
      (let [promotions   (successful-promotions history)
            premature    (premature-promotions history)
            unmeasurable (unmeasurable-promotions history)
            unsampled    (promotions-without-a-sampled-target history)
            no-evidence  (promotions-without-post-read-evidence history)
            lost         (lost-writes-across-promotions history)
            stalls       (learner-partition-read-failures history)
            fallbacks    (lease-fast-path-losses history)
            unmeasured   (unmeasured-partition-windows history)
            unenforced   (unenforced-min-applied-index history)
            no-probe     (unattempted-min-applied-index-probe history)
            writes       (count (filter #(and (= :write (:f %)) (= :ok (:type %))) history))
            reads        (count (filter #(and (= :read (:f %)) (= :ok (:type %))) history))]
        (when (seq premature)
          (warn "learner promoted before reaching its sampled target:" premature))
        (when (seq unmeasurable)
          (warn "promotion reported ok without catch-up evidence:" unmeasurable))
        (when (seq unenforced)
          (warn "server accepted a promotion below min_applied_index:" unenforced))
        (when (seq unmeasured)
          (warn "partition window produced no lease evidence:" unmeasured))
        {:valid?               (and (pos? (count promotions))
                                    (pos? writes)
                                    (pos? reads)
                                    (not no-probe)
                                    (empty? premature)
                                    (empty? unmeasurable)
                                    (empty? unsampled)
                                    (empty? no-evidence)
                                    (empty? lost)
                                    (empty? stalls)
                                    (empty? fallbacks)
                                    (empty? unmeasured)
                                    (empty? unenforced))
         :promotions           (count promotions)
         :ok-writes            writes
         :ok-reads             reads
         :premature-promotions premature
         :unmeasurable-promotions unmeasurable
         :unsampled-targets    unsampled
         :promotions-without-post-read-evidence no-evidence
         :lost-writes          lost
         :learner-read-failures stalls
         :lease-fast-path-losses fallbacks
         :unmeasured-partition-windows unmeasured
         :unenforced-min-applied-index unenforced
         :min-applied-index-probe-ran (not no-probe)}))))

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

(defn- redis-conn
  [node->port test node]
  (let [port (get node->port node 6379)
        host (or (:redis-host test) (name node))]
    {:pool {} :spec {:host host :port port :timeout-ms 10000}}))

(defrecord LearnerClient [node->port leader-addr conn leaseConn]
  client/Client
  (open! [this test node]
    (assoc this
           :conn (redis-conn node->port test node)
           ;; Lease probes go to a CONNECTED VOTER, not to this worker's own
           ;; node. A GET issued through the isolated learner has to proxy to
           ;; the leader across the partition and can fail even when the
           ;; leader's lease is perfectly healthy -- the checker would then
           ;; read client placement as a quorum-ack regression. Ordinary
           ;; register traffic stays distributed; only the lease measurement
           ;; is pinned.
           :leaseConn (redis-conn node->port test (first (:nodes test)))))

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
          ;; The reply is CHECKED. Carmine returns nil or a Throwable value on
          ;; missing, protocol and some error-reply paths, and treating those
          ;; as acknowledged would let the checker call an unconfirmed write
          ;; durable -- producing either a false loss or a false preservation
          ;; later. Anything that is not "OK" is :info (indeterminate), which
          ;; is what an unacknowledged write actually is.
          :write (let [reply (wcar conn (car/set register-key (:value op)))]
                   (if (= "OK" (some-> reply str))
                     (assoc op :type :ok)
                     (assoc op :type :info :error {:unexpected-reply reply})))

          ;; :lease? marks the read as one the leader may serve from its lease,
          ;; which is the path a learner wrongly counted in quorumAckTracker
          ;; would break. :latency-ms is recorded because losing the fast path
          ;; does not fail the read -- the engine falls back to
          ;; LinearizableRead -- it makes it take a Raft round trip.
          :read (let [started (System/nanoTime)
                      v       (wcar (:leaseConn this) (car/get register-key))
                      elapsed (/ (double (- (System/nanoTime) started)) 1e6)]
                  (assoc op :type :ok
                            :value {:lease?     true
                                    :latency-ms elapsed
                                    :value      (when v (Long/parseLong (str v)))}))

          :add-learner
          (let [candidate (name (:value op))]
            (raftadmin! leader addr "add_learner" candidate
                        (str candidate ":" (:grpc-port test 50051)) "0")
            (assoc op :type :ok))

          ;; Asks the server for something it must refuse: promotion at a
          ;; min_applied_index far above the leader's own commit index, which
          ;; no learner can have reached. Rejection is the pass. This is the
          ;; only way the enforcement can be observed -- the live promotion
          ;; path waits for catch-up first, so it can never present the server
          ;; with a violation to reject.
          :promote-learner-early
          (let [candidate      (name (:value op))
                candidate-addr (str candidate ":" (:grpc-port test 50051))
                commit         (leader-commit-index leader addr)
                unreachable    (+ (or commit 0) 1000000)]
            (raftadmin! leader addr "promote_learner" candidate
                        "0" (str unreachable))
            ;; Reached only when the server ACCEPTED it -- the defect.
            (assoc op :type :ok
                      :value {:node                candidate
                              :addr                candidate-addr
                              :leader-commit-index commit
                              :min-applied-index   unreachable}))

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
          ;; A failed READ keeps its :lease? marker. Returning the bare op left
          ;; every real read exception without it, so
          ;; learner-partition-read-failures filtered them all out and the
          ;; lease regression could never invalidate a run.
          ;; :promote-learner-early lands here on the happy path -- its
          ;; rejection is the property holding, not a workload failure, which
          ;; is why only an :ok from it is reported as a defect.
          (if (= :read (:f op))
            (assoc op :type :fail
                      :value {:lease? true}
                      :error (.getMessage e))
            (assoc op :type :fail :error (.getMessage e))))))))

;; ---------------------------------------------------------------------------
;; Nemesis
;; ---------------------------------------------------------------------------

(defn learner-partition-nemesis
  "Isolates ONLY the learner candidate, leaving every voter connected.

  That is the shape the quorum property needs: voters retain quorum among
  themselves, so anything that degrades must be attributable to the learner
  being counted where it should not be.

  Implemented directly rather than via nemesis/partitioner, which dispatches on
  :start and :stop. Feeding it :start-partition / :stop-partition matched no
  branch, so the learner was never isolated and the partition workload ran with
  no fault at all -- and its :value would have been taken as the grudge itself,
  which is not one."
  [nodes]
  (let [learner (learner-candidate nodes)
        grudge  (nemesis/complete-grudge [[learner] (voter-nodes nodes)])]
    (reify nemesis/Nemesis
      (setup! [this test]
        (net/heal! (:net test) test)
        this)

      (invoke! [_this test op]
        ;; The lease counters are sampled on the node the client sends its
        ;; lease probes to, at both edges of the window. The DELTA across the
        ;; window is the evidence: misses are reads that fell back to
        ;; LinearizableRead, which is precisely the regression, and hits prove
        ;; reads were actually served in the window rather than the window
        ;; being empty.
        (let [probe-node (first (:nodes test))]
          (case (:f op)
            :start-partition (let [before (ekdb/lease-read-counters probe-node)]
                               ;; jepsen.net/drop-all! is the 2-arity wrapper in
                               ;; jepsen.net -- it reads (:net test) itself. Only
                               ;; jepsen.net.proto/drop-all! takes net first, and
                               ;; heal! below differs because it is import-vars'd
                               ;; straight from the protocol.
                               (net/drop-all! test grudge)
                               (assoc op :value {:scope          :learners-only
                                                 :isolated       [learner]
                                                 :lease-counters before}))
            :stop-partition  (do (net/heal! (:net test) test)
                                 (assoc op :value {:scope          :learners-only
                                                   :healed         true
                                                   :lease-counters (ekdb/lease-read-counters
                                                                     probe-node)}))
            (assoc op :value :unsupported))))

      (teardown! [_this test]
        (net/heal! (:net test) test)))))

(defn learner-nemesis-generator
  "ONE start-partition / stop-partition pair, tagged :learners-only.

  Not a cycle. The promotion converts the candidate into a voter, so a later
  window carrying the same :learners-only label is isolating a node that now
  counts toward quorum: later failures or latency would be attributed to
  learner handling, and on a small cluster the isolation could remove a
  legitimate voter quorum outright. The single window is opened before the
  promotion phase and closed before it runs."
  []
  ;; A seq is a generator in Jepsen 0.3.x; gen/seq was removed.
  ;;
  ;; The window opens at 6s, AFTER :add-learner at ~5s. At 2s it opened before
  ;; the candidate was attached, leaving barely a second in which the node was
  ;; both attached and isolated -- and at rate 5 with stagger that interval
  ;; could contain no read at all, so the quorum-ack property went unexercised
  ;; while reads elsewhere in the run satisfied the coverage gate.
  [(gen/sleep 6)
   {:type :info :f :start-partition}
   (gen/sleep 4)
   {:type :info :f :stop-partition}])

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
      ;; Spans the 6s..10s partition window, so the reads the lease counters
      ;; measure happen with the candidate attached and isolated.
      (gen/time-limit 6 register)
      ;; While it is still a learner: ask for a promotion the server must
      ;; refuse, which is the only observation that its min_applied_index test
      ;; is doing anything.
      (gen/once {:f :promote-learner-early :value candidate})
      (gen/time-limit 2 register)
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
      ;; Carried into the test map so LearnerClient.open! can honour it.
      ;; Without this a programmatic caller could not override the host, and
      ;; local or port-mapped runs tried to resolve each logical node name.
      :redis-host  (:redis-host opts)
      :db          db
      :os          (if local? os/noop debian/os)
      :net         (if local? net/noop net/iptables)
      :ssh         (merge {:username "vagrant"
                           :private-key-path "/home/vagrant/.ssh/id_rsa"
                           :strict-host-key-checking false}
                          (when local? {:dummy true})
                          (:ssh opts))
      :remote      control/ssh
      :client      (->LearnerClient ports nil nil nil)
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

(defn prepare-learner-opts
  "Normalise parsed CLI options for elastickv-learner-test.

  Not `identity`. run-workload! hands the raw parsed map straight through, and
  common-cli-opts leaves :nodes as the comma-separated STRING it came from -- so
  the constructor treated a string as a node collection: learner-candidate
  returned its last character, the port map was keyed by characters, and the
  resulting Jepsen :nodes was invalid. parse-common-opts splits it, and the host
  override is copied to :redis-host so the client can honour it."
  [options]
  (let [options (cli/parse-common-opts options nil)]
    (assoc options :redis-host (:host options))))

(defn -main
  "Runnable entry point. Without one, neither invocation form could select
  this workload: the namespace had no -main, and the shared dispatcher in
  elastickv.jepsen-test neither required it nor listed it, so passing its
  name fell through to the Redis test."
  [& args]
  (cli/run-workload! args cli/common-cli-opts prepare-learner-opts elastickv-learner-test))
