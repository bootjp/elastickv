(ns elastickv.learner-workload-test
  "Unit tests for the learner workload's checker and wiring.

  The checker tests matter more than usual here: the workload's whole value is
  that it can FAIL when the learner primitive misbehaves, and an earlier
  revision could not — it had no client, no nemesis and a generator that
  emitted nothing, so every run produced an empty history the checker called
  valid. Several properties were also measuring the wrong thing. Each test
  below names the way it used to pass wrongly."
  (:require [clojure.test :refer :all]
            [elastickv.db :as ekdb]
            [elastickv.jepsen-test :as jt]
            [elastickv.learner-workload :as lw]
            [jepsen.checker :as checker]
            [jepsen.nemesis]))

(defn- check [history]
  (checker/check (lw/learner-safety-checker) {} history {}))

(def ^:private healthy-prefix
  [{:type :invoke :f :write :value 1 :time 10 :process 0}
   {:type :ok     :f :write :value 1 :time 20 :process 0}
   {:type :invoke :f :read  :time 30 :process 0}
   {:type :ok     :f :read  :value {:lease? true :value 1} :time 40 :process 0}])

(defn- promotion
  [m time]
  {:type :ok :f :promote-learner :time time
   :value (merge {:node "n5" :catch-up-target 100
                  :target-source :leader-commit-index :match 100}
                 m)})

(defn- healthy-history []
  (concat healthy-prefix
          [{:type :invoke :f :promote-learner :value "n5" :time 50 :process 0}
           (promotion {} 60)
           ;; Invoked AFTER the promotion completed, which is what makes this
           ;; read evidence about post-promotion state.
           {:type :invoke :f :read :time 70 :process 0}
           {:type :ok     :f :read :value {:lease? true :value 1} :time 80 :process 0}]))

;; A post-promotion read, invoke and completion, for histories whose subject is
;; some other property but which still have to satisfy the coverage gate.
(defn- post-promotion-read
  ([t] (post-promotion-read t 1))
  ([t v] [{:type :invoke :f :read :time t :process 0}
          {:type :ok :f :read :value {:lease? true :value v} :time (inc t) :process 0}]))

(deftest healthy-history-is-valid
  (is (:valid? (check (healthy-history)))))

;; ---------------------------------------------------------------------------
;; 1. Promotion never outruns catch-up
;; ---------------------------------------------------------------------------

(deftest premature-promotion-is-rejected
  (let [r (check (concat healthy-prefix [(promotion {:match 90 :catch-up-target 100} 60)]))]
    (is (false? (:valid? r)))
    (is (= 1 (count (:premature-promotions r))))))

(deftest catch-up-is-measured-against-the-sampled-target-not-a-moving-leader
  ;; The documented safe workflow: sample target T, wait for the learner to
  ;; reach T, promote. The leader keeps committing, so at promotion time the
  ;; learner is at T while the leader is well past it. Comparing Match with
  ;; the leader's CURRENT commit index rejected this healthy run.
  (let [r (check (concat healthy-prefix
                         [(promotion {:catch-up-target 100
                                      :match 100
                                      :leader-commit-index 100000} 60)]
                         (post-promotion-read 70)))]
    (is (:valid? r)
        "a learner that reached its sampled target is caught up, whatever the leader did since")))

(deftest a-target-read-off-the-learner-is-rejected
  ;; The loophole the Match comparison cannot close: the engine's own test is
  ;; Match >= min_applied_index, so passing the learner's current Match
  ;; satisfies it by construction. The procedure, not the arithmetic, has to
  ;; be checked.
  (let [r (check (concat healthy-prefix
                         [(promotion {:target-source :learner-match} 60)]))]
    (is (false? (:valid? r)))
    (is (= 1 (count (:unsampled-targets r))))))

(deftest a-promotion-without-evidence-fails-closed
  ;; Previously the numeric guards simply skipped these, so a successful but
  ;; unmeasurable promotion left the checker reporting valid having verified
  ;; nothing.
  (doseq [missing [{:match nil} {:catch-up-target nil}]]
    (let [r (check (concat healthy-prefix [(promotion missing 60)]))]
      (is (false? (:valid? r)) (str "missing " (keys missing)))
      (is (= 1 (count (:unmeasurable-promotions r)))))))

(deftest promotions-are-counted-once-per-completed-call
  ;; An op appears as :invoke plus a completion, so counting both reported two
  ;; promotions per call and counted a bare invocation as one.
  (let [r (check (concat healthy-prefix
                         [{:type :invoke :f :promote-learner :value "n5" :time 50}
                          (promotion {} 60)]))]
    (is (= 1 (:promotions r)))))

;; ---------------------------------------------------------------------------
;; 2. No acknowledged write is lost across a promotion
;; ---------------------------------------------------------------------------

(deftest a-write-lost-across-a-promotion-is-rejected
  (let [r (check [{:type :invoke :f :write :value 7 :time 10}
                  {:type :ok     :f :write :value 7 :time 20}
                  {:type :invoke :f :promote-learner :value "n5" :time 30}
                  (promotion {} 40)
                  {:type :invoke :f :read :time 50}
                  {:type :ok     :f :read :value {:lease? true :value 3} :time 60}])]
    (is (false? (:valid? r)))
    (is (= 1 (count (:lost-writes r))))))

(deftest an-overwritten-value-is-not-a-lost-write
  ;; Set subtraction reported 1 as lost in this legal register history merely
  ;; because it was overwritten before anyone read it.
  (let [r (check [{:type :invoke :f :write :value 1 :time 10}
                  {:type :ok     :f :write :value 1 :time 20}
                  {:type :invoke :f :write :value 2 :time 30}
                  {:type :ok     :f :write :value 2 :time 40}
                  {:type :invoke :f :promote-learner :value "n5" :time 50}
                  (promotion {} 60)
                  {:type :invoke :f :read :time 70}
                  {:type :ok     :f :read :value {:lease? true :value 2} :time 80}])]
    (is (:valid? r) (str "lost-writes=" (:lost-writes r)))))

(deftest a-concurrent-write-makes-the-expected-value-ambiguous-not-wrong
  ;; With another write in flight between the acked write and the read, the
  ;; register's value is not pinned, so no conclusion is drawn rather than a
  ;; false loss being reported.
  (let [r (check [{:type :invoke :f :write :value 1 :time 10}
                  {:type :ok     :f :write :value 1 :time 20}
                  {:type :invoke :f :promote-learner :value "n5" :time 30}
                  (promotion {} 40)
                  {:type :invoke :f :write :value 9 :time 50}
                  {:type :invoke :f :read :time 60}
                  {:type :ok     :f :read :value {:lease? true :value 9} :time 70}])]
    (is (:valid? r) (str "lost-writes=" (:lost-writes r)))))

;; ---------------------------------------------------------------------------
;; 3. A learner never counts toward the lease
;; ---------------------------------------------------------------------------

(defn- partition-window [start stop]
  [{:type :info :process :nemesis :f :start-partition
    :value {:scope :learners-only} :time start}
   {:type :info :process :nemesis :f :stop-partition
    :value {:scope :learners-only} :time stop}])

(deftest a-lease-read-failing-under-learner-isolation-is-rejected
  ;; Reads, not writes: quorumAckTracker feeds LastQuorumAck and the
  ;; lease-read fast path, not the write-commit quorum, so a write-failure
  ;; check stayed green through the exact regression it claimed to detect.
  (let [r (check (concat healthy-prefix
                         [(promotion {} 50)]
                         (partition-window 100 200)
                         [{:type :invoke :f :read :time 120}
                          {:type :fail   :f :read :value {:lease? true} :time 130}]))]
    (is (false? (:valid? r)))
    (is (= 1 (count (:learner-read-failures r))))))

(deftest every-learner-partition-window-is-checked-not-just-the-first
  ;; Taking the first start and the first later stop ignored every subsequent
  ;; isolation window, so a regression in the second one could not fail.
  (let [r (check (concat healthy-prefix
                         [(promotion {} 50)]
                         (partition-window 100 200)
                         (partition-window 300 400)
                         [{:type :invoke :f :read :time 320}
                          {:type :fail   :f :read :value {:lease? true} :time 330}]))]
    (is (false? (:valid? r)))
    (is (= 1 (count (:learner-read-failures r)))
        "a failure in the SECOND window must still be caught")))

(deftest a-read-failure-outside-any-window-is-not-attributed-to-the-learner
  (let [r (check (concat healthy-prefix
                         [(promotion {} 50)]
                         (post-promotion-read 60)
                         (partition-window 100 200)
                         [{:type :invoke :f :read :time 500}
                          {:type :fail   :f :read :value {:lease? true} :time 510}]))]
    (is (:valid? r) (str "failures=" (:learner-read-failures r)))))

;; ---------------------------------------------------------------------------
;; The checker must not pass a run that proved nothing
;; ---------------------------------------------------------------------------

(deftest an-empty-history-is-invalid
  ;; THE load-bearing test. Before the workload had a client, a nemesis and a
  ;; generator that emitted its documented operations, a real run produced an
  ;; empty history — and the checker called it valid, so the gate could never
  ;; fail.
  (is (false? (:valid? (check [])))))

(deftest a-history-with-no-promotion-is-invalid
  (is (false? (:valid? (check healthy-prefix)))
      "a learner test that never promoted has not tested promotion"))

(deftest a-history-with-no-reads-is-invalid
  (is (false? (:valid? (check [{:type :invoke :f :write :value 1 :time 10}
                               {:type :ok :f :write :value 1 :time 20}
                               (promotion {} 30)])))
      "the lease property is unobservable without reads"))

;; ---------------------------------------------------------------------------
;; Wiring: the workload has to be able to run at all
;; ---------------------------------------------------------------------------

(deftest the-test-map-has-a-client-and-a-nemesis
  ;; It had neither, so nothing could drive the cluster.
  (let [t (lw/elastickv-learner-test {:nodes ["n1" "n2" "n3" "n4" "n5"]})]
    (is (some? (:client t)))
    (is (some? (:nemesis t)))
    (is (some? (:generator t)))))

(deftest the-generator-emits-every-documented-operation
  ;; The old generator was gen/nemesis applied to nil, so none of :write,
  ;; :read, :add-learner or :promote-learner could ever appear.
  (let [ops (->> (lw/client-generator ["n1" "n2" "n3" "n4" "n5"])
                 (tree-seq coll? seq)
                 (keep #(when (map? %) (:f %)))
                 set)]
    (is (contains? ops :add-learner))
    (is (contains? ops :promote-learner))))

(deftest a-node-is-reserved-outside-the-initial-voter-set
  ;; ElastickvDB's setup adds every node after the bootstrap one as a voter,
  ;; so without a reservation there is no non-member left to attach and
  ;; :add-learner cannot run.
  (let [nodes ["n1" "n2" "n3" "n4" "n5"]]
    (is (= "n5" (lw/learner-candidate nodes)))
    (is (= ["n2" "n3" "n4"] (ekdb/voter-peers nodes "n5"))
        "the reserved candidate must not be joined as a voter")
    (is (= ["n2" "n3" "n4" "n5"] (ekdb/voter-peers nodes nil))
        "with no reservation the existing behaviour is unchanged")
    (is (= "n5" (get-in (lw/elastickv-learner-test {:nodes nodes})
                        [:db :opts :reserve-learner])))))

(deftest the-workload-is-reachable-from-the-shared-dispatcher
  ;; Neither invocation form could select it: the namespace had no -main and
  ;; the dispatcher neither required nor listed it, so the name fell through
  ;; to the Redis test.
  (is (fn? (deref (resolve 'elastickv.learner-workload/-main)))
      "the namespace needs a -main to be runnable directly")
  (is (= "elastickv-learner"
         (:name (jt/elastickv-learner-test {:nodes ["n1" "n2" "n3"]})))))

(deftest raft-status-parsing-reads-the-indices-the-promotion-needs
  (let [out (str "state: Leader\n"
                 "leader_id: \"n1\"\n"
                 "term: 3\n"
                 "commit_index: 4211\n"
                 "applied_index: 4207\n"
                 "pending_conf_change: false\n")
        got (ekdb/parse-raft-status out)]
    (is (= 4211 (:commit_index got)))
    (is (= 4207 (:applied_index got)))
    (is (= "n1" (:leader_id got)))))

;; ---------------------------------------------------------------------------
;; Round 3: the workload has to be able to run, and each property has to be
;; able to fire. Every test below names the way the previous revision passed
;; wrongly.
;; ---------------------------------------------------------------------------

(deftest coverage-requires-a-successful-promotion
  ;; The gate counted every completion, so a history with good reads and writes
  ;; plus a FAILED promotion had a positive count while every safety predicate
  ;; — which all filter to :ok — saw nothing. A run in which no learner was
  ;; ever promoted was reported valid.
  (let [failed (check (concat healthy-prefix
                              [{:type :invoke :f :promote-learner :value "n5" :time 50}
                               {:type :fail :f :promote-learner :time 60}]
                              (post-promotion-read 70)))]
    (is (false? (:valid? failed)))
    (is (zero? (:promotions failed))
        "a failed promotion is not a promotion")))

(deftest a-promotion-with-no-read-after-it-is-invalid
  ;; lost-writes-across-promotions SILENTLY SKIPS a promotion it cannot pair
  ;; with a later read, which happens when catch-up finishes near the time
  ;; limit. Counting reads anywhere in the history hid that: the run passed
  ;; having observed no post-promotion state at all.
  (let [r (check (concat healthy-prefix [(promotion {} 60)]))]
    (is (false? (:valid? r)))
    (is (= 1 (count (:promotions-without-post-read-evidence r))))))

(deftest a-read-invoked-before-the-promotion-is-not-post-promotion-evidence
  ;; With concurrent workers a read invoked before the promotion can linearize
  ;; against the old value and return after it. Selecting on completion time
  ;; alone picked up exactly that read — both accepting it as evidence and
  ;; reporting the acknowledged write as lost.
  (let [r (check [{:type :invoke :f :write :value 7 :time 10}
                  {:type :ok     :f :write :value 7 :time 20}
                  ;; Invoked BEFORE the promotion, returns after it.
                  {:type :invoke :f :read :time 30 :process 1}
                  {:type :invoke :f :promote-learner :value "n5" :time 40}
                  (promotion {} 50)
                  {:type :ok :f :read :value {:lease? true :value 7} :time 60 :process 1}])]
    (is (false? (:valid? r)))
    (is (= 1 (count (:promotions-without-post-read-evidence r)))
        "a read that started before the promotion proves nothing about after it")
    (is (empty? (:lost-writes r))
        "and it must not be mistaken for a lost write either")))

(deftest a-slow-lease-read-under-learner-isolation-is-rejected
  ;; Failures alone cannot detect the quorum-ack regression: a Redis GET calls
  ;; LeaseReadForKeyThrough, and when the lease is unavailable the engine falls
  ;; back to LinearizableRead transparently — so with the voters connected the
  ;; read SUCCEEDS and a failure-only check stays empty through exactly the
  ;; regression it claims to catch. What changes is the latency.
  (let [r (check (concat healthy-prefix
                         [(promotion {} 50)]
                         (post-promotion-read 60)
                         (partition-window 100 200)
                         [{:type :invoke :f :read :time 120}
                          {:type :ok :f :read :time 130
                           :value {:lease? true :latency-ms 900.0 :value 1}}]))]
    (is (false? (:valid? r)))
    (is (= 1 (count (:slow-lease-reads r)))
        "a lease read that fell back to a Raft round trip must fail the run")))

(deftest a-fast-lease-read-under-learner-isolation-is-fine
  (let [r (check (concat healthy-prefix
                         [(promotion {} 50)]
                         (post-promotion-read 60)
                         (partition-window 100 200)
                         [{:type :invoke :f :read :time 120}
                          {:type :ok :f :read :time 130
                           :value {:lease? true :latency-ms 3.0 :value 1}}]))]
    (is (:valid? r) (str "slow=" (:slow-lease-reads r)))))

(deftest a-slow-lease-read-outside-the-window-is-not-attributed-to-the-learner
  (let [r (check (concat healthy-prefix
                         [(promotion {} 50)]
                         (post-promotion-read 60)
                         (partition-window 100 200)
                         [{:type :invoke :f :read :time 500}
                          {:type :ok :f :read :time 510
                           :value {:lease? true :latency-ms 900.0 :value 1}}]))]
    (is (:valid? r) (str "slow=" (:slow-lease-reads r)))))

(deftest the-nemesis-handles-the-operations-the-generator-emits
  ;; jepsen.nemesis/partitioner dispatches on :start and :stop. Feeding it
  ;; :start-partition / :stop-partition matched no branch, so the learner was
  ;; never isolated and the partition workload ran with no fault at all — and
  ;; its :value would have been taken as the grudge, which is not one.
  (let [nodes ["n1" "n2" "n3" "n4" "n5"]
        emitted (->> (lw/learner-nemesis-generator)
                     (tree-seq coll? seq)
                     (keep #(when (map? %) (:f %)))
                     set)]
    (is (= #{:start-partition :stop-partition} emitted))
    ;; And the nemesis must claim exactly those.
    (let [n (lw/learner-partition-nemesis nodes)]
      (is (some? n))
      (is (satisfies? jepsen.nemesis/Nemesis n)))))

(deftest the-learner-partition-runs-once-not-in-a-cycle
  ;; The promotion converts the candidate into a voter, so a later window
  ;; carrying the same :learners-only label isolates a node that now counts
  ;; toward quorum: failures and latency after that are not learner behaviour,
  ;; and on a small cluster the isolation can remove a legitimate voter quorum.
  (let [ops (->> (lw/learner-nemesis-generator)
                 (tree-seq coll? seq)
                 (keep #(when (map? %) (:f %))))]
    (is (= 1 (count (filter #{:start-partition} ops))))
    (is (= 1 (count (filter #{:stop-partition} ops))))))

(deftest cli-options-are-parsed-before-the-workload-is-constructed
  ;; run-workload! hands the raw parsed map straight through, and
  ;; common-cli-opts leaves :nodes as the comma-separated STRING. With identity
  ;; as the preparation fn the constructor treated that string as a node
  ;; collection: learner-candidate returned its last character and the port map
  ;; was keyed by characters.
  (let [prepared (lw/prepare-learner-opts {:nodes "n1,n2,n3" :faults ""})]
    (is (= ["n1" "n2" "n3"] (:nodes prepared))
        "a comma-separated string must be split before it reaches the constructor")
    (is (= "n3" (lw/learner-candidate (:nodes prepared))))))

(deftest the-redis-host-override-reaches-the-test-map
  ;; LearnerClient.open! reads :redis-host from the completed test map, so the
  ;; constructor has to carry it; otherwise a programmatic caller could not
  ;; override the host and local runs tried to resolve each logical node name.
  (let [t (lw/elastickv-learner-test {:nodes ["n1" "n2" "n3"] :redis-host "127.0.0.1"})]
    (is (= "127.0.0.1" (:redis-host t)))))

(deftest the-reserved-candidate-starts-with-a-join-configuration
  ;; Reserving the candidate only kept it out of the add_voter loop; it still
  ;; launched as a fresh non-bootstrap process with no peers, which the etcd
  ;; engine refuses (errNoPeersConfigured), so it exited before :add-learner
  ;; could attach it.
  (let [args (set (ekdb/server-args {:node "n5" :grpc "n5:50051" :redis "n5:6379"
                                     :data-dir "/var/lib/elastickv"
                                     :raft-redis-map "n5=n5:6379"
                                     :join-as-learner true
                                     :join-members "n1=n1:50051,n2=n2:50051"}))]
    (is (contains? args "--raftJoinAsLearner"))
    (is (contains? args "--raftJoinMembers"))
    (is (contains? args "n1=n1:50051,n2=n2:50051")))

  ;; The candidate itself is excluded from the members it discovers: it is
  ;; joining, not already a member.
  (is (= "n1=n1:50051,n2=n2:50051"
         (ekdb/join-members-arg ["n1" "n2" "n5"] "n5" 50051)))

  ;; An ordinary voter gets neither flag.
  (let [args (set (ekdb/server-args {:node "n2" :grpc "n2:50051" :redis "n2:6379"
                                     :data-dir "/var/lib/elastickv"
                                     :raft-redis-map "n2=n2:6379"}))]
    (is (not (contains? args "--raftJoinAsLearner")))
    (is (not (contains? args "--raftJoinMembers")))))
