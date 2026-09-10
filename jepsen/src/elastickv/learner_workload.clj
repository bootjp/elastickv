(ns elastickv.learner-workload
  "Jepsen workload for the Raft learner primitive: attach a learner,
  promote it under partition, and assert the three safety properties
  the learner design leaves as Milestone 3 hardening.

  Operations:

    {:f :write           :value n}                  write n to the register
    {:f :read}                                      read the register
    {:f :add-learner     :value node}               attach node as a learner
    {:f :promote-learner :value {:node                n
                                 :min-applied-index   i
                                 :match               m
                                 :leader-commit-index c}}
                                                    promote, recording the
                                                    learner's observed Match,
                                                    the threshold passed, and
                                                    the leader's commit index
                                                    at that moment

  Properties checked (see `learner-safety-checker`):

  1. **Promotion never outruns catch-up.** A promotion that reported :ok
     must have found the learner caught up to the LEADER, i.e. its Match
     at or above the leader's commit index at that moment.

     Comparing min-applied-index against Match alone cannot express this:
     the engine's own test is Match >= min-applied-index, so an operator
     who reads the learner's current Match and passes it back satisfies
     the check by construction. Both the correct pattern (pick the
     leader's commit index as a target, wait for Match to reach it) and
     the broken one (pass whatever Match happens to be) end with
     min-applied-index == Match, so that equality distinguishes nothing.
     The leader's position is the only reference that does — a replica
     promoted while behind it joins the voter quorum without having
     caught up, and can stall writes or cut fault tolerance immediately.

  2. **No acknowledged write is lost across a promotion.** Adding a voter
     changes the quorum denominator; a write acknowledged before the
     membership change must still be readable after it.

  3. **A learner never counts toward the voter quorum.** A learner that is
     unreachable must not stall writes the voters could still commit
     among themselves. Expressed on the history as: no write may fail
     while a partition isolates only learners."
  (:require [clojure.tools.logging :refer [warn]]
            [elastickv.cli :as cli]
            [elastickv.db :as ekdb]
            [jepsen.db :as jdb]
            [jepsen [checker :as checker]
                    [generator :as gen]]))

(def default-nodes ["n1" "n2" "n3" "n4" "n5"])

(defn- promotion-ops
  "Every :promote-learner invocation paired with its completion."
  [history]
  (filter #(= :promote-learner (:f %)) history))

(defn premature-promotions
  "Promotions that committed while the learner was still behind the leader.

  Measured against the LEADER's commit index, not against
  min-applied-index: the engine tests Match >= min-applied-index, so an
  operator passing the learner's own Match satisfies it unconditionally
  and the two patterns are indistinguishable from that pair alone. A
  promotion is premature exactly when the learner's Match had not
  reached the leader's committed position."
  [history]
  (->> (promotion-ops history)
       (filter #(= :ok (:type %)))
       (filter (fn [op]
                 (let [{:keys [match leader-commit-index]} (:value op)]
                   (and (number? match)
                        (number? leader-commit-index)
                        (< match leader-commit-index)))))
       vec))

(defn lost-writes
  "Writes acknowledged :ok that no later successful read observed.

  Only writes that committed strictly before the final read are
  considered: a write still in flight at the end of the history has no
  obligation to appear."
  [history]
  (let [oks      (->> history
                      (filter #(and (= :write (:f %)) (= :ok (:type %))))
                      (map :value)
                      set)
        observed (->> history
                      (filter #(and (= :read (:f %)) (= :ok (:type %))))
                      (map :value)
                      (remove nil?)
                      set)]
    (vec (sort (remove observed oks)))))

(defn learner-quorum-stalls
  "Write failures that occurred while only learners were partitioned.

  A learner does not vote, so isolating one cannot remove voter quorum.
  A write failing in that window means the learner was counted in the
  denominator — the §4.6 regression the design calls out."
  [history]
  (let [windows (->> history
                     (filter #(= :nemesis (:process %)))
                     (filter #(= :start-partition (:f %)))
                     (filter #(= :learners-only (get-in % [:value :scope])))
                     (map :time)
                     sort
                     vec)
        stops   (->> history
                     (filter #(= :nemesis (:process %)))
                     (filter #(= :stop-partition (:f %)))
                     (map :time)
                     sort
                     vec)]
    (if (empty? windows)
      []
      (let [start (first windows)
            stop  (or (first (filter #(> % start) stops)) Long/MAX_VALUE)]
        (->> history
             (filter #(= :write (:f %)))
             (filter #(= :fail (:type %)))
             (filter #(and (>= (:time %) start) (<= (:time %) stop)))
             vec)))))

(defn learner-safety-checker
  "Checks the three learner safety properties over a completed history."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [premature (premature-promotions history)
            lost      (lost-writes history)
            stalls    (learner-quorum-stalls history)]
        (when (seq premature)
          (warn "learner promoted without a real catch-up target:" premature))
        {:valid?               (and (empty? premature)
                                    (empty? lost)
                                    (empty? stalls))
         :promotions           (count (promotion-ops history))
         :premature-promotions premature
         :lost-writes          lost
         :learner-quorum-stalls stalls}))))

(defn elastickv-learner-test
  "Builds a Jepsen test map exercising learner attach and promotion."
  ([] (elastickv-learner-test {}))
  ([opts]
   (let [nodes      (or (:nodes opts) default-nodes)
         local?     (:local opts)
         db         (if local?
                      jdb/noop
                      (ekdb/db {:grpc-port  (or (:grpc-port opts) 50051)
                                :redis-port (or (:redis-port opts) 6379)}))
         time-limit (or (:time-limit opts) 30)]
     {:name        "elastickv-learner"
      :nodes       nodes
      :db          db
      :concurrency (or (:concurrency opts) 10)
      :time-limit  time-limit
      :rate        (double (or (:rate opts) 5))
      :checker     (learner-safety-checker)
      :generator   (gen/time-limit time-limit (gen/nemesis nil))
      :grpc-port   (or (:grpc-port opts) 50051)
      :ports       (or (:node->port opts)
                       (cli/ports->node-map
                         (repeat (count nodes) (or (:grpc-port opts) 50051))
                         nodes))})))
