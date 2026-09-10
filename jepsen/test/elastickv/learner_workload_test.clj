(ns elastickv.learner-workload-test
  (:require [clojure.test :refer :all]
            [jepsen.checker :as checker]
            [elastickv.learner-workload :as workload]))

(defn- check-history [history]
  (checker/check (workload/learner-safety-checker) {} history {}))

(defn- write-op [t v & {:keys [type process] :or {type :ok process 0}}]
  {:type type :f :write :time t :process process :value v})

(defn- read-op [t v & {:keys [type process] :or {type :ok process 0}}]
  {:type type :f :read :time t :process process :value v})

(defn- promote-op [t node min-idx match leader-commit & {:keys [type] :or {type :ok}}]
  {:type type :f :promote-learner :time t :process 0
   :value {:node node :min-applied-index min-idx :match match
           :leader-commit-index leader-commit}})

(defn- partition-op [t f scope]
  {:type :info :f f :time t :process :nemesis :value {:scope scope}})

(deftest builds-test-spec
  (let [test-map (workload/elastickv-learner-test {})]
    (is (map? test-map))
    (is (= "elastickv-learner" (:name test-map)))
    (is (= ["n1" "n2" "n3" "n4" "n5"] (:nodes test-map)))))

(deftest custom-options-override-defaults
  (let [test-map (workload/elastickv-learner-test
                   {:time-limit 60 :concurrency 20 :grpc-port 50999})]
    (is (= 20 (:concurrency test-map)))
    (is (= 60 (:time-limit test-map)))
    (is (= 50999 (:grpc-port test-map)))))

;; ---------------------------------------------------------------------------
;; Property 1 — promotion must not outrun catch-up
;; ---------------------------------------------------------------------------

(deftest promotion-of-a-caught-up-learner-is-valid
  ;; Match reached the leader's commit index before the promotion. This
  ;; is the correct operator pattern: pick the leader's position as the
  ;; target, wait for Match to reach it, then promote at that target.
  (let [r (check-history [(promote-op 100 "n4" 100 100 100)])]
    (is (:valid? r))
    (is (= 1 (:promotions r)))
    (is (empty? (:premature-promotions r)))))

(deftest promotion-of-a-lagging-learner-is-premature
  ;; The broken pattern: the operator read the learner's current Match
  ;; (10) and passed it back while the leader was committed through 100.
  ;; min-applied-index == match, so the engine's check passes by
  ;; construction and a replica 90 entries behind joins the voter quorum.
  (let [r (check-history [(promote-op 100 "n4" 10 10 100)])]
    (is (false? (:valid? r)))
    (is (= 1 (count (:premature-promotions r))))))

(deftest match-equal-to-min-applied-index-does-not-decide-the-property
  ;; Both the correct and the broken call end with
  ;; min-applied-index == match, so that equality distinguishes nothing.
  ;; Only the leader's position separates them — these two histories are
  ;; identical on (min-applied-index, match) and must still be judged
  ;; differently.
  (let [caught-up (check-history [(promote-op 100 "n4" 50 50 50)])
        lagging   (check-history [(promote-op 100 "n4" 50 50 500)])]
    (is (:valid? caught-up))
    (is (false? (:valid? lagging)))))

(deftest a-failed-premature-promotion-is-not-flagged
  ;; The engine rejected it, so no lagging replica was promoted. Only
  ;; promotions that actually committed can violate the property.
  (let [r (check-history [(promote-op 100 "n4" 10 10 100 :type :fail)])]
    (is (:valid? r))
    (is (empty? (:premature-promotions r)))))

;; ---------------------------------------------------------------------------
;; Property 2 — no acknowledged write lost across a promotion
;; ---------------------------------------------------------------------------

(deftest writes-surviving-a-promotion-are-valid
  (let [r (check-history [(write-op 100 1)
                          (promote-op 200 "n4" 100 100 100)
                          (read-op 300 1)])]
    (is (:valid? r))
    (is (empty? (:lost-writes r)))))

(deftest a-write-lost-across-a-promotion-is-detected
  (let [r (check-history [(write-op 100 1)
                          (write-op 150 2)
                          (promote-op 200 "n4" 100 100 100)
                          (read-op 300 1)])]
    (is (false? (:valid? r)))
    (is (= [2] (:lost-writes r)))))

(deftest a-failed-write-is-not-required-to-survive
  (let [r (check-history [(write-op 100 1)
                          (write-op 150 2 :type :fail)
                          (read-op 300 1)])]
    (is (:valid? r))
    (is (empty? (:lost-writes r)))))

;; ---------------------------------------------------------------------------
;; Property 3 — a learner never counts toward the voter quorum
;; ---------------------------------------------------------------------------

(deftest isolating-only-learners-must-not-stall-writes
  ;; A learner does not vote, so isolating one cannot remove voter
  ;; quorum. A write failing in that window means the learner was in the
  ;; denominator — the §4.6 regression.
  (let [r (check-history [(partition-op 100 :start-partition :learners-only)
                          (write-op 150 1 :type :fail)
                          (partition-op 200 :stop-partition :learners-only)])]
    (is (false? (:valid? r)))
    (is (= 1 (count (:learner-quorum-stalls r))))))

(deftest writes-succeeding-while-learners-are-isolated-are-valid
  (let [r (check-history [(partition-op 100 :start-partition :learners-only)
                          (write-op 150 1)
                          (partition-op 200 :stop-partition :learners-only)
                          (read-op 300 1)])]
    (is (:valid? r))
    (is (empty? (:learner-quorum-stalls r)))))

(deftest a-write-failing-under-a-voter-partition-is-not-a-learner-stall
  ;; Isolating voters legitimately removes quorum, so a failure there is
  ;; expected and must not be reported against the learner property.
  (let [r (check-history [(partition-op 100 :start-partition :voters)
                          (write-op 150 1 :type :fail)
                          (partition-op 200 :stop-partition :voters)])]
    (is (empty? (:learner-quorum-stalls r)))))

(deftest a-write-failing-outside-the-partition-window-is-not-a-stall
  (let [r (check-history [(partition-op 100 :start-partition :learners-only)
                          (partition-op 200 :stop-partition :learners-only)
                          (write-op 300 1 :type :fail)])]
    (is (empty? (:learner-quorum-stalls r)))))

(deftest clean-history-reports-valid
  (let [r (check-history [(write-op 100 1)
                          (promote-op 200 "n4" 100 100 100)
                          (read-op 300 1)])]
    (is (:valid? r))
    (is (= 1 (:promotions r)))))
