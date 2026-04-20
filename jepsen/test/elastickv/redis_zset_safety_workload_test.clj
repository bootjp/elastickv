(ns elastickv.redis-zset-safety-workload-test
  "Unit tests for the ZSet safety workload's test-spec construction and
  the model-based checker's edge cases (no-op ZREM, :info ZINCRBY)."
  (:require [clojure.test :refer :all]
            [jepsen.checker :as checker]
            [elastickv.redis-zset-safety-workload :as workload]))

;; ---------------------------------------------------------------------------
;; Test-spec construction
;; ---------------------------------------------------------------------------

(deftest builds-test-spec
  (let [t (workload/elastickv-zset-safety-test {})]
    (is (map? t))
    (is (= "elastickv-redis-zset-safety" (:name t)))
    (is (= ["n1" "n2" "n3" "n4" "n5"] (:nodes t)))
    (is (some? (:client t)))
    (is (some? (:checker t)))
    (is (some? (:generator t)))))

(deftest custom-options-override-defaults
  (let [t (workload/elastickv-zset-safety-test
            {:time-limit 30
             :concurrency 8
             :rate 4})]
    (is (= 8 (:concurrency t)))))

;; ---------------------------------------------------------------------------
;; Checker edge cases
;; ---------------------------------------------------------------------------

(defn- run-checker
  "Run the workload's safety checker against an in-memory history.
  Bypasses the composed timeline.html checker (which writes files to
  the test store) so tests stay hermetic."
  [history]
  (checker/check (workload/zset-safety-checker)
                 (workload/elastickv-zset-safety-test {})
                 history
                 nil))

(deftest noop-zrem-does-not-flag-correct-read
  ;; ZREM of a member that was never added returns 0 (no-op). The model
  ;; must not treat it as a deletion. A subsequent read showing the
  ;; absence of that member is correct.
  (let [history [{:type :invoke :process 0 :f :zrem  :value "ghost" :index 0}
                 {:type :ok     :process 0 :f :zrem  :value ["ghost" false] :index 1}
                 {:type :invoke :process 0 :f :zadd  :value ["m1" 1] :index 2}
                 {:type :ok     :process 0 :f :zadd  :value ["m1" 1] :index 3}
                 {:type :invoke :process 0 :f :zrange-all :index 4}
                 {:type :ok     :process 0 :f :zrange-all :value [["m1" 1.0]] :index 5}]
        result  (run-checker history)]
    (is (:valid? result) (str "expected valid, got: " result))))

(deftest info-zincrby-skips-strict-score-check
  ;; ZINCRBY whose response was lost (:info) leaves the resulting score
  ;; unknown. A read concurrent with such an op observing some derived
  ;; score must NOT be flagged as a score mismatch.
  (let [history [{:type :invoke :process 0 :f :zadd    :value ["m1" 1] :index 0}
                 {:type :ok     :process 0 :f :zadd    :value ["m1" 1] :index 1}
                 {:type :invoke :process 1 :f :zincrby :value ["m1" 5] :index 2}
                 {:type :invoke :process 0 :f :zrange-all :index 3}
                 {:type :ok     :process 0 :f :zrange-all :value [["m1" 6.0]] :index 4}
                 {:type :info   :process 1 :f :zincrby :value ["m1" 5] :index 5}]
        result  (run-checker history)]
    (is (:valid? result) (str "expected valid, got: " result))))

(deftest score-mismatch-is-detected-when-no-uncertainty
  ;; Sanity check: with all ops :ok and no concurrency, an obviously
  ;; wrong observed score IS flagged.
  (let [history [{:type :invoke :process 0 :f :zadd :value ["m1" 1] :index 0}
                 {:type :ok     :process 0 :f :zadd :value ["m1" 1] :index 1}
                 {:type :invoke :process 0 :f :zrange-all :index 2}
                 {:type :ok     :process 0 :f :zrange-all :value [["m1" 999.0]] :index 3}]
        result  (run-checker history)]
    (is (not (:valid? result)) (str "expected mismatch, got: " result))))

(deftest no-op-zrem-alone-does-not-false-positive
  ;; CI-observed false positive: a member whose only prior ops are no-op
  ;; ZREMs was classified as :score-mismatch with :allowed #{} instead
  ;; of treated as never-existed (:phantom candidate, empty read -> OK).
  ;; After the existence-evidence? fix, a read that observes NO such
  ;; member must be accepted as valid.
  (let [history [{:type :invoke :process 0 :f :zrem :value "never-added" :index 0}
                 {:type :invoke :process 1 :f :zrange-all :index 1}
                 {:type :ok     :process 1 :f :zrange-all :value [] :index 2}
                 {:type :ok     :process 0 :f :zrem :value ["never-added" false] :index 3}]
        result  (run-checker history)]
    (is (:valid? result) (str "expected valid, got: " result))))

(deftest duplicate-members-are-flagged
  ;; CodeRabbit finding: ZRANGE must not return the same member twice.
  ;; With a hypothetical committed + concurrent score for the same
  ;; member, a duplicate could sneak past sort + score-membership
  ;; checks. Enforce distinctness explicitly.
  (let [history [{:type :invoke :process 0 :f :zadd :value ["m1" 1] :index 0}
                 {:type :ok     :process 0 :f :zadd :value ["m1" 1] :index 1}
                 {:type :invoke :process 0 :f :zrange-all :index 2}
                 {:type :ok     :process 0 :f :zrange-all
                  :value [["m1" 1.0] ["m1" 1.0]] :index 3}]
        result  (run-checker history)]
    (is (not (:valid? result)) (str "expected duplicate-members error, got: " result))))

(deftest overlapping-committed-zadds-allow-either-score
  ;; CodeRabbit finding: two :ok ZADDs for the same member whose
  ;; invoke/complete windows overlap have ambiguous serialization
  ;; order. Either's resulting score is a valid post-state; the checker
  ;; must not pin to the higher :complete-idx value only.
  ;; Timeline (overlap between A's [invoke=0, complete=3] and
  ;; B's [invoke=1, complete=2]):
  (let [history [{:type :invoke :process 0 :f :zadd :value ["m1" 5] :index 0}
                 {:type :invoke :process 1 :f :zadd :value ["m1" 9] :index 1}
                 {:type :ok     :process 1 :f :zadd :value ["m1" 9] :index 2}
                 {:type :ok     :process 0 :f :zadd :value ["m1" 5] :index 3}
                 ;; Post-commit: either 5 or 9 is a valid final score.
                 ;; A read observing 5 must NOT be flagged as mismatch.
                 {:type :invoke :process 2 :f :zrange-all :index 4}
                 {:type :ok     :process 2 :f :zrange-all
                  :value [["m1" 5.0]] :index 5}]
        result  (run-checker history)]
    (is (:valid? result)
        (str "expected valid under overlapping-writes relaxation, got: " result))))

(deftest info-before-read-is-considered-uncertain
  ;; CodeRabbit finding: an :info mutation that completed before a
  ;; later read may have taken effect. It must be considered a possible
  ;; source of state for that read, rather than being ignored by both
  ;; model-before and the concurrent window.
  (let [history [;; Add m1 with score 1.
                 {:type :invoke :process 0 :f :zadd :value ["m1" 1] :index 0}
                 {:type :ok     :process 0 :f :zadd :value ["m1" 1] :index 1}
                 ;; ZINCRBY m1 by 5 -- response lost, recorded :info.
                 {:type :invoke :process 1 :f :zincrby :value ["m1" 5] :index 2}
                 {:type :info   :process 1 :f :zincrby :value ["m1" 5] :index 3}
                 ;; Later read observes m1 at score 6 (increment applied
                 ;; server-side before the response was lost). Valid.
                 {:type :invoke :process 2 :f :zrange-all :index 4}
                 {:type :ok     :process 2 :f :zrange-all
                  :value [["m1" 6.0]] :index 5}]
        result  (run-checker history)]
    (is (:valid? result)
        (str "expected :info-before-read to skip strict score check, got: " result))))
