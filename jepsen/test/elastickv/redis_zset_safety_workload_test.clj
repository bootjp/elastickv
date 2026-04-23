(ns elastickv.redis-zset-safety-workload-test
  "Unit tests for the ZSet safety workload's test-spec construction and
  the model-based checker's edge cases (no-op ZREM, :info ZINCRBY)."
  (:require [clojure.test :refer :all]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
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

(deftest single-ok-concurrent-zincrby-still-validates-scores
  ;; Codex P1: :unknown-score? must NOT be set when exactly one
  ;; concurrent ZINCRBY is :ok (and therefore has a known resulting
  ;; score). The read may observe either the pre-op score or the
  ;; post-op score, both of which are in :scores. An arbitrary
  ;; impossible score (e.g. 999.0) must still be flagged as a
  ;; :score-mismatch, not waved through by `:unknown-score?`.
  (let [history [{:type :invoke :process 0 :f :zadd    :value ["m1" 1]   :index 0}
                 {:type :ok     :process 0 :f :zadd    :value ["m1" 1]   :index 1}
                 {:type :invoke :process 1 :f :zincrby :value ["m1" 5]   :index 2}
                 {:type :invoke :process 0 :f :zrange-all                :index 3}
                 ;; Read observes 999.0 — not 1.0 (pre) or 6.0 (post).
                 {:type :ok     :process 0 :f :zrange-all
                  :value [["m1" 999.0]] :index 4}
                 ;; ZINCRBY eventually completes :ok with known score 6.
                 {:type :ok     :process 1 :f :zincrby :value ["m1" 6.0] :index 5}]
        result  (run-checker history)
        kinds   (set (map :kind (:first-errors result)))]
    (is (not (:valid? result))
        (str "expected score-mismatch to still fire, got: " result))
    (is (contains? kinds :score-mismatch)
        (str "expected :score-mismatch, got kinds=" kinds))))

(deftest two-concurrent-zincrbys-relax-score-check
  ;; Prefix-sum ordering matters: with two concurrent ZINCRBYs, the
  ;; intermediate score (pre + one delta) is reachable and need not be
  ;; in :scores. The checker must relax the strict score check.
  (let [history [{:type :invoke :process 0 :f :zadd    :value ["m1" 1]   :index 0}
                 {:type :ok     :process 0 :f :zadd    :value ["m1" 1]   :index 1}
                 {:type :invoke :process 1 :f :zincrby :value ["m1" 2]   :index 2}
                 {:type :invoke :process 2 :f :zincrby :value ["m1" 3]   :index 3}
                 {:type :invoke :process 0 :f :zrange-all                :index 4}
                 ;; Intermediate 3.0 = 1 + 2 (before +3 applied).
                 {:type :ok     :process 0 :f :zrange-all
                  :value [["m1" 3.0]] :index 5}
                 {:type :ok     :process 1 :f :zincrby :value ["m1" 3.0] :index 6}
                 {:type :ok     :process 2 :f :zincrby :value ["m1" 6.0] :index 7}]
        result  (run-checker history)]
    (is (:valid? result)
        (str "expected relaxation for >=2 concurrent ZINCRBYs, got: " result))))

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

;; ---------------------------------------------------------------------------
;; Stale-read / phantom / superseded-committed checks (gemini HIGH)
;; ---------------------------------------------------------------------------

(deftest phantom-member-is-flagged
  ;; gemini HIGH: a read that observes a member which was never added
  ;; (no ZADD/ZINCRBY/true-ZREM anywhere) must be rejected.
  (let [history [{:type :invoke :process 0 :f :zrange-all :index 0}
                 {:type :ok     :process 0 :f :zrange-all
                  :value [["never-added" 42.0]] :index 1}]
        result  (run-checker history)
        kinds   (set (map :kind (:first-errors result)))]
    (is (not (:valid? result)) (str "expected phantom error, got: " result))
    (is (contains? kinds :unexpected-presence)
        (str "expected :unexpected-presence, got kinds=" kinds))))

(deftest stale-read-after-committed-zrem-is-flagged
  ;; gemini HIGH: once a ZADD and a subsequent real (:removed? true) ZREM
  ;; have BOTH committed (with no concurrent re-add), a later read that
  ;; still sees the member must be rejected as a stale read.
  (let [history [;; Add then remove m1 — both committed before any read.
                 {:type :invoke :process 0 :f :zadd :value ["m1" 1] :index 0}
                 {:type :ok     :process 0 :f :zadd :value ["m1" 1] :index 1}
                 {:type :invoke :process 0 :f :zrem :value "m1" :index 2}
                 {:type :ok     :process 0 :f :zrem :value ["m1" true] :index 3}
                 ;; Stale read: m1 somehow still appears.
                 {:type :invoke :process 1 :f :zrange-all :index 4}
                 {:type :ok     :process 1 :f :zrange-all
                  :value [["m1" 1.0]] :index 5}]
        result  (run-checker history)
        kinds   (set (map :kind (:first-errors result)))]
    (is (not (:valid? result)) (str "expected stale-read error, got: " result))
    (is (contains? kinds :unexpected-presence)
        (str "expected :unexpected-presence, got kinds=" kinds))))

(deftest superseded-committed-score-is-not-allowed
  ;; gemini HIGH: a ZADD committed BEFORE another ZADD for the same
  ;; member whose invoke strictly followed it should not be treated as
  ;; a valid post-state score. Only the latest committed score (plus
  ;; concurrent in-flight) may be observed.
  (let [history [;; ZADD m1 1 commits first ...
                 {:type :invoke :process 0 :f :zadd :value ["m1" 1] :index 0}
                 {:type :ok     :process 0 :f :zadd :value ["m1" 1] :index 1}
                 ;; ... then ZADD m1 2 is invoked strictly after, and
                 ;; also commits before the read.
                 {:type :invoke :process 0 :f :zadd :value ["m1" 2] :index 2}
                 {:type :ok     :process 0 :f :zadd :value ["m1" 2] :index 3}
                 ;; Read observing the SUPERSEDED score 1.0 — invalid.
                 {:type :invoke :process 1 :f :zrange-all :index 4}
                 {:type :ok     :process 1 :f :zrange-all
                  :value [["m1" 1.0]] :index 5}]
        result  (run-checker history)]
    (is (not (:valid? result))
        (str "expected superseded-score mismatch, got: " result))))

;; ---------------------------------------------------------------------------
;; Infinity score parsing
;; ---------------------------------------------------------------------------

;; ---------------------------------------------------------------------------
;; Linearization of concurrent ops / uncertain mutations (gemini HIGH batch 2)
;; ---------------------------------------------------------------------------

(deftest concurrent-zadd-zrem-both-completed-accepts-either-outcome
  ;; gemini HIGH: ZADD and ZREM for the same member whose invoke/complete
  ;; windows overlap (both commit before the read) have ambiguous
  ;; linearization. A linearizable store may serialize either one last,
  ;; so the read legitimately observes EITHER [["m1" 1.0]] OR [].
  ;; Windows: ZADD=[inv=0, cmp=3], ZREM=[inv=1, cmp=2] — overlap.
  (let [base [{:type :invoke :process 0 :f :zadd :value ["m1" 1] :index 0}
              {:type :invoke :process 1 :f :zrem :value "m1" :index 1}
              {:type :ok     :process 1 :f :zrem :value ["m1" true] :index 2}
              {:type :ok     :process 0 :f :zadd :value ["m1" 1] :index 3}]
        hist-present (conj base
                        {:type :invoke :process 2 :f :zrange-all :index 4}
                        {:type :ok     :process 2 :f :zrange-all
                         :value [["m1" 1.0]] :index 5})
        hist-absent (conj base
                       {:type :invoke :process 2 :f :zrange-all :index 4}
                       {:type :ok     :process 2 :f :zrange-all
                        :value [] :index 5})]
    (is (:valid? (run-checker hist-present))
        "expected read observing ZADD's outcome to be accepted")
    (is (:valid? (run-checker hist-absent))
        "expected read observing ZREM's outcome (absent) to be accepted")))

(deftest info-zrem-concurrent-with-read-allows-missing-member
  ;; gemini HIGH: an :info ZREM that might have applied before a read
  ;; leaves the member's presence uncertain. A ZRANGE that omits the
  ;; member must NOT be flagged as a completeness failure.
  (let [history [;; ZADD m1 committed before the read.
                 {:type :invoke :process 0 :f :zadd :value ["m1" 1] :index 0}
                 {:type :ok     :process 0 :f :zadd :value ["m1" 1] :index 1}
                 ;; ZREM m1 is invoked, then the read runs, then the
                 ;; ZREM response is lost (:info). The ZREM may or may
                 ;; not have applied server-side.
                 {:type :invoke :process 1 :f :zrem :value "m1" :index 2}
                 {:type :invoke :process 0 :f :zrange-all :index 3}
                 {:type :ok     :process 0 :f :zrange-all :value [] :index 4}
                 {:type :info   :process 1 :f :zrem :value "m1" :index 5}]
        result  (run-checker history)]
    (is (:valid? result)
        (str "expected :info ZREM to make absence acceptable, got: " result))))

(deftest info-zincrby-does-not-flag-zrangebyscore-completeness
  ;; gemini HIGH: a pre-read :info / concurrent ZINCRBY leaves the
  ;; resulting score unknown. ZRANGEBYSCORE filtering on a specific
  ;; range must not flag the member as missing, because its score may
  ;; have moved outside [lo, hi].
  (let [history [;; ZADD m1 at score 1 (committed well before read).
                 {:type :invoke :process 0 :f :zadd :value ["m1" 1] :index 0}
                 {:type :ok     :process 0 :f :zadd :value ["m1" 1] :index 1}
                 ;; ZINCRBY m1 +100 — response lost (:info) BEFORE read.
                 {:type :invoke :process 1 :f :zincrby :value ["m1" 100] :index 2}
                 {:type :info   :process 1 :f :zincrby :value ["m1" 100] :index 3}
                 ;; ZRANGEBYSCORE [0, 10] — m1's score is uncertain; it
                 ;; may now be 101 (outside range) or still 1. The
                 ;; checker must not complain about m1's absence.
                 {:type :invoke :process 2 :f :zrangebyscore :value [0.0 10.0] :index 4}
                 {:type :ok     :process 2 :f :zrangebyscore
                  :value {:bounds [0.0 10.0] :members []} :index 5}]
        result  (run-checker history)]
    (is (:valid? result)
        (str "expected :info ZINCRBY to skip completeness, got: " result))))

(deftest zrangebyscore-completeness-still-detects-truly-missing-member
  ;; Sanity: when NO uncertainty exists and a model member's committed
  ;; score is definitively inside [lo, hi], its absence IS flagged.
  (let [history [{:type :invoke :process 0 :f :zadd :value ["m1" 5] :index 0}
                 {:type :ok     :process 0 :f :zadd :value ["m1" 5] :index 1}
                 {:type :invoke :process 0 :f :zrangebyscore :value [0.0 10.0] :index 2}
                 {:type :ok     :process 0 :f :zrangebyscore
                  :value {:bounds [0.0 10.0] :members []} :index 3}]
        result  (run-checker history)
        kinds   (set (map :kind (:first-errors result)))]
    (is (not (:valid? result)) (str "expected missing-member-range, got: " result))
    (is (contains? kinds :missing-member-range)
        (str "expected :missing-member-range, got kinds=" kinds))))

(deftest zrange-completeness-still-detects-truly-missing-member
  ;; Sanity: no uncertainty, member committed-present. Absence flagged.
  (let [history [{:type :invoke :process 0 :f :zadd :value ["m1" 5] :index 0}
                 {:type :ok     :process 0 :f :zadd :value ["m1" 5] :index 1}
                 {:type :invoke :process 0 :f :zrange-all :index 2}
                 {:type :ok     :process 0 :f :zrange-all :value [] :index 3}]
        result  (run-checker history)
        kinds   (set (map :kind (:first-errors result)))]
    (is (not (:valid? result)) (str "expected missing-member, got: " result))
    (is (contains? kinds :missing-member)
        (str "expected :missing-member, got kinds=" kinds))))

;; ---------------------------------------------------------------------------
;; Failed-concurrent mutations must not contribute to uncertainty (codex P1)
;; ---------------------------------------------------------------------------

(deftest failed-concurrent-zrem-does-not-relax-must-be-present
  ;; codex P1: a concurrent ZREM that completes with :fail did NOT take
  ;; effect. Its window must NOT make the member's presence uncertain,
  ;; so a read that omits the member (which was ZADDed and committed
  ;; beforehand) must be flagged as :missing-member.
  (let [history [;; ZADD m1 committed before the read.
                 {:type :invoke :process 0 :f :zadd :value ["m1" 1] :index 0}
                 {:type :ok     :process 0 :f :zadd :value ["m1" 1] :index 1}
                 ;; ZREM m1 is invoked concurrently with the read but
                 ;; ultimately :fails -- the op definitively did NOT run.
                 {:type :invoke :process 1 :f :zrem :value "m1" :index 2}
                 {:type :invoke :process 0 :f :zrange-all :index 3}
                 ;; Read observes m1 ABSENT -- without the fix, the
                 ;; failed ZREM would admit this as "possibly removed".
                 {:type :ok     :process 0 :f :zrange-all :value [] :index 4}
                 {:type :fail   :process 1 :f :zrem :value "m1" :index 5}]
        result  (run-checker history)
        kinds   (set (map :kind (:first-errors result)))]
    (is (not (:valid? result))
        (str "expected :missing-member despite failed ZREM, got: " result))
    (is (contains? kinds :missing-member)
        (str "expected :missing-member, got kinds=" kinds))))

(deftest failed-concurrent-zadd-does-not-contribute-allowed-score
  ;; codex P1: a concurrent ZADD that completes with :fail did NOT take
  ;; effect. Its score must NOT be added to the allowed set. A read
  ;; observing that score must be flagged as :score-mismatch rather than
  ;; being waved through by the failed ZADD's ghost contribution.
  (let [history [;; ZADD m1 at score 1 committed before the read.
                 {:type :invoke :process 0 :f :zadd :value ["m1" 1] :index 0}
                 {:type :ok     :process 0 :f :zadd :value ["m1" 1] :index 1}
                 ;; Concurrent ZADD m1 at score 42 ultimately :fails.
                 {:type :invoke :process 1 :f :zadd :value ["m1" 42] :index 2}
                 {:type :invoke :process 0 :f :zrange-all :index 3}
                 ;; Read observes score 42 -- only valid if the failed
                 ;; ZADD is (incorrectly) admitted as a possible write.
                 {:type :ok     :process 0 :f :zrange-all
                  :value [["m1" 42.0]] :index 4}
                 {:type :fail   :process 1 :f :zadd :value ["m1" 42] :index 5}]
        result  (run-checker history)
        kinds   (set (map :kind (:first-errors result)))]
    (is (not (:valid? result))
        (str "expected :score-mismatch ignoring failed ZADD, got: " result))
    (is (contains? kinds :score-mismatch)
        (str "expected :score-mismatch, got kinds=" kinds))))

;; ---------------------------------------------------------------------------
;; Chained committed ZINCRBYs: only the linearization-chain tail is a
;; valid final score. Earlier intermediate return values are stale. (codex P1)
;; ---------------------------------------------------------------------------

(deftest chained-committed-zincrby-rejects-stale-intermediate
  ;; codex P1: sequential committed ZINCRBYs form a forced linearization
  ;; chain. The first ZINCRBY's return value is an intermediate that no
  ;; post-chain read may observe. Expect :score-mismatch on the stale
  ;; intermediate.
  (let [history [;; Start with score 1.
                 {:type :invoke :process 0 :f :zadd    :value ["m1" 1]    :index 0}
                 {:type :ok     :process 0 :f :zadd    :value ["m1" 1]    :index 1}
                 ;; ZINCRBY +2 -> ok=3 (committed).
                 {:type :invoke :process 0 :f :zincrby :value ["m1" 2]    :index 2}
                 {:type :ok     :process 0 :f :zincrby :value ["m1" 3.0]  :index 3}
                 ;; ZINCRBY +3 -> ok=6 (committed). Strictly follows the
                 ;; previous ZINCRBY in real time (invoke 4 > complete 3).
                 {:type :invoke :process 0 :f :zincrby :value ["m1" 3]    :index 4}
                 {:type :ok     :process 0 :f :zincrby :value ["m1" 6.0]  :index 5}
                 ;; Read AFTER the whole chain observes the stale
                 ;; intermediate 3.0 -- not admissible under any
                 ;; linearization.
                 {:type :invoke :process 1 :f :zrange-all                 :index 6}
                 {:type :ok     :process 1 :f :zrange-all
                  :value [["m1" 3.0]] :index 7}]
        result  (run-checker history)
        kinds   (set (map :kind (:first-errors result)))]
    (is (not (:valid? result))
        (str "expected stale-intermediate to be flagged, got: " result))
    (is (contains? kinds :score-mismatch)
        (str "expected :score-mismatch, got kinds=" kinds))))

(deftest chained-committed-zincrby-accepts-latest
  ;; codex P1: same history but the read observes the LATEST chain tail
  ;; (6.0) -- accept as valid.
  (let [history [{:type :invoke :process 0 :f :zadd    :value ["m1" 1]    :index 0}
                 {:type :ok     :process 0 :f :zadd    :value ["m1" 1]    :index 1}
                 {:type :invoke :process 0 :f :zincrby :value ["m1" 2]    :index 2}
                 {:type :ok     :process 0 :f :zincrby :value ["m1" 3.0]  :index 3}
                 {:type :invoke :process 0 :f :zincrby :value ["m1" 3]    :index 4}
                 {:type :ok     :process 0 :f :zincrby :value ["m1" 6.0]  :index 5}
                 {:type :invoke :process 1 :f :zrange-all                 :index 6}
                 {:type :ok     :process 1 :f :zrange-all
                  :value [["m1" 6.0]] :index 7}]
        result  (run-checker history)]
    (is (:valid? result)
        (str "expected chain-tail score to be accepted, got: " result))))

(deftest concurrent-zincrby-both-admissible
  ;; codex P1: two overlapping-in-real-time ZINCRBYs whose returned
  ;; scores are BOTH candidate final states under some linearization.
  ;; Read observing either value must be accepted.
  ;; Overlap: A=[inv=2, cmp=5], B=[inv=3, cmp=4].
  (let [base [{:type :invoke :process 0 :f :zadd    :value ["m1" 1]    :index 0}
              {:type :ok     :process 0 :f :zadd    :value ["m1" 1]    :index 1}
              {:type :invoke :process 1 :f :zincrby :value ["m1" 2]    :index 2}
              {:type :invoke :process 2 :f :zincrby :value ["m1" 3]    :index 3}
              ;; B completes first with ok=4 (delta applied to score 1).
              {:type :ok     :process 2 :f :zincrby :value ["m1" 4.0]  :index 4}
              ;; A completes with ok=6 (delta applied after B).
              {:type :ok     :process 1 :f :zincrby :value ["m1" 6.0]  :index 5}]
        read-a (conj base
                 {:type :invoke :process 3 :f :zrange-all :index 6}
                 {:type :ok     :process 3 :f :zrange-all
                  :value [["m1" 4.0]] :index 7})
        read-b (conj base
                 {:type :invoke :process 3 :f :zrange-all :index 6}
                 {:type :ok     :process 3 :f :zrange-all
                  :value [["m1" 6.0]] :index 7})]
    (is (:valid? (run-checker read-a))
        "expected B's return value (4.0) admissible under overlap")
    (is (:valid? (run-checker read-b))
        "expected A's return value (6.0) admissible under overlap")))

(deftest zadd-resets-zincrby-chain
  ;; codex P1: a committed ZADD between ZINCRBYs resets the chain --
  ;; subsequent ZINCRBYs operate on the new ZADD'd value. The pre-reset
  ;; ZINCRBY score is NOT a valid read after the chain completes.
  (let [base [;; ZADD m1 1
              {:type :invoke :process 0 :f :zadd    :value ["m1" 1]    :index 0}
              {:type :ok     :process 0 :f :zadd    :value ["m1" 1]    :index 1}
              ;; ZINCRBY +2 -> 3
              {:type :invoke :process 0 :f :zincrby :value ["m1" 2]    :index 2}
              {:type :ok     :process 0 :f :zincrby :value ["m1" 3.0]  :index 3}
              ;; ZADD m1 10 -- chain reset to absolute value.
              {:type :invoke :process 0 :f :zadd    :value ["m1" 10]   :index 4}
              {:type :ok     :process 0 :f :zadd    :value ["m1" 10]   :index 5}
              ;; ZINCRBY +1 -> 11
              {:type :invoke :process 0 :f :zincrby :value ["m1" 1]    :index 6}
              {:type :ok     :process 0 :f :zincrby :value ["m1" 11.0] :index 7}]
        read-ok (conj base
                  {:type :invoke :process 1 :f :zrange-all :index 8}
                  {:type :ok     :process 1 :f :zrange-all
                   :value [["m1" 11.0]] :index 9})
        read-bad (conj base
                   {:type :invoke :process 1 :f :zrange-all :index 8}
                   {:type :ok     :process 1 :f :zrange-all
                    :value [["m1" 3.0]] :index 9})]
    (is (:valid? (run-checker read-ok))
        "expected post-reset chain tail (11.0) to be accepted")
    (is (not (:valid? (run-checker read-bad)))
        "expected pre-reset intermediate (3.0) to be flagged")))

;; ---------------------------------------------------------------------------
;; unknown-score? gate: restricted to :info ZINCRBYs only. Two concurrent
;; :ok ZINCRBYs with known return values do NOT make the score check
;; unknown -- their return values pin the linearization and the
;; admissible score set is constrained by :scores (candidates + uncertain
;; ok return values). (codex P1)
;; ---------------------------------------------------------------------------

(deftest two-ok-concurrent-zincrbys-reject-impossible-score
  ;; codex P1: two overlapping :ok ZINCRBYs with known return values
  ;; (3 and 6) constrain the admissible post-chain read set to {1,3,6}.
  ;; A read of 999 is impossible under any linearization; the checker
  ;; must flag it as :score-mismatch (no longer swallowed by the old
  ;; "2+ uncertain zincrbys -> unknown-score?" shortcut).
  (let [history [{:type :invoke :process 0 :f :zadd    :value ["m1" 1]    :index 0}
                 {:type :ok     :process 0 :f :zadd    :value ["m1" 1]    :index 1}
                 ;; Two concurrent ZINCRBYs. Windows overlap the read.
                 {:type :invoke :process 1 :f :zincrby :value ["m1" 2]    :index 2}
                 {:type :invoke :process 2 :f :zincrby :value ["m1" 3]    :index 3}
                 {:type :ok     :process 1 :f :zincrby :value ["m1" 3.0]  :index 4}
                 {:type :ok     :process 2 :f :zincrby :value ["m1" 6.0]  :index 5}
                 ;; Read observes an impossible score.
                 {:type :invoke :process 3 :f :zrange-all                 :index 6}
                 {:type :ok     :process 3 :f :zrange-all
                  :value [["m1" 999.0]] :index 7}]
        result  (run-checker history)
        kinds   (set (map :kind (:first-errors result)))]
    (is (not (:valid? result))
        (str "expected impossible score to be flagged, got: " result))
    (is (contains? kinds :score-mismatch)
        (str "expected :score-mismatch, got kinds=" kinds))))

(deftest two-ok-concurrent-zincrbys-accept-known-chain-tail
  ;; codex P1: same concurrent :ok ZINCRBY history, but the read
  ;; observes one of the recorded return values. Both 3.0 (linearization
  ;; where +3 ran first, then +2) and 6.0 (the other order) must be
  ;; accepted as valid.
  (let [base [{:type :invoke :process 0 :f :zadd    :value ["m1" 1]    :index 0}
              {:type :ok     :process 0 :f :zadd    :value ["m1" 1]    :index 1}
              {:type :invoke :process 1 :f :zincrby :value ["m1" 2]    :index 2}
              {:type :invoke :process 2 :f :zincrby :value ["m1" 3]    :index 3}
              {:type :ok     :process 1 :f :zincrby :value ["m1" 3.0]  :index 4}
              {:type :ok     :process 2 :f :zincrby :value ["m1" 6.0]  :index 5}]
        read-6 (conj base
                 {:type :invoke :process 3 :f :zrange-all :index 6}
                 {:type :ok     :process 3 :f :zrange-all
                  :value [["m1" 6.0]] :index 7})
        read-3 (conj base
                 {:type :invoke :process 3 :f :zrange-all :index 6}
                 {:type :ok     :process 3 :f :zrange-all
                  :value [["m1" 3.0]] :index 7})]
    (is (:valid? (run-checker read-6))
        "expected 6.0 (one linearization) to be accepted")
    (is (:valid? (run-checker read-3))
        "expected 3.0 (other linearization) to be accepted")))

(deftest info-plus-ok-concurrent-zincrby-stays-unknown
  ;; codex P1: when at least one concurrent ZINCRBY is :info (unknown
  ;; post-op score), the strict score check must be relaxed regardless
  ;; of how many other :ok ZINCRBYs are concurrent. Any numeric score
  ;; must be accepted for this read.
  (let [history [{:type :invoke :process 0 :f :zadd    :value ["m1" 1]    :index 0}
                 {:type :ok     :process 0 :f :zadd    :value ["m1" 1]    :index 1}
                 ;; One :info ZINCRBY (unknown outcome).
                 {:type :invoke :process 1 :f :zincrby :value ["m1" 2]    :index 2}
                 ;; One :ok ZINCRBY with known return value.
                 {:type :invoke :process 2 :f :zincrby :value ["m1" 3]    :index 3}
                 {:type :ok     :process 2 :f :zincrby :value ["m1" 4.0]  :index 4}
                 {:type :info   :process 1 :f :zincrby :value ["m1" 2]
                  :error "conn reset" :index 5}
                 ;; Read observes an "arbitrary" score -- admissible
                 ;; because the :info ZINCRBY could have produced any
                 ;; post-op state visible to the read.
                 {:type :invoke :process 3 :f :zrange-all                 :index 6}
                 {:type :ok     :process 3 :f :zrange-all
                  :value [["m1" 42.0]] :index 7}]]
    (is (:valid? (run-checker history))
        "expected any score accepted when :info ZINCRBY is concurrent")))

;; ---------------------------------------------------------------------------
;; Infinity score parsing
;; ---------------------------------------------------------------------------

;; ---------------------------------------------------------------------------
;; Client setup! / invoke! robustness (gemini MEDIUM)
;; ---------------------------------------------------------------------------

(deftest setup-bang-tolerates-missing-conn-spec
  ;; gemini MEDIUM: if open! failed to populate :conn-spec (unresolvable
  ;; host, etc.), setup! must NOT throw. Otherwise stale data from a
  ;; prior run stays under zset-key and produces false-positive safety
  ;; failures. The fix logs the skip loudly but returns normally.
  (let [client (workload/->ElastickvRedisZSetSafetyClient {} nil)]
    (is (= client (client/setup! client {}))
        "setup! must return the client (not throw) when :conn-spec is nil")))

(deftest setup-bang-tolerates-unreachable-redis
  ;; gemini MEDIUM: a Redis that can't be reached must surface as a
  ;; logged warn, not an uncaught throw that aborts the whole run. The
  ;; fix catches Throwable in setup!.
  (let [client (workload/->ElastickvRedisZSetSafetyClient
                 {} {:pool {} :spec {:host "127.0.0.1"
                                     :port 1   ; guaranteed unreachable
                                     :timeout-ms 100}})]
    (is (= client (client/setup! client {}))
        "setup! must swallow connection errors and keep the run going")))

(deftest zincrby-invoke-handles-nil-response
  ;; gemini MEDIUM: if car/wcar for ZINCRBY returns nil (error reply
  ;; coerced, unexpected protocol edge), the op must complete as :info
  ;; with a structured :error, not throw NumberFormatException from
  ;; parse-double-safe swallowing (str nil) -> "nil".
  (let [client (workload/->ElastickvRedisZSetSafetyClient
                 {} {:pool {} :spec {:host "localhost" :port 6379
                                     :timeout-ms 100}})
        op     {:type :invoke :f :zincrby :value ["m1" 1.0] :process 0 :index 0}]
    (with-redefs [workload/zincrby! (fn [& _] nil)]
      (let [result (client/invoke! client {} op)]
        (is (= :info (:type result))
            (str "expected :info on nil ZINCRBY reply, got: " result))
        (is (some? (:error result))
            (str "expected :error to be populated, got: " result))))))

(deftest zincrby-invoke-handles-unexpected-response
  ;; gemini MEDIUM: same guard, but for a non-string / non-number reply.
  ;; Must complete :info rather than propagate a parse failure.
  (let [client (workload/->ElastickvRedisZSetSafetyClient
                 {} {:pool {} :spec {:host "localhost" :port 6379
                                     :timeout-ms 100}})
        op     {:type :invoke :f :zincrby :value ["m1" 1.0] :process 0 :index 0}]
    (with-redefs [workload/zincrby! (fn [& _] {:unexpected :map})]
      (let [result (client/invoke! client {} op)]
        (is (= :info (:type result))
            (str "expected :info on unexpected ZINCRBY reply, got: " result))))))

(deftest zincrby-invoke-accepts-numeric-response
  ;; Sanity: some Carmine versions coerce integer scores to longs.
  ;; Must parse cleanly to a Double and complete :ok.
  (let [client (workload/->ElastickvRedisZSetSafetyClient
                 {} {:pool {} :spec {:host "localhost" :port 6379
                                     :timeout-ms 100}})
        op     {:type :invoke :f :zincrby :value ["m1" 1.0] :process 0 :index 0}]
    (with-redefs [workload/zincrby! (fn [& _] 7)]
      (let [result (client/invoke! client {} op)]
        (is (= :ok (:type result))
            (str "expected :ok on numeric reply, got: " result))
        (is (= ["m1" 7.0] (:value result)))))))

(deftest parse-withscores-handles-inf-strings
  ;; gemini HIGH: Redis returns "inf" / "+inf" / "-inf" for infinite
  ;; ZSET scores. Double/parseDouble expects "Infinity"; the workload's
  ;; parser must normalize both encodings instead of throwing.
  (let [flat ["m-pos"  "inf"
              "m-pos2" "+inf"
              "m-neg"  "-inf"
              "m-jvm"  "Infinity"
              "m-num"  "3.5"]
        parsed (#'workload/parse-withscores flat)]
    (is (= [["m-pos"  Double/POSITIVE_INFINITY]
            ["m-pos2" Double/POSITIVE_INFINITY]
            ["m-neg"  Double/NEGATIVE_INFINITY]
            ["m-jvm"  Double/POSITIVE_INFINITY]
            ["m-num"  3.5]]
           parsed))))
