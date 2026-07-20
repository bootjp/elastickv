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

(deftest info-zincrby-allows-derived-score
  ;; ZINCRBY whose response was lost (:info) still has a known delta. A
  ;; read concurrent with such an op may observe the derived score.
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
  ;; A single concurrent :ok ZINCRBY has a known return value. The read
  ;; may observe either the pre-op score or the post-op score, but an
  ;; arbitrary impossible score must still be flagged.
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

(deftest two-concurrent-zincrbys-accept-reachable-prefix
  ;; Prefix-sum ordering matters: with two concurrent ZINCRBYs, the
  ;; intermediate score (pre + one delta) is reachable and must be in
  ;; the enumerated score set.
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
        (str "expected reachable prefix for >=2 concurrent ZINCRBYs, got: " result))))

(deftest no-op-zrem-alone-does-not-false-positive
  ;; CI-observed false positive: a member whose only prior ops are no-op
  ;; ZREMs was classified as :score-mismatch with :allowed #{} instead
  ;; of treated as never-existed (:phantom candidate, empty read -> OK).
  ;; A read that observes NO such member must be accepted as valid.
  (let [history [{:type :invoke :process 0 :f :zrem :value "never-added" :index 0}
                 {:type :invoke :process 1 :f :zrange-all :index 1}
                 {:type :ok     :process 1 :f :zrange-all :value [] :index 2}
                 {:type :ok     :process 0 :f :zrem :value ["never-added" false] :index 3}]
        result  (run-checker history)]
    (is (:valid? result) (str "expected valid, got: " result))))

(deftest no-op-zrem-does-not-admit-impossible-absence
  ;; If ZADD and ZREM overlap, a ZREM returning 0 constrains the ZREM to
  ;; have observed the member absent. With a later-overlapping ZADD that
  ;; completes before the read, the final state must be present; the
  ;; no-op ZREM must not count as a possible deletion.
  (let [history [{:type :invoke :process 0 :f :zadd :value ["m1" 1] :index 0}
                 {:type :invoke :process 1 :f :zrem :value "m1" :index 1}
                 {:type :ok     :process 1 :f :zrem :value ["m1" false] :index 2}
                 {:type :ok     :process 0 :f :zadd :value ["m1" 1] :index 3}
                 {:type :invoke :process 2 :f :zrange-all :index 4}
                 {:type :ok     :process 2 :f :zrange-all :value [] :index 5}]
        result  (run-checker history)
        kinds   (set (map :kind (:first-errors result)))]
    (is (not (:valid? result))
        (str "expected no-op ZREM not to admit absence, got: " result))
    (is (contains? kinds :missing-member)
        (str "expected :missing-member, got kinds=" kinds))))

(deftest no-op-zrem-after-present-member-is-impossible
  ;; ZREM returning 0 is only compatible with the member being absent at
  ;; the ZREM linearization point. If a prior ZADD definitely made the
  ;; member present before ZREM was invoked, a false ZREM reply is an
  ;; impossible successful mutation chain, not permission to keep the
  ;; present state.
  (let [history [{:type :invoke :process 0 :f :zadd :value ["m1" 1] :index 0}
                 {:type :ok     :process 0 :f :zadd :value ["m1" 1] :index 1}
                 {:type :invoke :process 0 :f :zrem :value "m1" :index 2}
                 {:type :ok     :process 0 :f :zrem :value ["m1" false] :index 3}
                 {:type :invoke :process 1 :f :zrange-all :index 4}
                 {:type :ok     :process 1 :f :zrange-all
                  :value [["m1" 1.0]] :index 5}]
        result  (run-checker history)
        kinds   (set (map :kind (:first-errors result)))]
    (is (not (:valid? result))
        (str "expected impossible false ZREM to fail, got: " result))
    (is (contains? kinds :impossible-mutation-chain)
        (str "expected :impossible-mutation-chain, got kinds=" kinds))))

(deftest duplicate-members-are-flagged
  ;; ZRANGE must not return the same member twice.
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
  ;; Two :ok ZADDs for the same member whose
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

(deftest overlapping-base-zadds-are-enumerated-before-tail-zincrby
  ;; Base mutations before a later tail candidate can still overlap each
  ;; other. The checker must enumerate their real-time-consistent orders
  ;; before applying the non-concurrent ZINCRBY tail; sorting the base by
  ;; completion time alone would fix the base at score 1 and reject the
  ;; valid final score 3.
  (let [history [{:type :invoke :process 0 :f :zadd :value ["m1" 1] :index 0}
                 {:type :invoke :process 1 :f :zadd :value ["m1" 2] :index 1}
                 {:type :ok     :process 1 :f :zadd :value ["m1" 2] :index 2}
                 {:type :ok     :process 0 :f :zadd :value ["m1" 1] :index 3}
                 {:type :invoke :process 2 :f :zincrby :value ["m1" 1] :index 4}
                 {:type :ok     :process 2 :f :zincrby :value ["m1" 3.0] :index 5}
                 {:type :invoke :process 3 :f :zrange-all :index 6}
                 {:type :ok     :process 3 :f :zrange-all
                  :value [["m1" 3.0]] :index 7}]
        result  (run-checker history)]
    (is (:valid? result)
        (str "expected base ZADD order score 2 -> ZINCRBY +1 to be valid, got: "
             result))))

(deftest info-before-read-is-considered-uncertain
  ;; An :info mutation that completed before a
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
        (str "expected :info-before-read derived score to be valid, got: " result))))

(deftest pre-read-info-zincrby-can-feed-later-ok-zincrby
  ;; A pre-read :info ZINCRBY that completed before a later :ok ZINCRBY
  ;; may be required to make the later ok reply consistent. Relative
  ;; increments do not supersede prior uncertain increments.
  (let [history [{:type :invoke :process 0 :f :zincrby :value ["m1" 5] :index 0}
                 {:type :info   :process 0 :f :zincrby :value ["m1" 5] :index 1}
                 {:type :invoke :process 1 :f :zincrby :value ["m1" 1] :index 2}
                 {:type :ok     :process 1 :f :zincrby :value ["m1" 6.0] :index 3}
                 {:type :invoke :process 2 :f :zrange-all :index 4}
                 {:type :ok     :process 2 :f :zrange-all
                  :value [["m1" 6.0]] :index 5}]
        result  (run-checker history)]
    (is (:valid? result)
        (str "expected earlier :info increment to remain admissible, got: "
             result))))

(deftest pre-read-info-zadd-can-feed-zincrby-before-later-zadd
  ;; The final ZADD resets the score observed by the read, but it must not
  ;; discard an earlier :info ZADD that is needed to explain an intervening
  ;; committed ZINCRBY reply. This ordering appeared in the scheduled Jepsen
  ;; history: info ZADD 29, ZINCRBY +9 -> 38, then ZADD 35.
  (let [history [{:type :invoke :process 0 :f :zadd :value ["m1" 29] :index 0}
                 {:type :info   :process 0 :f :zadd :value ["m1" 29] :index 1}
                 {:type :invoke :process 1 :f :zincrby :value ["m1" 9] :index 2}
                 {:type :ok     :process 1 :f :zincrby :value ["m1" 38.0] :index 3}
                 {:type :invoke :process 2 :f :zadd :value ["m1" 35] :index 4}
                 {:type :ok     :process 2 :f :zadd :value ["m1" 35] :index 5}
                 {:type :invoke :process 3 :f :zrange-all :index 6}
                 {:type :ok     :process 3 :f :zrange-all
                  :value [["m1" 35.0]] :index 7}]
        result (run-checker history)]
    (is (:valid? result)
        (str "expected dependent :info ZADD to remain admissible, got: "
             result))))

(deftest pre-read-info-zincrby-superseded-by-later-zadd
  ;; A pre-read :info ZINCRBY is uncertainty only until a later committed
  ;; state-changing op strictly follows it before the read. The later ZADD
  ;; overwrites any possible increment outcome, so an arbitrary score must
  ;; be rejected.
  (let [history [{:type :invoke :process 0 :f :zincrby :value ["m1" 5] :index 0}
                 {:type :info   :process 0 :f :zincrby :value ["m1" 5] :index 1}
                 {:type :invoke :process 1 :f :zadd    :value ["m1" 2] :index 2}
                 {:type :ok     :process 1 :f :zadd    :value ["m1" 2] :index 3}
                 {:type :invoke :process 2 :f :zrange-all :index 4}
                 {:type :ok     :process 2 :f :zrange-all
                  :value [["m1" 42.0]] :index 5}]
        result  (run-checker history)
        kinds   (set (map :kind (:first-errors result)))]
    (is (not (:valid? result))
        (str "expected superseded :info ZINCRBY not to admit score, got: " result))
    (is (contains? kinds :score-mismatch)
        (str "expected :score-mismatch, got kinds=" kinds))))

(deftest pre-read-info-zrem-superseded-by-later-zadd
  ;; Same supersession rule for presence: an :info ZREM completed before a
  ;; later committed ZADD cannot make a post-ZADD read's absence valid.
  (let [history [{:type :invoke :process 0 :f :zrem :value "m1" :index 0}
                 {:type :info   :process 0 :f :zrem :value "m1" :index 1}
                 {:type :invoke :process 1 :f :zadd :value ["m1" 1] :index 2}
                 {:type :ok     :process 1 :f :zadd :value ["m1" 1] :index 3}
                 {:type :invoke :process 2 :f :zrange-all :index 4}
                 {:type :ok     :process 2 :f :zrange-all :value [] :index 5}]
        result  (run-checker history)
        kinds   (set (map :kind (:first-errors result)))]
    (is (not (:valid? result))
        (str "expected superseded :info ZREM not to admit absence, got: " result))
    (is (contains? kinds :missing-member)
        (str "expected :missing-member, got kinds=" kinds))))

;; ---------------------------------------------------------------------------
;; Stale-read / phantom / superseded-committed checks
;; ---------------------------------------------------------------------------

(deftest phantom-member-is-flagged
  ;; A read that observes a member which was never added
  ;; (no ZADD/ZINCRBY/true-ZREM anywhere) must be rejected.
  (let [history [{:type :invoke :process 0 :f :zrange-all :index 0}
                 {:type :ok     :process 0 :f :zrange-all
                  :value [["never-added" 42.0]] :index 1}]
        result  (run-checker history)
        kinds   (set (map :kind (:first-errors result)))]
    (is (not (:valid? result)) (str "expected phantom error, got: " result))
    (is (contains? kinds :unexpected-presence)
        (str "expected :unexpected-presence, got kinds=" kinds))))

(deftest phantom-from-info-zrem-still-flagged
  ;; An :info ZREM is the ONLY history contact
  ;; with a member (no ZADD/ZINCRBY ever). Because completed-mutation-
  ;; window defaults :removed? to true on :info ZREMs (for uncertainty
  ;; accounting), the checker must NOT treat ZREM as proof the member
  ;; ever existed. A read observing the member present must be flagged
  ;; as :unexpected-presence. Since setup! clears the key at test
  ;; start, every observed member must trace back to a successful (or
  ;; in-flight) ZADD/ZINCRBY -- never to a ZREM.
  (let [history [;; ZREM of a member that was never added. Invoked
                 ;; concurrently with the read, response eventually
                 ;; lost (:info). No ZADD/ZINCRBY anywhere in history.
                 {:type :invoke :process 0 :f :zrem  :value "phantom" :index 0}
                 {:type :invoke :process 1 :f :zrange-all :index 1}
                 ;; Read observes the phantom present at some score.
                 {:type :ok     :process 1 :f :zrange-all
                  :value [["phantom" 7.0]] :index 2}
                 {:type :info   :process 0 :f :zrem  :value "phantom" :index 3}]
        result  (run-checker history)
        kinds   (set (map :kind (:first-errors result)))]
    (is (not (:valid? result))
        (str "expected :unexpected-presence for phantom, got: " result))
    (is (contains? kinds :unexpected-presence)
        (str "expected :unexpected-presence, got kinds=" kinds))))

(deftest stale-read-after-committed-zrem-is-flagged
  ;; Once a ZADD and a subsequent real (:removed? true) ZREM
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
  ;; A ZADD committed BEFORE another ZADD for the same
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
;; Linearization of concurrent ops / uncertain mutations
;; ---------------------------------------------------------------------------

(deftest true-zrem-constrains-overlapping-zadd-order
  ;; ZADD and ZREM for the same member whose invoke/complete
  ;; windows overlap (both commit before the read) are not enough to admit
  ;; either final state. ZREM returning true proves it observed the member
  ;; present, so with an initially absent member it must serialize after
  ;; the ZADD and the final state must be absent.
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
    (is (not (:valid? (run-checker hist-present)))
        "expected read observing ZADD's outcome to be rejected")
    (is (:valid? (run-checker hist-absent))
        "expected read observing ZREM's outcome (absent) to be accepted")))

(deftest info-zrem-concurrent-with-read-allows-missing-member
  ;; An :info ZREM that might have applied before a read
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
  ;; A pre-read :info / concurrent ZINCRBY leaves the
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

(deftest missing-member-range-error-reports-full-allowed-score-set
  ;; When a member is missing from ZRANGEBYSCORE and multiple
  ;; concurrent writers make several scores admissible, the error map
  ;; must surface the FULL admissible set under :allowed (matching
  ;; :score-mismatch-range convention) rather than pick an arbitrary
  ;; single :expected-score.
  (let [history [;; Two concurrent ZADDs for m1, both committed before
                 ;; the read. Either score (5 or 6) is admissible, both
                 ;; fall inside [0, 10].
                 {:type :invoke :process 0 :f :zadd :value ["m1" 5] :index 0}
                 {:type :invoke :process 1 :f :zadd :value ["m1" 6] :index 1}
                 {:type :ok     :process 0 :f :zadd :value ["m1" 5] :index 2}
                 {:type :ok     :process 1 :f :zadd :value ["m1" 6] :index 3}
                 ;; Read sees nothing -- m1 must appear under any
                 ;; admissible linearization, so :missing-member-range
                 ;; fires.
                 {:type :invoke :process 2 :f :zrangebyscore :value [0.0 10.0] :index 4}
                 {:type :ok     :process 2 :f :zrangebyscore
                  :value {:bounds [0.0 10.0] :members []} :index 5}]
        result  (run-checker history)
        miss    (first (filter #(= :missing-member-range (:kind %))
                               (:first-errors result)))]
    (is (not (:valid? result)))
    (is (some? miss)
        (str "expected a :missing-member-range error, got: " (:first-errors result)))
    (is (contains? miss :allowed)
        (str "error map must include :allowed, got: " miss))
    (is (= #{5.0 6.0} (set (:allowed miss)))
        (str "expected :allowed to contain both admissible scores, got: " miss))
    ;; :expected-score is retained for backcompat but MUST be nil when
    ;; there is more than one admissible score, to avoid misleading
    ;; consumers that read it.
    (is (nil? (:expected-score miss))
        (str "expected :expected-score nil for multi-score set, got: " miss))))

(deftest missing-member-range-error-keeps-expected-score-when-single
  ;; Backcompat: when the admissible set has exactly one score,
  ;; :expected-score matches it.
  (let [history [{:type :invoke :process 0 :f :zadd :value ["m1" 5] :index 0}
                 {:type :ok     :process 0 :f :zadd :value ["m1" 5] :index 1}
                 {:type :invoke :process 0 :f :zrangebyscore :value [0.0 10.0] :index 2}
                 {:type :ok     :process 0 :f :zrangebyscore
                  :value {:bounds [0.0 10.0] :members []} :index 3}]
        result  (run-checker history)
        miss    (first (filter #(= :missing-member-range (:kind %))
                               (:first-errors result)))]
    (is (some? miss))
    (is (= #{5.0} (set (:allowed miss))))
    (is (= 5.0 (:expected-score miss)))))

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
;; Failed-concurrent mutations must not contribute to uncertainty
;; ---------------------------------------------------------------------------

(deftest failed-concurrent-zrem-does-not-relax-must-be-present
  ;; A concurrent ZREM that completes with :fail did NOT take
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
  ;; A concurrent ZADD that completes with :fail did NOT take
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
;; valid final score. Earlier intermediate return values are stale.
;; ---------------------------------------------------------------------------

(deftest chained-committed-zincrby-rejects-stale-intermediate
  ;; Sequential committed ZINCRBYs form a forced linearization
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
  ;; Same history but the read observes the LATEST chain tail
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

(deftest committed-zincrby-return-values-constrain-final-score
  ;; Two overlapping-in-real-time ZINCRBYs can both be candidates, but
  ;; their returned scores still constrain the serialization. Here B
  ;; returns 4.0 and A returns 6.0; A's result proves B was an
  ;; intermediate prefix, so a post-completion read of 4.0 is stale while
  ;; 6.0 is valid.
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
    (is (not (:valid? (run-checker read-a)))
        "expected B's intermediate return value (4.0) to be rejected")
    (is (:valid? (run-checker read-b))
        "expected A's final return value (6.0) to be accepted")))

(deftest zadd-resets-zincrby-chain
  ;; A committed ZADD between ZINCRBYs resets the chain --
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

(deftest overlapping-zadd-and-zincrby-respect-return-value-order
  ;; ZINCRBY +1 returning 1.0 proves it ran from an absent/zero score. If
  ;; an overlapping ZADD 10.0 also commits before the read, the only valid
  ;; order is ZINCRBY then ZADD, so the final readable score is 10.0.
  (let [base [{:type :invoke :process 0 :f :zincrby :value ["m1" 1]    :index 0}
              {:type :invoke :process 1 :f :zadd    :value ["m1" 10]   :index 1}
              {:type :ok     :process 0 :f :zincrby :value ["m1" 1.0]  :index 2}
              {:type :ok     :process 1 :f :zadd    :value ["m1" 10]   :index 3}]
        read-zadd (conj base
                    {:type :invoke :process 2 :f :zrange-all :index 4}
                    {:type :ok     :process 2 :f :zrange-all
                     :value [["m1" 10.0]] :index 5})
        read-incr (conj base
                    {:type :invoke :process 2 :f :zrange-all :index 4}
                    {:type :ok     :process 2 :f :zrange-all
                     :value [["m1" 1.0]] :index 5})]
    (is (:valid? (run-checker read-zadd))
        "expected ZADD's final score to be accepted")
    (is (not (:valid? (run-checker read-incr)))
        "expected ZINCRBY's pre-ZADD return to be rejected")))

(deftest zincrby-return-value-must-match-prior-state
  ;; A non-concurrent ZINCRBY's ok reply must equal prior-score + delta.
  ;; The checker must not trust an impossible reply as the member's state.
  (let [history [{:type :invoke :process 0 :f :zadd    :value ["m1" 1]     :index 0}
                 {:type :ok     :process 0 :f :zadd    :value ["m1" 1]     :index 1}
                 {:type :invoke :process 0 :f :zincrby :value ["m1" 5]     :index 2}
                 {:type :ok     :process 0 :f :zincrby :value ["m1" 999.0] :index 3}
                 {:type :invoke :process 1 :f :zrange-all                  :index 4}
                 {:type :ok     :process 1 :f :zrange-all
                  :value [["m1" 999.0]] :index 5}]
        result  (run-checker history)]
    (is (not (:valid? result))
        (str "expected impossible ZINCRBY reply to be rejected, got: " result))))

(deftest impossible-mutation-chain-fails-even-when-read-is-empty
  ;; Empty state sets from successful mutation replies are checker
  ;; failures, not absent members. Otherwise a bad ZINCRBY reply can be
  ;; dropped from the model and an empty read can falsely pass.
  (let [history [{:type :invoke :process 0 :f :zadd    :value ["m1" 1]     :index 0}
                 {:type :ok     :process 0 :f :zadd    :value ["m1" 1]     :index 1}
                 {:type :invoke :process 0 :f :zincrby :value ["m1" 5]     :index 2}
                 {:type :ok     :process 0 :f :zincrby :value ["m1" 999.0] :index 3}
                 {:type :invoke :process 1 :f :zrange-all                  :index 4}
                 {:type :ok     :process 1 :f :zrange-all :value []        :index 5}]
        result  (run-checker history)
        kinds   (set (map :kind (:first-errors result)))]
    (is (not (:valid? result))
        (str "expected impossible chain to fail even on empty read, got: "
             result))
    (is (contains? kinds :impossible-mutation-chain)
        (str "expected :impossible-mutation-chain, got kinds=" kinds))))

(deftest negative-zincrby-tail-remains-admissible
  ;; Negative deltas can make a later valid tail numerically lower than an
  ;; earlier return. Pairwise score pruning must not discard that final tail.
  (let [base [{:type :invoke :process 0 :f :zadd    :value ["m1" 5]    :index 0}
              {:type :ok     :process 0 :f :zadd    :value ["m1" 5]    :index 1}
              {:type :invoke :process 1 :f :zincrby :value ["m1" -2]   :index 2}
              {:type :invoke :process 2 :f :zincrby :value ["m1" -3]   :index 3}
              {:type :ok     :process 1 :f :zincrby :value ["m1" 3.0]  :index 4}
              {:type :ok     :process 2 :f :zincrby :value ["m1" 0.0]  :index 5}]
        read-tail (conj base
                    {:type :invoke :process 3 :f :zrange-all :index 6}
                    {:type :ok     :process 3 :f :zrange-all
                     :value [["m1" 0.0]] :index 7})
        read-prefix (conj base
                      {:type :invoke :process 3 :f :zrange-all :index 6}
                      {:type :ok     :process 3 :f :zrange-all
                       :value [["m1" 3.0]] :index 7})]
    (is (:valid? (run-checker read-tail))
        "expected negative-delta final tail to be accepted")
    (is (not (:valid? (run-checker read-prefix)))
        "expected intermediate negative-delta return to be rejected")))

(deftest pre-read-info-zincrby-does-not-admit-arbitrary-score
  ;; A completed-before-read :info ZINCRBY may have happened or not, but
  ;; its delta still bounds the possible scores when no later uncertainty
  ;; exists.
  (let [history [{:type :invoke :process 0 :f :zadd    :value ["m1" 1]  :index 0}
                 {:type :ok     :process 0 :f :zadd    :value ["m1" 1]  :index 1}
                 {:type :invoke :process 1 :f :zincrby :value ["m1" 5]  :index 2}
                 {:type :info   :process 1 :f :zincrby :value ["m1" 5]  :index 3}
                 {:type :invoke :process 2 :f :zrange-all               :index 4}
                 {:type :ok     :process 2 :f :zrange-all
                  :value [["m1" 42.0]] :index 5}]
        result  (run-checker history)
        kinds   (set (map :kind (:first-errors result)))]
    (is (not (:valid? result))
        (str "expected arbitrary pre-read :info ZINCRBY score rejected, got: " result))
    (is (contains? kinds :score-mismatch)
        (str "expected :score-mismatch, got kinds=" kinds))))

;; ---------------------------------------------------------------------------
;; ZINCRBY return values pin the linearization. Concurrent :ok ZINCRBYs
;; add only the scores reachable under a real-time-consistent order.
;; ---------------------------------------------------------------------------

(deftest two-ok-concurrent-zincrbys-reject-impossible-score
  ;; Two overlapping :ok ZINCRBYs with known return values
  ;; (3 and 6) constrain the admissible post-chain read set to {1,3,6}.
  ;; A read of 999 is impossible under any linearization; the checker
  ;; must flag it as :score-mismatch.
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

(deftest two-ok-concurrent-zincrbys-reject-superseded-return
  ;; Same concurrent :ok ZINCRBY history, but both ops complete before the
  ;; read. The returned scores identify 3.0 as an intermediate prefix and
  ;; 6.0 as the chain tail.
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
    (is (not (:valid? (run-checker read-3)))
        "expected 3.0 intermediate return value to be rejected")))

(deftest overlapping-base-mutation-is-not-forced-before-tail-candidates
  ;; The first ZINCRBY completes before the read and before the second
  ;; increment starts, but it still overlaps the earlier ZADD. It must stay in
  ;; the same real-time enumeration as that ZADD; forcing it into a fixed base
  ;; prefix would apply it to the absent state and reject this valid history.
  (let [history [{:type :invoke :process 0 :f :zadd    :value ["m1" 1]   :index 0}
                 {:type :invoke :process 1 :f :zincrby :value ["m1" 1]   :index 1}
                 {:type :ok     :process 1 :f :zincrby :value ["m1" 2.0] :index 2}
                 {:type :invoke :process 2 :f :zincrby :value ["m1" 1]   :index 3}
                 {:type :ok     :process 2 :f :zincrby :value ["m1" 3.0] :index 4}
                 {:type :ok     :process 0 :f :zadd    :value ["m1" 1]   :index 5}
                 {:type :invoke :process 3 :f :zrange-all                :index 6}
                 {:type :ok     :process 3 :f :zrange-all
                  :value [["m1" 3.0]] :index 7}]]
    (is (:valid? (run-checker history))
        "expected overlapping base mutation to remain reorderable with the tail")))

(deftest later-read-cannot-switch-overlapping-write-order
  ;; Once all ambiguous writes completed before the first read, later reads
  ;; cannot independently choose a different serialization unless another
  ;; mutation could have taken effect between the reads.
  (let [history [{:type :invoke :process 0 :f :zadd       :value ["m1" 1] :index 0}
                 {:type :invoke :process 1 :f :zadd       :value ["m1" 2] :index 1}
                 {:type :ok     :process 0 :f :zadd       :value ["m1" 1] :index 2}
                 {:type :ok     :process 1 :f :zadd       :value ["m1" 2] :index 3}
                 {:type :invoke :process 2 :f :zrange-all                 :index 4}
                 {:type :ok     :process 2 :f :zrange-all
                  :value [["m1" 2.0]] :index 5}
                 {:type :invoke :process 3 :f :zrange-all                 :index 6}
                 {:type :ok     :process 3 :f :zrange-all
                  :value [["m1" 1.0]] :index 7}]
        result  (run-checker history)
        kinds   (set (map :kind (:first-errors result)))]
    (is (not (:valid? result))
        (str "expected read order switch to be rejected, got: " result))
    (is (contains? kinds :unstable-read-without-mutation)
        (str "expected :unstable-read-without-mutation, got kinds=" kinds))))

(deftest range-reads-cannot-switch-overlapping-write-order
  ;; Stability is not just a full-read property. Two non-overlapping bounded
  ;; reads cannot pick different serializations of the same completed writes.
  (let [history [{:type :invoke :process 0 :f :zadd         :value ["m1" 1] :index 0}
                 {:type :invoke :process 1 :f :zadd         :value ["m1" 2] :index 1}
                 {:type :ok     :process 0 :f :zadd         :value ["m1" 1] :index 2}
                 {:type :ok     :process 1 :f :zadd         :value ["m1" 2] :index 3}
                 {:type :invoke :process 2 :f :zrangebyscore
                  :value [2.0 2.0] :index 4}
                 {:type :ok     :process 2 :f :zrangebyscore
                  :value {:bounds [2.0 2.0]
                          :members [["m1" 2.0]]} :index 5}
                 {:type :invoke :process 3 :f :zrangebyscore
                  :value [1.0 1.0] :index 6}
                 {:type :ok     :process 3 :f :zrangebyscore
                  :value {:bounds [1.0 1.0]
                          :members [["m1" 1.0]]} :index 7}]
        result  (run-checker history)
        kinds   (set (map :kind (:first-errors result)))]
    (is (not (:valid? result))
        (str "expected range read order switch to be rejected, got: " result))
    (is (contains? kinds :unstable-read-without-mutation)
        (str "expected :unstable-read-without-mutation, got kinds=" kinds))))

(deftest range-read-stability-detects-first-only-member-omission
  ;; A later range read with the same bounds cannot drop a member observed by
  ;; an earlier non-overlapping range read unless a mutation could affect that
  ;; member between the reads.
  (let [history [{:type :invoke :process 0 :f :zadd         :value ["m1" 1] :index 0}
                 {:type :ok     :process 0 :f :zadd         :value ["m1" 1] :index 1}
                 {:type :invoke :process 1 :f :zrem         :value "m1"     :index 2}
                 {:type :info   :process 1 :f :zrem
                  :value "m1" :error "conn reset" :index 3}
                 {:type :invoke :process 2 :f :zrangebyscore
                  :value [0.0 10.0] :index 4}
                 {:type :ok     :process 2 :f :zrangebyscore
                  :value {:bounds [0.0 10.0]
                          :members [["m1" 1.0]]} :index 5}
                 {:type :invoke :process 3 :f :zrangebyscore
                  :value [0.0 10.0] :index 6}
                 {:type :ok     :process 3 :f :zrangebyscore
                  :value {:bounds [0.0 10.0]
                          :members []} :index 7}]
        result  (run-checker history)
        kinds   (set (map :kind (:first-errors result)))]
    (is (not (:valid? result))
        (str "expected first-only range member omission rejected, got: " result))
    (is (contains? kinds :unstable-read-without-mutation)
        (str "expected :unstable-read-without-mutation, got kinds=" kinds))))

(deftest zrange-all-uses-one-prefix-across-members
  ;; Seeing m1's concurrent ZADD forces the read prefix past any successful
  ;; mutation that completed before that ZADD was invoked. Omitting m2 would
  ;; combine two different prefixes in one full snapshot.
  (let [history [{:type :invoke :process 0 :f :zadd       :value ["m2" 2] :index 0}
                 {:type :invoke :process 1 :f :zrange-all                 :index 1}
                 {:type :ok     :process 0 :f :zadd       :value ["m2" 2] :index 2}
                 {:type :invoke :process 2 :f :zadd       :value ["m1" 1] :index 3}
                 {:type :ok     :process 2 :f :zadd       :value ["m1" 1] :index 4}
                 {:type :ok     :process 1 :f :zrange-all
                  :value [["m1" 1.0]] :index 5}]
        result  (run-checker history)
        kinds   (set (map :kind (:first-errors result)))]
    (is (not (:valid? result))
        (str "expected fractured full read to be rejected, got: " result))
    (is (contains? kinds :fractured-read-prefix)
        (str "expected :fractured-read-prefix, got kinds=" kinds))))

(deftest zrangebyscore-uses-one-prefix-across-members
  ;; The same prefix rule applies to range reads when the omitted predecessor
  ;; is forced to be present inside the requested score bounds.
  (let [history [{:type :invoke :process 0 :f :zadd         :value ["m2" 2] :index 0}
                 {:type :invoke :process 1 :f :zrangebyscore
                  :value [0.0 10.0] :index 1}
                 {:type :ok     :process 0 :f :zadd         :value ["m2" 2] :index 2}
                 {:type :invoke :process 2 :f :zadd         :value ["m1" 1] :index 3}
                 {:type :ok     :process 2 :f :zadd         :value ["m1" 1] :index 4}
                 {:type :ok     :process 1 :f :zrangebyscore
                  :value {:bounds [0.0 10.0]
                          :members [["m1" 1.0]]} :index 5}]
        result  (run-checker history)
        kinds   (set (map :kind (:first-errors result)))]
    (is (not (:valid? result))
        (str "expected fractured range read to be rejected, got: " result))
    (is (contains? kinds :fractured-read-prefix-range)
        (str "expected :fractured-read-prefix-range, got kinds=" kinds))))

(deftest interchangeable-writers-anchor-common-predecessor
  ;; The observed score can be produced by either of two overlapping writers,
  ;; so neither writer is individually required. Since every possible writer
  ;; real-time follows m2, observing m1 still forces m2 into the same prefix.
  (let [history [{:type :invoke :process 0 :f :zadd       :value ["m2" 2] :index 0}
                 {:type :invoke :process 1 :f :zrange-all                 :index 1}
                 {:type :ok     :process 0 :f :zadd       :value ["m2" 2] :index 2}
                 {:type :invoke :process 2 :f :zadd       :value ["m1" 1] :index 3}
                 {:type :invoke :process 3 :f :zadd       :value ["m1" 1] :index 4}
                 {:type :ok     :process 2 :f :zadd       :value ["m1" 1] :index 5}
                 {:type :ok     :process 3 :f :zadd       :value ["m1" 1] :index 6}
                 {:type :ok     :process 1 :f :zrange-all
                  :value [["m1" 1.0]] :index 7}]
        result  (run-checker history)
        kinds   (set (map :kind (:first-errors result)))]
    (is (not (:valid? result))
        (str "expected interchangeable-writer prefix violation, got: " result))
    (is (contains? kinds :fractured-read-prefix)
        (str "expected :fractured-read-prefix, got kinds=" kinds))))

(deftest zrangebyscore-prefix-check-ignores-predecessor-outside-bounds
  ;; A visible concurrent write still anchors the read prefix, but a
  ;; predecessor whose forced state is outside the requested score bounds
  ;; does not have to appear in the range result.
  (let [history [{:type :invoke :process 0 :f :zadd         :value ["m2" -100] :index 0}
                 {:type :invoke :process 1 :f :zrangebyscore
                  :value [0.0 10.0] :index 1}
                 {:type :ok     :process 0 :f :zadd         :value ["m2" -100] :index 2}
                 {:type :invoke :process 2 :f :zadd         :value ["m1" 1] :index 3}
                 {:type :ok     :process 2 :f :zadd         :value ["m1" 1] :index 4}
                 {:type :ok     :process 1 :f :zrangebyscore
                  :value {:bounds [0.0 10.0]
                          :members [["m1" 1.0]]} :index 5}]]
    (is (:valid? (run-checker history)))))

(deftest zrangebyscore-prefix-check-detects-stale-in-range-predecessor
  ;; A predecessor may be optional for omission because its forced state is
  ;; outside the requested bounds, but if the read returns that predecessor at
  ;; an older in-range score it is still a fractured snapshot.
  (let [history [{:type :invoke :process 0 :f :zadd         :value ["m2" 5] :index 0}
                 {:type :invoke :process 1 :f :zrangebyscore
                  :value [0.0 10.0] :index 1}
                 {:type :ok     :process 0 :f :zadd         :value ["m2" 5] :index 2}
                 {:type :invoke :process 2 :f :zadd         :value ["m2" 50] :index 3}
                 {:type :ok     :process 2 :f :zadd         :value ["m2" 50] :index 4}
                 {:type :invoke :process 3 :f :zadd         :value ["m1" 1] :index 5}
                 {:type :ok     :process 3 :f :zadd         :value ["m1" 1] :index 6}
                 {:type :ok     :process 1 :f :zrangebyscore
                  :value {:bounds [0.0 10.0]
                          :members [["m1" 1.0] ["m2" 5.0]]} :index 7}]
        result  (run-checker history)
        kinds   (set (map :kind (:first-errors result)))]
    (is (not (:valid? result))
        (str "expected stale in-range predecessor to be rejected, got: " result))
    (is (contains? kinds :fractured-read-prefix-range)
        (str "expected :fractured-read-prefix-range, got kinds=" kinds))))

(deftest zrangebyscore-score-change-omission-anchors-prefix-check
  ;; A range read can omit a member because a concurrent score change moved it
  ;; out of bounds. Seeing that omission forces real-time predecessors of the
  ;; score change into the same range prefix.
  (let [history [{:type :invoke :process 0 :f :zadd         :value ["m1" 5] :index 0}
                 {:type :ok     :process 0 :f :zadd         :value ["m1" 5] :index 1}
                 {:type :invoke :process 1 :f :zrangebyscore
                  :value [0.0 10.0] :index 2}
                 {:type :invoke :process 2 :f :zadd         :value ["m2" 2] :index 3}
                 {:type :ok     :process 2 :f :zadd         :value ["m2" 2] :index 4}
                 {:type :invoke :process 3 :f :zadd         :value ["m1" 50] :index 5}
                 {:type :ok     :process 3 :f :zadd         :value ["m1" 50] :index 6}
                 {:type :ok     :process 1 :f :zrangebyscore
                  :value {:bounds [0.0 10.0]
                          :members []} :index 7}]
        result  (run-checker history)
        kinds   (set (map :kind (:first-errors result)))]
    (is (not (:valid? result))
        (str "expected score-change omission prefix violation, got: " result))
    (is (contains? kinds :fractured-read-prefix-range)
        (str "expected :fractured-read-prefix-range, got kinds=" kinds))))

(deftest interchangeable-score-changes-anchor-absence-prefix-check
  ;; If either of two score changes can explain a range omission, neither
  ;; mutation is individually required. The group is still required, so common
  ;; real-time predecessors must share the same read prefix.
  (let [history [{:type :invoke :process 0 :f :zadd         :value ["m1" 5] :index 0}
                 {:type :ok     :process 0 :f :zadd         :value ["m1" 5] :index 1}
                 {:type :invoke :process 1 :f :zrangebyscore
                  :value [0.0 10.0] :index 2}
                 {:type :invoke :process 2 :f :zadd         :value ["m2" 2] :index 3}
                 {:type :ok     :process 2 :f :zadd         :value ["m2" 2] :index 4}
                 {:type :invoke :process 3 :f :zadd         :value ["m1" 50] :index 5}
                 {:type :invoke :process 4 :f :zadd         :value ["m1" 60] :index 6}
                 {:type :ok     :process 3 :f :zadd         :value ["m1" 50] :index 7}
                 {:type :ok     :process 4 :f :zadd         :value ["m1" 60] :index 8}
                 {:type :ok     :process 1 :f :zrangebyscore
                  :value {:bounds [0.0 10.0]
                          :members []} :index 9}]
        result  (run-checker history)
        kinds   (set (map :kind (:first-errors result)))]
    (is (not (:valid? result))
        (str "expected interchangeable score-change prefix violation, got: "
             result))
    (is (contains? kinds :fractured-read-prefix-range)
        (str "expected :fractured-read-prefix-range, got kinds=" kinds))))

(deftest zrem-omission-anchors-prefix-check
  ;; If m1's absence is only explainable by a concurrent ZREM, that visible
  ;; deletion still anchors the read prefix. A predecessor completed before
  ;; the ZREM was invoked must be visible in the same full read.
  (let [history [{:type :invoke :process 0 :f :zadd       :value ["m1" 1] :index 0}
                 {:type :ok     :process 0 :f :zadd       :value ["m1" 1] :index 1}
                 {:type :invoke :process 1 :f :zrange-all                 :index 2}
                 {:type :invoke :process 2 :f :zadd       :value ["m2" 2] :index 3}
                 {:type :ok     :process 2 :f :zadd       :value ["m2" 2] :index 4}
                 {:type :invoke :process 3 :f :zrem       :value "m1"     :index 5}
                 {:type :ok     :process 1 :f :zrange-all
                  :value [] :index 6}
                 {:type :ok     :process 3 :f :zrem
                  :value ["m1" true] :index 7}]
        result  (run-checker history)
        kinds   (set (map :kind (:first-errors result)))]
    (is (not (:valid? result))
        (str "expected ZREM-anchored fractured read to be rejected, got: " result))
    (is (contains? kinds :fractured-read-prefix)
        (str "expected :fractured-read-prefix, got kinds=" kinds))))

(deftest interchangeable-zrems-anchor-absence-prefix-check
  ;; If either of two concurrent ZREMs can explain an omitted member, neither
  ;; delete is individually required. Their common real-time predecessors still
  ;; have to appear in the same read prefix.
  (let [history [{:type :invoke :process 0 :f :zadd       :value ["m1" 1] :index 0}
                 {:type :ok     :process 0 :f :zadd       :value ["m1" 1] :index 1}
                 {:type :invoke :process 1 :f :zrange-all                 :index 2}
                 {:type :invoke :process 2 :f :zadd       :value ["m2" 2] :index 3}
                 {:type :ok     :process 2 :f :zadd       :value ["m2" 2] :index 4}
                 {:type :invoke :process 3 :f :zrem       :value "m1"     :index 5}
                 {:type :invoke :process 4 :f :zrem       :value "m1"     :index 6}
                 {:type :ok     :process 3 :f :zrem
                  :value ["m1" true] :index 7}
                 {:type :ok     :process 4 :f :zrem
                  :value ["m1" true] :index 8}
                 {:type :ok     :process 1 :f :zrange-all
                  :value [] :index 9}]
        result  (run-checker history)
        kinds   (set (map :kind (:first-errors result)))]
    (is (not (:valid? result))
        (str "expected interchangeable ZREM prefix violation, got: " result))
    (is (contains? kinds :fractured-read-prefix)
        (str "expected :fractured-read-prefix, got kinds=" kinds))))

(deftest pre-read-info-zrem-omission-anchors-prefix-check
  ;; A response-lost ZREM that completed before the read can still be the
  ;; visible reason m1 is absent. Its real-time predecessors must share the
  ;; same read prefix even though the ZREM is no longer concurrent.
  (let [history [{:type :invoke :process 0 :f :zadd       :value ["m1" 1] :index 0}
                 {:type :ok     :process 0 :f :zadd       :value ["m1" 1] :index 1}
                 {:type :invoke :process 1 :f :zadd       :value ["m2" 2] :index 2}
                 {:type :ok     :process 1 :f :zadd       :value ["m2" 2] :index 3}
                 {:type :invoke :process 2 :f :zrem       :value "m1"     :index 4}
                 {:type :info   :process 2 :f :zrem
                  :value "m1" :error "conn reset" :index 5}
                 {:type :invoke :process 3 :f :zrange-all                 :index 6}
                 {:type :ok     :process 3 :f :zrange-all
                  :value [] :index 7}]
        result  (run-checker history)
        kinds   (set (map :kind (:first-errors result)))]
    (is (not (:valid? result))
        (str "expected pre-read info ZREM anchored violation, got: " result))
    (is (contains? kinds :fractured-read-prefix)
        (str "expected :fractured-read-prefix, got kinds=" kinds))))

(deftest forced-prefix-detects-stale-member-after-zrem
  ;; If a visible concurrent anchor real-time follows a successful ZREM of a
  ;; predecessor, the forced predecessor state is absent. Returning that
  ;; predecessor at its old score is fractured even though omission would be OK.
  (let [history [{:type :invoke :process 0 :f :zadd       :value ["m2" 1] :index 0}
                 {:type :ok     :process 0 :f :zadd       :value ["m2" 1] :index 1}
                 {:type :invoke :process 1 :f :zrange-all                 :index 2}
                 {:type :invoke :process 2 :f :zrem       :value "m2"     :index 3}
                 {:type :ok     :process 2 :f :zrem
                  :value ["m2" true] :index 4}
                 {:type :invoke :process 3 :f :zadd       :value ["m1" 1] :index 5}
                 {:type :ok     :process 3 :f :zadd       :value ["m1" 1] :index 6}
                 {:type :ok     :process 1 :f :zrange-all
                  :value [["m1" 1.0] ["m2" 1.0]] :index 7}]
        result  (run-checker history)
        kinds   (set (map :kind (:first-errors result)))]
    (is (not (:valid? result))
        (str "expected stale predecessor after forced ZREM, got: " result))
    (is (contains? kinds :fractured-read-prefix)
        (str "expected :fractured-read-prefix, got kinds=" kinds))))

(deftest info-zincrby-visible-score-anchors-prefix-check
  ;; A response-lost ZINCRBY has a known delta. If the observed score is
  ;; reachable only by applying that :info op, it anchors the prefix just like
  ;; a visible concurrent ZADD.
  (let [history [{:type :invoke :process 0 :f :zadd       :value ["m1" 1] :index 0}
                 {:type :ok     :process 0 :f :zadd       :value ["m1" 1] :index 1}
                 {:type :invoke :process 1 :f :zrange-all                 :index 2}
                 {:type :invoke :process 2 :f :zadd       :value ["m2" 2] :index 3}
                 {:type :ok     :process 2 :f :zadd       :value ["m2" 2] :index 4}
                 {:type :invoke :process 3 :f :zincrby    :value ["m1" 5] :index 5}
                 {:type :ok     :process 1 :f :zrange-all
                  :value [["m1" 6.0]] :index 6}
                 {:type :info   :process 3 :f :zincrby
                  :value ["m1" 5] :error "conn reset" :index 7}]
        result  (run-checker history)
        kinds   (set (map :kind (:first-errors result)))]
    (is (not (:valid? result))
        (str "expected :info ZINCRBY-anchored fractured read, got: " result))
    (is (contains? kinds :fractured-read-prefix)
        (str "expected :fractured-read-prefix, got kinds=" kinds))))

(deftest forced-predecessor-state-is-validated-when-present
  ;; Seeing m1's concurrent ZADD forces the read prefix past m2's later ZADD.
  ;; Including m2 at its older score is still fractured; presence alone is not
  ;; sufficient.
  (let [history [{:type :invoke :process 0 :f :zadd       :value ["m2" 1] :index 0}
                 {:type :ok     :process 0 :f :zadd       :value ["m2" 1] :index 1}
                 {:type :invoke :process 1 :f :zrange-all                 :index 2}
                 {:type :invoke :process 2 :f :zadd       :value ["m2" 2] :index 3}
                 {:type :ok     :process 2 :f :zadd       :value ["m2" 2] :index 4}
                 {:type :invoke :process 3 :f :zadd       :value ["m1" 1] :index 5}
                 {:type :ok     :process 3 :f :zadd       :value ["m1" 1] :index 6}
                 {:type :ok     :process 1 :f :zrange-all
                  :value [["m1" 1.0] ["m2" 1.0]] :index 7}]
        result  (run-checker history)
        kinds   (set (map :kind (:first-errors result)))]
    (is (not (:valid? result))
        (str "expected stale forced predecessor to be rejected, got: " result))
    (is (contains? kinds :fractured-read-prefix)
        (str "expected :fractured-read-prefix, got kinds=" kinds))))

(deftest info-plus-ok-zincrby-stays-bounded
  ;; A :info ZINCRBY still has a known delta. Once the surrounding state is
  ;; known, it admits the pre-info state or the delta-applied state, not an
  ;; arbitrary numeric score.
  (let [history [{:type :invoke :process 0 :f :zadd    :value ["m1" 1]    :index 0}
                 {:type :ok     :process 0 :f :zadd    :value ["m1" 1]    :index 1}
                 ;; One :info ZINCRBY (unknown outcome).
                 {:type :invoke :process 1 :f :zincrby :value ["m1" 2]    :index 2}
                 ;; One :ok ZINCRBY with known return value.
                 {:type :invoke :process 2 :f :zincrby :value ["m1" 3]    :index 3}
                 {:type :ok     :process 2 :f :zincrby :value ["m1" 4.0]  :index 4}
                 {:type :info   :process 1 :f :zincrby :value ["m1" 2]
                  :error "conn reset" :index 5}
                 ;; Read observes an arbitrary score: invalid. The possible
                 ;; scores are 4.0 (only the :ok ZINCRBY) or 6.0 (both).
                 {:type :invoke :process 3 :f :zrange-all                 :index 6}
                 {:type :ok     :process 3 :f :zrange-all
                  :value [["m1" 42.0]] :index 7}]]
    (is (not (:valid? (run-checker history)))
        "expected arbitrary score rejected when :info ZINCRBY is bounded")))

;; ---------------------------------------------------------------------------
;; Infinity score parsing
;; ---------------------------------------------------------------------------

;; ---------------------------------------------------------------------------
;; Client setup! / invoke! robustness
;; ---------------------------------------------------------------------------

(deftest setup-bang-hard-fails-when-conn-spec-missing
  ;; If open! failed to populate :conn-spec (unresolvable
  ;; host, etc.), setup! MUST throw rather than silently proceed.
  ;; Continuing with a no-op setup would leave stale data from a prior
  ;; run under zset-key and risk false-positive checker verdicts from
  ;; that dirty state. We want Jepsen to surface the failure.
  (let [client (workload/->ElastickvRedisZSetSafetyClient {} nil)]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #":conn-spec is missing"
                          (client/setup! client {}))
        "setup! must throw ex-info when :conn-spec is nil")))

(deftest setup-bang-hard-fails-when-cleanup-del-errors
  ;; Even when :conn-spec is populated, if the actual
  ;; cleanup (DEL zset-key) fails or errors, setup! must NOT silently
  ;; proceed. Stale data surviving from a prior run under zset-key
  ;; would cause false-positive safety verdicts. Propagate the
  ;; exception so Jepsen aborts the run.
  (let [client (workload/->ElastickvRedisZSetSafetyClient
                 {} {:pool {} :spec {:host "127.0.0.1"
                                     :port 1   ; guaranteed unreachable
                                     :timeout-ms 100}})]
    (is (thrown? Throwable
                 (client/setup! client {}))
        "setup! must propagate cleanup failures, not swallow them")))

(deftest zincrby-invoke-handles-nil-response
  ;; If car/wcar for ZINCRBY returns nil (error reply
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
  ;; Same guard, but for a non-string / non-number reply.
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

(deftest zincrby-invoke-accepts-byte-array-response
  ;; Carmine may surface Redis bulk-string scores as raw bytes depending
  ;; on protocol/config. The client must parse them as UTF-8, not via
  ;; `(str bytes)`, which yields "[B@...".
  (let [client (workload/->ElastickvRedisZSetSafetyClient
                 {} {:pool {} :spec {:host "localhost" :port 6379
                                     :timeout-ms 100}})
        op     {:type :invoke :f :zincrby :value ["m1" 1.0] :process 0 :index 0}]
    (with-redefs [workload/zincrby! (fn [& _] (.getBytes "7.5" "UTF-8"))]
      (let [result (client/invoke! client {} op)]
        (is (= :ok (:type result))
            (str "expected :ok on byte-array reply, got: " result))
        (is (= ["m1" 7.5] (:value result)))))))

;; ---------------------------------------------------------------------------
;; Vacuous-pass guard
;; ---------------------------------------------------------------------------

(deftest empty-history-is-unknown-not-valid
  ;; An empty history (e.g. Redis unreachable, all ops
  ;; downgraded to :info) produces zero successful reads. The checker
  ;; MUST NOT return :valid? true in that case -- that would be a
  ;; false-green. Expect :valid? :unknown plus a diagnostic :reason.
  (let [result (run-checker [])]
    (is (= :unknown (:valid? result))
        (str "expected :unknown on empty history, got: " result))
    (is (string? (:reason result))
        (str "expected :reason to be populated, got: " result))
    (is (zero? (:reads result)))))

(deftest all-info-history-is-unknown-not-valid
  ;; A run where every operation was downgraded to :info
  ;; (Redis unreachable / every read timed out) still has read-pairs
  ;; filtered down to zero :ok reads. Must surface as :valid? :unknown.
  (let [history [{:type :invoke :process 0 :f :zadd       :value ["m1" 1] :index 0}
                 {:type :info   :process 0 :f :zadd       :value ["m1" 1] :index 1
                  :error "conn refused"}
                 {:type :invoke :process 0 :f :zrange-all                 :index 2}
                 {:type :info   :process 0 :f :zrange-all                 :index 3
                  :error "conn refused"}]
        result  (run-checker history)]
    (is (= :unknown (:valid? result))
        (str "expected :unknown when all ops are :info, got: " result))
    (is (string? (:reason result)))))

(deftest one-successful-read-is-enough-to-validate
  ;; Sanity: the vacuous-pass guard must only kick in when there are
  ;; ZERO successful reads. A single :ok read with no errors is a
  ;; legitimate :valid? true.
  (let [history [{:type :invoke :process 0 :f :zrange-all :index 0}
                 {:type :ok     :process 0 :f :zrange-all :value [] :index 1}]
        result  (run-checker history)]
    (is (true? (:valid? result))
        (str "expected :valid? true with one :ok read, got: " result))))

(deftest malformed-zrange-read-is-checker-failure
  ;; A read whose Redis command returned successfully but produced a
  ;; malformed WITHSCORES payload is safety evidence, not a timeout. The
  ;; checker must fail it instead of filtering it out as non-:ok.
  (let [history [{:type :invoke :process 0 :f :zrange-all :index 0}
                 {:type :ok     :process 0 :f :zrange-all
                  :value {:malformed? true
                          :error "WITHSCORES reply has odd element count"
                          :payload ["m1" "1" "dangling"]}
                  :index 1}]
        result  (run-checker history)
        kinds   (set (map :kind (:first-errors result)))]
    (is (not (:valid? result))
        (str "expected malformed ZRANGE read to fail, got: " result))
    (is (contains? kinds :malformed-read)
        (str "expected :malformed-read, got kinds=" kinds))))

(deftest malformed-zrangebyscore-read-is-checker-failure
  (let [history [{:type :invoke :process 0 :f :zrangebyscore
                  :value [0.0 10.0] :index 0}
                 {:type :ok     :process 0 :f :zrangebyscore
                  :value {:bounds [0.0 10.0]
                          :malformed? true
                          :error "WITHSCORES reply has odd element count"
                          :payload ["m1" "1" "dangling"]}
                  :index 1}]
        result  (run-checker history)
        kinds   (set (map :kind (:first-errors result)))]
    (is (not (:valid? result))
        (str "expected malformed ZRANGEBYSCORE read to fail, got: " result))
    (is (contains? kinds :malformed-read-range)
        (str "expected :malformed-read-range, got kinds=" kinds))))

(deftest zrem-invoke-handles-nil-response
  ;; If car/wcar for ZREM returns nil (protocol edge,
  ;; closed connection, etc.), the command may have reached Redis. Treat the
  ;; outcome as indeterminate instead of committing a false no-op ZREM.
  (let [client (workload/->ElastickvRedisZSetSafetyClient
                 {} {:pool {} :spec {:host "localhost" :port 6379
                                     :timeout-ms 100}})
        op     {:type :invoke :f :zrem :value "ghost" :process 0 :index 0}]
    (with-redefs [workload/zrem! (fn [& _] nil)]
      (let [result (client/invoke! client {} op)]
        (is (= :info (:type result))
            (str "expected :info on nil ZREM reply, got: " result))
        (is (= :nil-response (:error result))
            (str "expected nil-response marker, got: " result))))))

(deftest zrem-invoke-handles-numeric-response
  ;; Sanity: ZREM's normal reply is an integer count.
  (let [client (workload/->ElastickvRedisZSetSafetyClient
                 {} {:pool {} :spec {:host "localhost" :port 6379
                                     :timeout-ms 100}})
        op     {:type :invoke :f :zrem :value "m1" :process 0 :index 0}]
    (with-redefs [workload/zrem! (fn [& _] 1)]
      (let [result (client/invoke! client {} op)]
        (is (= :ok (:type result)))
        (is (= ["m1" true] (:value result)))))))

(deftest zrem-invoke-handles-string-response
  ;; Some Carmine versions / RESP3 codepaths surface ZREM's count as a
  ;; numeric string rather than a Long. `(long \"1\")` would throw
  ;; ClassCastException; the coerce-zrem-count helper must parse the
  ;; string and the op must still resolve as :ok with removed? true.
  (let [client (workload/->ElastickvRedisZSetSafetyClient
                 {} {:pool {} :spec {:host "localhost" :port 6379
                                     :timeout-ms 100}})
        op     {:type :invoke :f :zrem :value "m1" :process 0 :index 0}]
    (with-redefs [workload/zrem! (fn [& _] "1")]
      (let [result (client/invoke! client {} op)]
        (is (= :ok (:type result))
            (str "expected :ok on string ZREM reply, got: " result))
        (is (= ["m1" true] (:value result))
            (str "expected removed? true on string \"1\", got: " result))))))

(deftest zrem-invoke-handles-string-zero-response
  ;; String "0" must be parsed as removed? false (not truthy because it
  ;; is a non-empty string).
  (let [client (workload/->ElastickvRedisZSetSafetyClient
                 {} {:pool {} :spec {:host "localhost" :port 6379
                                     :timeout-ms 100}})
        op     {:type :invoke :f :zrem :value "ghost" :process 0 :index 0}]
    (with-redefs [workload/zrem! (fn [& _] "0")]
      (let [result (client/invoke! client {} op)]
        (is (= :ok (:type result)))
        (is (= ["ghost" false] (:value result)))))))

(deftest zrem-invoke-handles-bytes-response
  ;; Raw-bytes numeric reply (RESP binary-safe path) must be decoded as
  ;; UTF-8 and parsed. "1" => removed? true.
  (let [client (workload/->ElastickvRedisZSetSafetyClient
                 {} {:pool {} :spec {:host "localhost" :port 6379
                                     :timeout-ms 100}})
        op     {:type :invoke :f :zrem :value "m1" :process 0 :index 0}]
    (with-redefs [workload/zrem! (fn [& _] (.getBytes "1" "UTF-8"))]
      (let [result (client/invoke! client {} op)]
        (is (= :ok (:type result)))
        (is (= ["m1" true] (:value result)))))))

(deftest zrem-invoke-handles-unparseable-response
  ;; Totally unexpected reply shape: treat as 0 (nothing removed) rather
  ;; than throw. Keeps the op :ok and records removed? false.
  (let [client (workload/->ElastickvRedisZSetSafetyClient
                 {} {:pool {} :spec {:host "localhost" :port 6379
                                     :timeout-ms 100}})
        op     {:type :invoke :f :zrem :value "ghost" :process 0 :index 0}]
    (with-redefs [workload/zrem! (fn [& _] :weird)]
      (let [result (client/invoke! client {} op)]
        (is (= :ok (:type result)))
        (is (= ["ghost" false] (:value result)))))))

(deftest parse-withscores-handles-inf-strings
  ;; Redis returns "inf" / "+inf" / "-inf" for infinite
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

(deftest parse-withscores-rejects-nil-payload
  ;; `count` on nil is 0 in Clojure; nil must still be treated as a
  ;; malformed successful Redis reply, not as an empty ZSET result.
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo
        #"WITHSCORES reply is nil"
        (#'workload/parse-withscores nil))))

(deftest parse-withscores-rejects-odd-length-payload
  ;; A WITHSCORES reply with a dangling member (odd element count) is a
  ;; protocol violation. The checker must surface it rather than let
  ;; `(partition 2)` silently drop evidence of the anomaly.
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo
        #"odd element count"
        (#'workload/parse-withscores ["m1" "1.0" "m2-dangling"]))))
