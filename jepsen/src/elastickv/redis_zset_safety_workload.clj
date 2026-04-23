(ns elastickv.redis-zset-safety-workload
  "Jepsen workload verifying stronger safety properties of elastickv's
  Redis ZSet (sorted set) implementation under faults.

  Beyond the simple visibility check in redis-zset-workload, this workload
  exercises score correctness, ordering, range queries, phantom-member
  freedom, and atomicity of compound ZSet mutations by using a custom,
  model-based Checker.

  Operations (all target a single well-known key):

    {:f :zadd          :value [member score]}   ZADD key score member
    {:f :zincrby       :value [member delta]}   ZINCRBY key delta member
    {:f :zrem          :value member}           ZREM key member
    {:f :zrange-all}                            ZRANGE key 0 -1 WITHSCORES
    {:f :zrangebyscore :value [lo hi]}          ZRANGEBYSCORE key lo hi WITHSCORES

  Semantics checked (see `zset-safety-checker`):

  1. Score correctness: the score of any member observed by a :zrange-all
     read must match the model's latest committed score for that member,
     OR must match a score written by an operation that is concurrent with
     the read (we cannot linearize concurrent writes to the same member,
     so any such \"in-flight\" value is permitted).
  2. Order preservation: the result of :zrange-all must be sorted by
     (score ascending, member lexicographically ascending).
  3. ZRANGEBYSCORE correctness: every member in a score-range read must
     have a latest committed (or concurrent) score within [lo, hi]; and
     every model member with a score in [lo, hi] must either be present
     or be subject to a concurrent mutation.
  4. No phantom members: every member observed by a read must have been
     introduced by some successful (or in-flight) operation.
  5. Atomicity: there is no explicit \"partial\" state to probe from the
     client, but the checker treats every :ok operation as atomic — any
     visible inconsistency (member present with no matching op, score
     disagreeing with any known write, etc.) is reported."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [warn]]
            [elastickv.cli :as cli]
            [elastickv.db :as ekdb]
            [jepsen.db :as jdb]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [net :as net]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control :as control]
            [jepsen.nemesis :as nemesis]
            [jepsen.nemesis.combined :as combined]
            [jepsen.os :as os]
            [jepsen.os.debian :as debian]
            [taoensso.carmine :as car]))

;; ---------------------------------------------------------------------------
;; Constants
;; ---------------------------------------------------------------------------

(def ^:private zset-key "jepsen-zset-safety")

(def default-nodes ["n1" "n2" "n3" "n4" "n5"])

;; A small, fixed universe of members keeps contention high and makes the
;; model's state small enough to enumerate.
(def ^:private members
  (mapv #(str "m" %) (range 16)))

;; ---------------------------------------------------------------------------
;; Client
;; ---------------------------------------------------------------------------

(defn- parse-double-safe
  "Parse a Redis score string into a Double. Redis serializes infinite
  scores as \"inf\" / \"+inf\" / \"-inf\", which Java's Double/parseDouble
  does not accept (it expects \"Infinity\" / \"-Infinity\"). Handle both
  encodings so the checker doesn't throw on infinite ZSET scores."
  [s]
  (let [raw   (str s)
        lower (str/lower-case raw)]
    (cond
      (or (= lower "inf") (= lower "+inf") (= lower "infinity") (= lower "+infinity"))
      Double/POSITIVE_INFINITY

      (or (= lower "-inf") (= lower "-infinity"))
      Double/NEGATIVE_INFINITY

      :else
      (Double/parseDouble raw))))

(defn- coerce-zincrby-score
  "Carmine's ZINCRBY reply is normally a score string, but under error /
  timeout / protocol edge cases it may be nil, a numeric value, or
  something else entirely. Stringifying nil produces \"nil\", which
  parse-double-safe would then hand to Double/parseDouble and throw.
  Explicitly classify the response so the invoke! path can record
  :unknown-response as :info instead of masking it in a catch-all.

  Returns one of:
    [:ok   (double score)]
    [:nil]                    ; nil response
    [:error <string>]         ; Carmine error reply
    [:unexpected <value>]     ; anything else"
  [response]
  (cond
    (nil? response)
    [:nil]

    (number? response)
    [:ok (double response)]

    (string? response)
    (try
      [:ok (parse-double-safe response)]
      (catch NumberFormatException _
        [:unexpected response]))

    ;; Carmine surfaces Redis error replies as exceptions by default,
    ;; but some codepaths wrap them in an ex-info / Throwable value.
    (instance? Throwable response)
    [:error (.getMessage ^Throwable response)]

    :else
    [:unexpected response]))

(defn- parse-withscores
  "Carmine returns a flat [member score member score ...] vector for
  ZRANGE WITHSCORES. Convert to a sorted vector of [member (double score)]
  preserving server-returned order (score ascending, then member)."
  [flat]
  (->> flat
       (partition 2)
       (mapv (fn [[m s]]
               [(if (bytes? m) (String. ^bytes m) (str m))
                (parse-double-safe s)]))))

(defn- zincrby!
  "Executes a ZINCRBY against conn-spec and returns Carmine's raw reply
  (normally a score string). Extracted so tests can stub the Redis call
  without going through the `car/wcar` macro."
  [conn-spec key delta member]
  (car/wcar conn-spec (car/zincrby key (double delta) member)))

(defrecord ElastickvRedisZSetSafetyClient [node->port conn-spec]
  client/Client

  (open! [this test node]
    (let [port (get node->port node 6379)
          host (or (:redis-host test) (name node))]
      (assoc this :conn-spec {:pool {} :spec {:host host
                                              :port port
                                              :timeout-ms 10000}})))

  (close! [this _test] this)

  (setup! [this _test]
    (if-let [cs (:conn-spec this)]
      (try
        (car/wcar cs (car/del zset-key))
        (catch Throwable t
          ;; Do NOT swallow silently: repeated setup! failures across
          ;; runs would leave stale data under zset-key and could
          ;; produce false-positive safety failures. Log loudly so
          ;; operators notice.
          (warn "ZSet safety setup! DEL failed -- stale data may survive"
                "into this run:" (.getMessage t))))
      ;; open! failed to populate :conn-spec (e.g. unresolvable host);
      ;; flag it rather than silently proceeding with a no-op setup.
      (warn "ZSet safety setup! skipped: missing :conn-spec on client;"
            "prior state under" zset-key "may survive into this run"))
    this)

  (teardown! [this _test] this)

  (invoke! [_ _test op]
    (let [cs conn-spec]
      (try
        (case (:f op)
          :zadd
          (let [[member score] (:value op)]
            (car/wcar cs (car/zadd zset-key (double score) member))
            (assoc op :type :ok))

          :zincrby
          (let [[member delta] (:value op)
                new-score (zincrby! cs zset-key delta member)
                [tag v]   (coerce-zincrby-score new-score)]
            (case tag
              :ok         (assoc op :type :ok :value [member v])
              :nil        (do (warn "ZSet safety ZINCRBY returned nil for" member)
                              (assoc op :type :info
                                        :error :nil-response))
              :error      (do (warn "ZSet safety ZINCRBY returned error reply:" v)
                              (assoc op :type :info
                                        :error {:kind :error-response
                                                :message v}))
              :unexpected (do (warn "ZSet safety ZINCRBY returned unexpected reply:" (pr-str v))
                              (assoc op :type :info
                                        :error {:kind :unexpected-response
                                                :value (pr-str v)}))))

          :zrem
          (let [member (:value op)
                removed (car/wcar cs (car/zrem zset-key member))]
            (assoc op :type :ok :value [member (pos? (long removed))]))

          :zrange-all
          (let [flat (car/wcar cs (car/zrange zset-key 0 -1 "WITHSCORES"))]
            (assoc op :type :ok :value (parse-withscores flat)))

          :zrangebyscore
          (let [[lo hi] (:value op)
                flat (car/wcar cs (car/zrangebyscore zset-key
                                                     (double lo)
                                                     (double hi)
                                                     "WITHSCORES"))]
            (assoc op :type :ok :value {:bounds [lo hi]
                                        :members (parse-withscores flat)})))
        (catch Exception e
          (warn "ZSet safety op failed:" (:f op) (.getMessage e))
          (assoc op :type :info :error (.getMessage e)))))))

;; ---------------------------------------------------------------------------
;; Generator
;; ---------------------------------------------------------------------------

(defn- rand-member [] (rand-nth members))

(defn- gen-op []
  (let [roll (rand)]
    (cond
      (< roll 0.35)
      {:f :zadd :value [(rand-member) (double (- (rand-int 200) 100))]}

      (< roll 0.55)
      {:f :zincrby :value [(rand-member)
                           (double (- (rand-int 20) 10))]}

      (< roll 0.65)
      {:f :zrem :value (rand-member)}

      (< roll 0.90)
      {:f :zrange-all}

      :else
      (let [a (- (rand-int 200) 100)
            b (- (rand-int 200) 100)]
        {:f :zrangebyscore :value [(double (min a b)) (double (max a b))]}))))

(defn- op-generator []
  (reify gen/Generator
    (op [this test ctx]
      [(gen/fill-in-op (gen-op) ctx) this])
    (update [this _ _ _] this)))

;; ---------------------------------------------------------------------------
;; Checker
;; ---------------------------------------------------------------------------

(defn- sorted-by-score-then-member?
  "Validates the zset invariant: (score, member) ascending, strict."
  [entries]
  (loop [prev nil
         es entries]
    (cond
      (empty? es) true
      (nil? prev) (recur (first es) (rest es))
      :else
      (let [[pm ps] prev
            [cm cs] (first es)]
        (cond
          (< ps cs) (recur (first es) (rest es))
          (> ps cs) false
          ;; equal score: members must be strictly lexicographically ordered
          (neg? (compare pm cm)) (recur (first es) (rest es))
          :else false)))))

(defn- index-by-time
  "Return a vector of ops sorted by :index."
  [ops]
  (vec (sort-by :index ops)))

(defn- pair-invokes-with-completions
  "Returns a sequence of {:invoke inv :complete cmp} pairs for each
  completed op (ok/fail/info). Invokes without a matching completion are
  paired with nil (still in flight at history end)."
  [history]
  (let [by-process (group-by :process history)]
    (mapcat
      (fn [[_p ops]]
        (let [ops (index-by-time ops)]
          (loop [ops ops acc []]
            (if (empty? ops) acc
              (let [[o & rest-ops] ops]
                (cond
                  (= :invoke (:type o))
                  (let [c (first rest-ops)]
                    (if (and c (#{:ok :fail :info} (:type c)))
                      (recur (drop 1 rest-ops) (conj acc {:invoke o :complete c}))
                      (recur rest-ops (conj acc {:invoke o :complete nil}))))
                  :else (recur rest-ops acc)))))))
      by-process)))

(defn- mutation?
  [op]
  (#{:zadd :zincrby :zrem} (:f op)))

(defn- completed-mutation-window
  "For each completed mutation, produce
  {:member m :score s :zrem? bool? :unknown-score? bool? :invoke-idx i
   :complete-idx j :type t}.
  - :zadd: :score is the requested score (always known).
  - :zincrby: when :ok, :score is the server-returned final score. When
    :info or pending, the resulting score is unknown (depends on which
    other ops were applied first); :unknown-score? is set so allowed-
    scores-for-member can short-circuit the strict score check.
  - :zrem: :removed? is the boolean returned by ZREM (true iff the
    member existed). A no-op ZREM (returns 0) does NOT mutate state, so
    the model must not treat it as a deletion.
  :info / :pending mutations are still emitted so concurrent windows
  account for their possible effect."
  [pairs]
  (keep
    (fn [{:keys [invoke complete]}]
      (when (and invoke (mutation? invoke))
        (let [f (:f invoke)
              t (if complete (:type complete) :pending)
              inv-idx (:index invoke)
              cmp-idx (when complete (:index complete))]
          (case f
            :zadd
            (let [[m s] (:value invoke)]
              {:f :zadd :member m :score (double s)
               :type t :invoke-idx inv-idx :complete-idx cmp-idx})

            :zincrby
            (let [[m _delta] (:value invoke)
                  ;; ZINCRBY's resulting score is only knowable from the
                  ;; server reply. For :info/:pending we don't have it.
                  ok? (= :ok t)
                  s (when (and ok? (vector? (:value complete)))
                      (second (:value complete)))]
              {:f :zincrby :member m :score (some-> s double)
               :unknown-score? (not (and ok? (some? s)))
               :type t :invoke-idx inv-idx :complete-idx cmp-idx})

            :zrem
            (let [m (:value invoke)
                  ;; invoke! returns [member removed?]. For :info we don't
                  ;; know whether the member was removed.
                  removed? (cond
                             (and (= :ok t)
                                  (vector? (:value complete)))
                             (boolean (second (:value complete)))
                             ;; pending / info: assume removal could have
                             ;; happened; the checker treats it as a
                             ;; possibly-concurrent deletion via the
                             ;; concurrent window.
                             :else true)]
              {:f :zrem :member m :score nil
               :zrem? true :removed? removed?
               :type t :invoke-idx inv-idx :complete-idx cmp-idx})))))
    pairs))

(defn- mutations-by-member
  [mutations]
  (group-by :member mutations))

(defn- concurrent?
  "A mutation m is concurrent with a read r iff m's invoke precedes r's
  completion AND m's completion (or end-of-history) follows r's invoke."
  [m read-inv-idx read-cmp-idx]
  (and (<= (:invoke-idx m) read-cmp-idx)
       (or (nil? (:complete-idx m))
           (>= (:complete-idx m) read-inv-idx))))

(defn- apply-mutation-to-state
  "Fold one mutation into a per-member state {:present? bool :score s}.
  A no-op ZREM (member did not exist; :removed? false) leaves state
  unchanged so the checker doesn't falsely conclude the member is gone."
  [st m]
  (case (:f m)
    :zadd    {:present? true :score (:score m)}
    :zincrby {:present? true :score (:score m)}
    :zrem    (if (:removed? m)
               {:present? false :score nil}
               st)))

(defn- model-before
  "Construct authoritative per-member state from mutations whose
  completions strictly precede read-inv-idx. Returns
  {member -> {:present? bool :score s}}. Only :ok mutations contribute;
  :info / :pending are deferred to the concurrent-window check."
  [mutations-by-m read-inv-idx]
  (reduce-kv
    (fn [model member muts]
      (let [applied (->> muts
                         (filter #(and (= :ok (:type %))
                                       (some? (:complete-idx %))
                                       (< (:complete-idx %) read-inv-idx)))
                         (sort-by :complete-idx))
            state (reduce apply-mutation-to-state nil applied)]
        (if state
          (assoc model member state)
          model)))
    {}
    mutations-by-m))

(defn- concurrent-mutations-for-member
  "All mutations concurrent with the read window that could have taken
  effect. :fail completions are excluded: in Jepsen, :fail means the op
  definitively did NOT execute, so it contributes neither an allowed
  score nor uncertainty about presence. :ok and :info/:pending are
  included (either may be visible to the read)."
  [muts read-inv-idx read-cmp-idx]
  (filter #(and (not= :fail (:type %))
                (concurrent? % read-inv-idx read-cmp-idx))
          muts))

(defn- write-op?
  "True iff the mutation adds/updates the member's score (i.e. would
  make the member present). :zrem is NOT a write-op here."
  [m]
  (#{:zadd :zincrby} (:f m)))

(defn- allowed-scores-for-member
  "Compute the set of scores considered valid for `member` by a read
  whose window is [read-inv-idx, read-cmp-idx], based on committed state
  and any concurrent/uncertain mutations.

  Linearizability demands a read observes either (a) the latest committed
  state in real-time order, or (b) the effect of a write still concurrent
  with the read. We therefore restrict the committed score set to
  'candidates' — committed mutations NOT strictly followed in real time
  by another committed mutation (i.e. no other committed op's invoke
  comes after this op's completion). Scores from strictly superseded
  committed mutations are NOT admissible.

  When multiple candidates remain (their windows overlap), they can
  serialize in any real-time-consistent order: the read may legitimately
  observe the outcome of any of them. Thus presence is required only
  when EVERY admissible serialization leaves the member present; presence
  is forbidden only when EVERY admissible serialization leaves it absent.

  Returns:
    :scores            - set of acceptable scores (from candidate
                         committed ops + pre-read :info + concurrent
                         writes with a known score).
    :unknown-score?    - true iff any concurrent / pre-read :info
                         ZINCRBY's resulting score is unknown. When set,
                         the caller MUST skip the strict score-membership
                         check to stay sound.
    :can-be-present?   - true iff SOME admissible linearization leaves
                         the member present.
    :must-be-present?  - true iff EVERY admissible linearization leaves
                         the member present (i.e. some candidate is a
                         write, no candidate is a ZREM, and no uncertain
                         ZREM could have applied before the read)."
  [mutations-by-m member read-inv-idx read-cmp-idx]
  (let [muts (get mutations-by-m member [])
        ;; :ok mutations that completed strictly before the read.
        preceding (->> muts
                       (filter #(and (= :ok (:type %))
                                     (some? (:complete-idx %))
                                     (< (:complete-idx %) read-inv-idx))))
        ;; Real-time "last-wins" / chain-tail candidate filter: a
        ;; preceding mutation m is admissible iff no OTHER preceding
        ;; mutation m' has m'.invoke-idx > m.complete-idx (i.e. m'
        ;; strictly follows m in real time). Equivalent:
        ;; m.complete-idx >= max(invoke-idx) over preceding.
        ;;
        ;; Importantly this applies to :zincrby as well: a sequentially
        ;; committed ZINCRBY chain has a forced linearization (each
        ;; :ok :zincrby pins the pre-op and post-op states), so only
        ;; the latest chain tail's return value is a valid final score
        ;; for a post-chain read. An intermediate ZINCRBY's return
        ;; value is NOT admissible once another mutation strictly
        ;; follows it and commits before the read. A ZADD that strictly
        ;; follows a ZINCRBY likewise resets the chain (the ZADD's
        ;; absolute score becomes the only candidate).
        ;;
        ;; When multiple candidates remain (their invoke/complete
        ;; windows overlap), they may serialize in any real-time-
        ;; consistent order and any of their return values is a valid
        ;; final state.
        max-inv (reduce max -1 (map :invoke-idx preceding))
        candidates (filterv #(>= (:complete-idx %) max-inv) preceding)
        ;; :info mutations that completed before the read: they may or
        ;; may not have taken effect server-side.
        pre-read-info (->> muts
                           (filter #(and (= :info (:type %))
                                         (some? (:complete-idx %))
                                         (< (:complete-idx %) read-inv-idx))))
        ;; Concurrent mutations: windows overlap the read. Include both
        ;; :ok and :info since either may have taken effect.
        concurrent (concurrent-mutations-for-member muts read-inv-idx read-cmp-idx)
        ;; Uncertain mutations: anything whose effect on the read is not
        ;; fully determined by committed real-time order alone.
        uncertain (concat pre-read-info concurrent)

        add-scores (fn [acc m]
                     (case (:f m)
                       :zadd    (conj acc (:score m))
                       :zincrby (cond-> acc (some? (:score m)) (conj (:score m)))
                       :zrem    acc))
        ;; Admissible scores: candidate committed + pre-read :info +
        ;; concurrent writes (with a known score).
        scores (as-> #{} s
                 (reduce add-scores s candidates)
                 (reduce add-scores s uncertain))

        has-unknown-incr? (fn [coll]
                            (some #(and (= :zincrby (:f %))
                                        (:unknown-score? %))
                                  coll))
        ;; Classify uncertain ZINCRBYs by whether their resulting score
        ;; is known. The resulting score of a read relative to ZINCRBYs
        ;; depends only on which of them took effect before the read
        ;; observed state AND whether each such ZINCRBY's return value
        ;; is recorded.
        ;;   * Any uncertain ZINCRBY with UNKNOWN score (:info/:pending):
        ;;     the post-op score is not recoverable from the history, so
        ;;     we must relax the strict score check -- any numeric score
        ;;     is admissible.
        ;;   * All uncertain ZINCRBYs :ok with known return values:
        ;;     every recorded return pins the ZINCRBY's post-op state
        ;;     and (because ZINCRBY reads-then-writes atomically) its
        ;;     pre-op state. Any real-time consistent linearization
        ;;     therefore ends on one of those known return values (or
        ;;     on a candidate's score). :scores already contains all
        ;;     of them via the add-scores reduction over `uncertain`,
        ;;     so the strict score check is sound. Intermediate
        ;;     "prefix-sum" values (pre + delta_i for just one of
        ;;     several concurrent zincrbys) are NOT admissible final
        ;;     states: the return values constrain the serialization
        ;;     order, and no legitimate read can observe a partial sum
        ;;     that doesn't match any recorded post-op score.
        unknown-score? (has-unknown-incr? uncertain)

        any-candidate-write? (some write-op? candidates)
        any-candidate-zrem? (some #(= :zrem (:f %)) candidates)
        any-uncertain-write? (some write-op? uncertain)
        any-uncertain-zrem? (some #(= :zrem (:f %)) uncertain)

        ;; Some linearization of candidates ends with the member
        ;; present. Because candidates have overlapping windows (they
        ;; all share the same max-inv), any of them can serialize last.
        ;; So presence is allowed iff at least one candidate is a write.
        candidate-can-be-present? (boolean any-candidate-write?)
        ;; Some linearization of candidates ends with the member absent.
        candidate-can-be-absent? (or (empty? candidates)
                                     (boolean any-candidate-zrem?))

        ;; can-be-present?: at least one admissible linearization
        ;; (candidates + uncertain) ends with the member present.
        ;; Presence REQUIRES a write-op (ZADD / ZINCRBY) somewhere in
        ;; the admissible set -- either a candidate committed write or
        ;; an uncertain concurrent/pre-read :info write. ZREM never
        ;; contributes existence evidence: since `setup!` clears the
        ;; key at test start, an observed member that never had a ZADD
        ;; or ZINCRBY touch it must be a phantom regardless of any
        ;; ZREM's :removed? flag (which may be defaulted to true on
        ;; :info for uncertainty accounting only).
        can-be-present? (or candidate-can-be-present?
                            any-uncertain-write?)

        ;; must-be-present?: EVERY admissible linearization ends with
        ;; the member present. Requires the candidate outcome to be
        ;; always-present (candidate write, no candidate zrem) AND no
        ;; uncertain zrem that could reorder last to remove it.
        must-be-present? (boolean (and any-candidate-write?
                                       (not candidate-can-be-absent?)
                                       (not any-uncertain-zrem?)))]
    {:scores           scores
     :unknown-score?   (boolean unknown-score?)
     :can-be-present?  (boolean can-be-present?)
     :must-be-present? must-be-present?}))

(defn- score-definitely-in-range?
  "True iff the member's committed score is definitively in [lo, hi]
  for the purposes of completeness: every candidate score is inside the
  range AND no uncertain/concurrent mutation could have produced an
  unknown or out-of-range score. Used by ZRANGEBYSCORE completeness."
  [scores unknown-score? lo hi]
  (boolean (and (not unknown-score?)
                (seq scores)
                (every? #(<= lo % hi) scores))))

(defn- duplicate-members
  "Return the set of members that appear more than once in entries."
  [entries]
  (->> entries
       (map first)
       frequencies
       (keep (fn [[m n]] (when (> n 1) m)))
       set))

(defn- check-zrange-all
  [mutations-by-m {:keys [invoke complete] :as _pair}]
  (let [entries (:value complete)
        inv-idx (:index invoke)
        cmp-idx (:index complete)
        errors (atom [])]
    ;; 1. Ordering
    (when-not (sorted-by-score-then-member? entries)
      (swap! errors conj {:kind :unsorted
                          :index cmp-idx
                          :entries entries}))
    ;; 1b. No duplicate members: a ZSet read must return each member at
    ;; most once. A duplicate-member result could otherwise satisfy
    ;; ordering and score-membership checks while hiding a real bug.
    (let [dupes (duplicate-members entries)]
      (when (seq dupes)
        (swap! errors conj {:kind :duplicate-members
                            :index cmp-idx
                            :members dupes})))
    ;; 2. For each observed (member,score): validate presence + score.
    ;;    can-be-present? catches both phantoms (member never existed)
    ;;    and stale reads (member committed-removed before the read
    ;;    with no concurrent re-add).
    (doseq [[member score] entries]
      (let [{:keys [scores can-be-present? unknown-score?]}
            (allowed-scores-for-member mutations-by-m member inv-idx cmp-idx)]
        (cond
          (not can-be-present?)
          (swap! errors conj {:kind :unexpected-presence
                              :index cmp-idx
                              :member member
                              :score score})
          ;; Skip the strict score check when any concurrent ZINCRBY's
          ;; resulting score is unknown: the read could legitimately
          ;; observe any value the in-flight increment produces.
          unknown-score? nil
          (not (contains? scores score))
          (swap! errors conj {:kind :score-mismatch
                              :index cmp-idx
                              :member member
                              :observed score
                              :allowed scores}))))
    ;; 3. Completeness: model-required members must appear.
    ;;    A member is required-present only if every admissible
    ;;    linearization leaves it present (must-be-present?). This
    ;;    correctly skips members that an :info or concurrent ZREM
    ;;    might have removed before the read.
    (let [model (model-before mutations-by-m inv-idx)
          observed-members (into #{} (map first) entries)]
      (doseq [[member _] model]
        (let [{:keys [must-be-present?]}
              (allowed-scores-for-member mutations-by-m member inv-idx cmp-idx)]
          (when (and must-be-present?
                     (not (contains? observed-members member)))
            (swap! errors conj {:kind :missing-member
                                :index cmp-idx
                                :member member})))))
    @errors))

(defn- check-zrangebyscore
  [mutations-by-m {:keys [invoke complete] :as _pair}]
  (let [{:keys [bounds members]} (:value complete)
        [lo hi] bounds
        inv-idx (:index invoke)
        cmp-idx (:index complete)
        errors (atom [])]
    (when-not (sorted-by-score-then-member? members)
      (swap! errors conj {:kind :unsorted-range
                          :index cmp-idx
                          :bounds bounds
                          :members members}))
    (let [dupes (duplicate-members members)]
      (when (seq dupes)
        (swap! errors conj {:kind :duplicate-members-range
                            :index cmp-idx
                            :bounds bounds
                            :members dupes})))
    ;; Observed members must be within bounds AND have a known allowed score.
    (doseq [[member score] members]
      (when (or (< score lo) (> score hi))
        (swap! errors conj {:kind :out-of-range
                            :index cmp-idx
                            :bounds bounds
                            :member member
                            :score score}))
      (let [{:keys [scores can-be-present? unknown-score?]}
            (allowed-scores-for-member mutations-by-m member inv-idx cmp-idx)]
        (cond
          (not can-be-present?)
          (swap! errors conj {:kind :unexpected-presence-range
                              :index cmp-idx
                              :member member
                              :score score})
          unknown-score? nil
          (not (contains? scores score))
          (swap! errors conj {:kind :score-mismatch-range
                              :index cmp-idx
                              :member member
                              :observed score
                              :allowed scores}))))
    ;; Completeness within bounds: a model member must appear only when
    ;; (a) every admissible linearization leaves it present
    ;;     (must-be-present?), AND
    ;; (b) its score is definitively within [lo, hi] across all
    ;;     admissible linearizations (no uncertain ZINCRBY, every
    ;;     candidate score inside the bounds).
    ;; Uncertain scores (concurrent/:info ZINCRBY) must NOT cause
    ;; completeness failures when the resulting score is unknown.
    (let [model (model-before mutations-by-m inv-idx)
          observed-members (into #{} (map first) members)]
      (doseq [[member _] model]
        (let [{:keys [must-be-present? scores unknown-score?]}
              (allowed-scores-for-member mutations-by-m member inv-idx cmp-idx)]
          (when (and must-be-present?
                     (score-definitely-in-range? scores unknown-score? lo hi)
                     (not (contains? observed-members member)))
            (swap! errors conj {:kind :missing-member-range
                                :index cmp-idx
                                :bounds bounds
                                :member member
                                :expected-score (first scores)})))))
    @errors))

(defn zset-safety-checker
  "Custom Jepsen checker: validates ZSet safety properties using a
  last-writer model combined with a concurrent-write relaxation."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [pairs (pair-invokes-with-completions history)
            mutations (completed-mutation-window pairs)
            mutations-by-m (mutations-by-member mutations)
            read-pairs (filter (fn [{:keys [invoke complete]}]
                                 (and invoke complete
                                      (= :ok (:type complete))
                                      (#{:zrange-all :zrangebyscore}
                                       (:f invoke))))
                               pairs)
            all-errors (reduce
                         (fn [acc {:keys [invoke] :as pair}]
                           (into acc
                                 (case (:f invoke)
                                   :zrange-all    (check-zrange-all mutations-by-m pair)
                                   :zrangebyscore (check-zrangebyscore mutations-by-m pair))))
                         []
                         read-pairs)
            by-kind (group-by :kind all-errors)]
        {:valid? (empty? all-errors)
         :reads  (count read-pairs)
         :mutations (count mutations)
         :error-count (count all-errors)
         :errors-by-kind (into {} (map (fn [[k v]] [k (count v)]) by-kind))
         :first-errors (take 20 all-errors)}))))

;; ---------------------------------------------------------------------------
;; Workload
;; ---------------------------------------------------------------------------

(defn elastickv-zset-safety-workload
  [opts]
  (let [node->port (or (:node->port opts)
                       (zipmap default-nodes (repeat 6379)))
        client (->ElastickvRedisZSetSafetyClient node->port nil)]
    {:client    client
     :checker   (checker/compose
                  {:zset-safety (zset-safety-checker)
                   :timeline    (timeline/html)})
     :generator (op-generator)
     :final-generator (gen/once {:f :zrange-all})}))

(defn elastickv-zset-safety-test
  "Builds a Jepsen test map that drives elastickv's Redis ZSet safety
  workload."
  ([] (elastickv-zset-safety-test {}))
  ([opts]
   (let [nodes       (or (:nodes opts) default-nodes)
         redis-ports (or (:redis-ports opts)
                         (repeat (count nodes) (or (:redis-port opts) 6379)))
         node->port  (or (:node->port opts)
                         (cli/ports->node-map redis-ports nodes))
         local?      (:local opts)
         db          (if local?
                       jdb/noop
                       (ekdb/db {:grpc-port  (or (:grpc-port opts) 50051)
                                 :redis-port node->port
                                 :raft-groups (:raft-groups opts)
                                 :shard-ranges (:shard-ranges opts)}))
         rate        (double (or (:rate opts) 10))
         time-limit  (or (:time-limit opts) 60)
         faults      (if local?
                       []
                       (cli/normalize-faults (or (:faults opts) [:partition :kill])))
         nemesis-p   (when-not local?
                       (combined/nemesis-package {:db       db
                                                  :faults   faults
                                                  :interval (or (:fault-interval opts) 40)}))
         nemesis-gen (if nemesis-p
                       (:generator nemesis-p)
                       (gen/once {:type :info :f :noop}))
         workload    (elastickv-zset-safety-workload
                       (assoc opts :node->port node->port))]
     (merge workload
            {:name        (or (:name opts) "elastickv-redis-zset-safety")
             :nodes       nodes
             :db          db
             :redis-host  (:redis-host opts)
             :os          (if local? os/noop debian/os)
             :net         (if local? net/noop net/iptables)
             :ssh         (merge {:username             "vagrant"
                                  :private-key-path     "/home/vagrant/.ssh/id_rsa"
                                  :strict-host-key-checking false}
                                 (when local? {:dummy true})
                                 (:ssh opts))
             :remote      control/ssh
             :nemesis     (if nemesis-p (:nemesis nemesis-p) nemesis/noop)
             :final-generator nil
             :concurrency (or (:concurrency opts) 5)
             :generator   (->> (:generator workload)
                               (gen/nemesis nemesis-gen)
                               (gen/stagger (/ rate))
                               (gen/time-limit time-limit))}))))

;; ---------------------------------------------------------------------------
;; CLI
;; ---------------------------------------------------------------------------

(def zset-safety-cli-opts
  [[nil "--ports PORTS" "Comma-separated Redis ports (per node)."
    :default nil
    :parse-fn (fn [s]
                (->> (str/split s #",")
                     (remove str/blank?)
                     (mapv #(Integer/parseInt %))))]
   [nil "--redis-port PORT" "Redis port applied to all nodes."
    :default 6379
    :parse-fn #(Integer/parseInt %)]])

(defn- prepare-zset-safety-opts [options]
  (let [ports (or (:ports options) nil)
        options (cli/parse-common-opts options ports)]
    (assoc options
      :redis-host  (:host options)
      :redis-ports ports
      :redis-port  (:redis-port options))))

(defn -main [& args]
  (cli/run-workload! args
                     (into cli/common-cli-opts zset-safety-cli-opts)
                     prepare-zset-safety-opts
                     elastickv-zset-safety-test))
