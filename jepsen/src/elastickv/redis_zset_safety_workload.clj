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

(defn- parse-withscores
  "Carmine returns a flat [member score member score ...] vector for
  ZRANGE WITHSCORES. Convert to a sorted vector of [member (double score)]
  preserving server-returned order (score ascending, then member)."
  [flat]
  (->> flat
       (partition 2)
       (mapv (fn [[m s]]
               [(if (bytes? m) (String. ^bytes m) (str m))
                (Double/parseDouble (str s))]))))

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
    (when-let [cs (:conn-spec this)]
      (try (car/wcar cs (car/del zset-key))
           (catch Exception e
             (warn "ZSet safety setup DEL failed:" (.getMessage e)))))
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
                new-score (car/wcar cs (car/zincrby zset-key (double delta) member))]
            (assoc op :type :ok
                   :value [member (Double/parseDouble (str new-score))]))

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
  "All mutations (ok or info) that are concurrent with the read window."
  [muts read-inv-idx read-cmp-idx]
  (filter #(concurrent? % read-inv-idx read-cmp-idx) muts))

(defn- allowed-scores-for-member
  "Compute the set of scores considered valid for `member` by a read
  whose window is [read-inv-idx, read-cmp-idx], based on committed state
  and any concurrent mutations.

  Returns:
    :scores            - set of acceptable scores (committed + concurrent
                         :zadd / :ok :zincrby).
    :unknown-score?    - true iff any concurrent ZINCRBY's resulting score
                         is unknown (in-flight or :info). When set, the
                         caller MUST skip the strict score-membership
                         check to stay sound.
    :must-be-present?  - committed state says present and no concurrent
                         mutation could have removed/changed it.
    :any-known?        - some op claims to have touched this member."
  [mutations-by-m member read-inv-idx read-cmp-idx]
  (let [muts (get mutations-by-m member [])
        committed (->> muts
                       (filter #(and (= :ok (:type %))
                                     (some? (:complete-idx %))
                                     (< (:complete-idx %) read-inv-idx)))
                       (sort-by :complete-idx))
        committed-state (reduce apply-mutation-to-state nil committed)
        concurrent (concurrent-mutations-for-member muts read-inv-idx read-cmp-idx)
        scores (cond-> #{}
                 (and committed-state (:present? committed-state))
                 (conj (:score committed-state)))
        scores (reduce
                 (fn [acc m]
                   (case (:f m)
                     :zadd    (conj acc (:score m))
                     :zincrby (cond-> acc (some? (:score m)) (conj (:score m)))
                     :zrem    acc))
                 scores
                 concurrent)
        unknown-score? (some #(and (= :zincrby (:f %))
                                   (:unknown-score? %))
                             concurrent)]
      {:scores scores
       :unknown-score? (boolean unknown-score?)
       :must-be-present? (boolean (and committed-state (:present? committed-state)
                                       (empty? concurrent)))
       :any-known? (or (boolean committed-state) (seq concurrent))}))

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
    ;; 2. For each observed (member,score): validate score and non-phantom
    (doseq [[member score] entries]
      (let [{:keys [scores any-known? unknown-score?]}
            (allowed-scores-for-member mutations-by-m member inv-idx cmp-idx)]
        (cond
          (not any-known?)
          (swap! errors conj {:kind :phantom
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
    (let [model (model-before mutations-by-m inv-idx)
          observed-members (into #{} (map first) entries)]
      (doseq [[member {:keys [present?]}] model]
        (when (and present? (not (contains? observed-members member)))
          (let [muts (get mutations-by-m member [])
                concurrent (concurrent-mutations-for-member muts inv-idx cmp-idx)]
            (when (empty? concurrent)
              (swap! errors conj {:kind :missing-member
                                  :index cmp-idx
                                  :member member}))))))
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
    ;; Observed members must be within bounds AND have a known allowed score.
    (doseq [[member score] members]
      (when (or (< score lo) (> score hi))
        (swap! errors conj {:kind :out-of-range
                            :index cmp-idx
                            :bounds bounds
                            :member member
                            :score score}))
      (let [{:keys [scores any-known? unknown-score?]}
            (allowed-scores-for-member mutations-by-m member inv-idx cmp-idx)]
        (cond
          (not any-known?)
          (swap! errors conj {:kind :phantom-range
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
    ;; Completeness within bounds: any model member whose committed score
    ;; is in [lo,hi] with no concurrent mutation must appear.
    (let [model (model-before mutations-by-m inv-idx)
          observed-members (into #{} (map first) members)]
      (doseq [[member {:keys [present? score]}] model]
        (when (and present?
                   (<= lo score hi)
                   (not (contains? observed-members member)))
          (let [muts (get mutations-by-m member [])
                concurrent (concurrent-mutations-for-member muts inv-idx cmp-idx)]
            (when (empty? concurrent)
              (swap! errors conj {:kind :missing-member-range
                                  :index cmp-idx
                                  :bounds bounds
                                  :member member
                                  :expected-score score}))))))
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
