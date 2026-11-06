(ns elastickv.sqs-htfifo-workload
  "Jepsen workload for elastickv's HT-FIFO (high-throughput FIFO) SQS-compatible
   queues — partitioned FIFO queues created with PartitionCount > 1.

   Pattern follows aphyr's classic Jepsen RabbitMQ analysis: track every
   :send and :recv in the history, then a custom checker verifies three
   contracts that AWS HT-FIFO is supposed to honour even under partition
   and node-loss faults:

   1. Within-group ordering — raw ReceiveMessage responses preserve the
      real-time precedence of successful sends. Concurrent sends may arrive
      in either order; client sequence numbers identify messages, not order.
   2. No loss — every (group, seq) successfully :sent eventually appears
      in the :recv history. Sends with :info status are treated as
      possibly-committed and not counted as lost.
   3. No duplicate acknowledged consumption — every (group, seq) has at
      most one successful delete. Raw redeliveries remain in the ordering
      evidence even when deletion fails.

   Each MessageGroupId is hashed by partitionFor (FNV-1a) onto one of N
   partitions; with several distinct groups the workload exercises
   cross-partition delivery, while ContentBasedDeduplication + per-group
   monotonic seqs keeps the assertions checkable from the client side."
  (:gen-class)
  (:require [clojure.set :as cset]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [cognitect.aws.client.api :as aws]
            [elastickv.cli :as cli]
            [elastickv.sqs-common :as sqs]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]])
  (:import (java.nio.charset StandardCharsets)
           (java.security MessageDigest)
           (java.util UUID)))

;; ---------------------------------------------------------------------------
;; Constants
;; ---------------------------------------------------------------------------

(def ^:private default-sqs-port 9324)
(def ^:private default-partition-count 4)
(def ^:private default-group-count 8)
(def ^:private receive-batch-size 10)
;; WaitTimeSeconds=1: elastickv's receive path is short-poll-only today, so
;; this is a no-op at the wire but keeps clients SDK-portable if long-poll
;; lands later. Visibility 30s is long enough for delete to land between
;; receive and the next pass even under partition.
(def ^:private receive-wait-seconds 1)
(def ^:private visibility-timeout-seconds 30)

(defn- fresh-queue-name
  "Build a per-run unique queue name. Includes a millisecond timestamp
   so re-running the workload against an already-running cluster
   (--no-rebuild --no-cluster) starts with a fresh queue: prior-run
   messages cannot inflate :received, drift the per-group seq numbering,
   or block sends via the 5-minute ContentBasedDeduplication window.
   AWS SQS queue names admit [A-Za-z0-9_-] plus the .fifo suffix; the
   timestamp sits inside that alphabet."
  []
  (str "jepsen-htfifo-" (System/currentTimeMillis) ".fifo"))

;; ---------------------------------------------------------------------------
;; Queue setup
;; ---------------------------------------------------------------------------

(defn- create-htfifo-queue!
  "Idempotently create the HT-FIFO test queue. Returns the QueueUrl.
   Tolerates QueueAlreadyExists so concurrent workers calling setup!
   in parallel converge on the same QueueUrl (each worker calls
   setup! independently in Jepsen's lifecycle)."
  [sqs queue-name partition-count]
  (let [attrs {"FifoQueue"                 "true"
               "ContentBasedDeduplication" "true"
               "PartitionCount"            (str partition-count)
               "FifoThroughputLimit"       "perMessageGroupId"
               "DeduplicationScope"        "messageGroup"}
        resp  (try
                (sqs/invoke! sqs :CreateQueue
                             {:QueueName  queue-name
                              :Attributes attrs})
                (catch clojure.lang.ExceptionInfo e
                  (let [err-type (:type (ex-data e))]
                    (if (or (= "QueueAlreadyExists" err-type)
                            (= "QueueNameExists" err-type))
                      (sqs/invoke! sqs :GetQueueUrl {:QueueName queue-name})
                      (throw e)))))]
    (or (:QueueUrl resp)
        (throw (ex-info "CreateQueue did not return QueueUrl" {:resp resp})))))

;; ---------------------------------------------------------------------------
;; Per-group sequence counters (shared across all client workers)
;;
;; A single test-wide atom maps group-id → next sequence number. The atom is
;; constructed fresh per test run via fresh-seq-counters and shared with all
;; ClientRecord instances via the workload map's :seq-counters field.
;; ---------------------------------------------------------------------------

(defn- fresh-seq-counters
  "Build the shared seq-counter atom for the workload. Each group-id maps
   to a long (next seq to assign)."
  [groups]
  (atom (zipmap groups (repeat 0))))

(defn- next-seq!
  "Atomically increment the counter for `group` and return the previous
   value. Stable monotonic seqs across all workers."
  [counters group]
  (let [next-state (swap! counters update group inc)]
    ;; Returned seq = post-state - 1 = the value that was assigned.
    (dec (get next-state group))))

(defn- encode-body
  "Encode (group, seq) into the message body. Uses a simple `g:s` form
   (no JSON to avoid an extra dep). The encoding is the only thing the
   server sees; the checker decodes it on receive to reconstruct the
   logical (group, seq) tuple."
  [group seq-num]
  (str group ":" seq-num))

(defn- decode-body
  "Decode a body produced by encode-body. Returns nil if the payload
   doesn't match the expected shape so a corrupted body surfaces as a
   single failed assertion instead of crashing the checker."
  [body]
  (when (string? body)
    ;; str/split always returns a vector — never nil — so the
    ;; nil-safety here lives in the str/blank? checks below, not
    ;; in a when-let on the destructure. Plain let makes that
    ;; explicit; "g0" with no colon binds seq-str=nil, which
    ;; str/blank? treats as blank and the when guards skip.
    (let [[group seq-str] (str/split body #":" 2)]
      (when (and (not (str/blank? group))
                 (not (str/blank? seq-str)))
        (try
          {:group group
           :seq   (Long/parseLong seq-str)}
          (catch NumberFormatException _ nil))))))

;; ---------------------------------------------------------------------------
;; Jepsen client
;; ---------------------------------------------------------------------------

(defn- fresh-observer []
  (atom {:clock-id (str (UUID/randomUUID)) :origin (System/nanoTime) :last -1}))

(defn- observe! [observer]
  ;; Only timestamp allocation is serialized, never an SQS call. All workers
  ;; share this controller clock, independently of Jepsen's completion times.
  (locking observer
    (let [{:keys [origin last]} @observer
          t (max (inc last) (- (System/nanoTime) origin))]
      (swap! observer assoc :last t)
      t)))

(defn- observation [observer]
  {:version 1 :clock-id (:clock-id @observer)})

(defn- identity-hash [s]
  (when (and (string? s) (not (str/blank? s)))
    (let [digest (.digest (MessageDigest/getInstance "SHA-256")
                          (.getBytes s StandardCharsets/UTF_8))]
      (apply str (map #(format "%02x" (bit-and 255 %)) digest)))))

(defn- classify-error [op e]
  (if (instance? clojure.lang.ExceptionInfo e)
    (let [{err-type :type category :category} (ex-data e)]
      (cond
        (and (nil? err-type)
             (#{:cognitect.anomalies/fault :cognitect.anomalies/unavailable
                :cognitect.anomalies/interrupted} category))
        (assoc op :type :info :error :network-error)

        (#{"InternalFailure" "InternalServerError" "ServiceUnavailable"} err-type)
        (assoc op :type :info :error (str err-type))

        (#{"InvalidParameterValue" "QueueDoesNotExist"
           "ReceiptHandleIsInvalid" "InvalidIdFormat"} err-type)
        (assoc op :type :fail :error (str err-type))

        :else
        (assoc op :type :info
               :error (cond
                        (#{"MissingParameter" "QueueNameExists" "QueueAlreadyExists"
                           "AWS.SimpleQueueService.NonExistentQueue"
                           "AWS.SimpleQueueService.PurgeQueueInProgress"
                           "InvalidAttributeName" "InvalidAttributeValue"
                           "IncompleteSignature" "SignatureDoesNotMatch" "RequestTimeTooSkewed"
                           "InvalidAction" "MalformedQueryString" "Throttling" "RequestThrottled"
                           "InvalidReceiptHandle" "MessageNotInflight" "AccessDenied"} err-type)
                        err-type
                        (#{:cognitect.anomalies/incorrect :cognitect.anomalies/forbidden
                           :cognitect.anomalies/unsupported :cognitect.anomalies/not-found
                           :cognitect.anomalies/conflict :cognitect.anomalies/busy} category)
                        category
                        :else :unexpected-client-error))))
    (assoc op :type :info :error :unexpected-client-error)))

(defn- observe-send! [sqs queue-url groups counters observer current]
  (let [group (rand-nth groups)
        seq-num (next-seq! counters group)
        body (encode-body group seq-num)
        obs (assoc (observation observer) :send-start (observe! observer))]
    (vswap! current assoc :value [group seq-num] :sqs-observation obs)
    (sqs/invoke! sqs :SendMessage
                 {:QueueUrl @queue-url :MessageBody body :MessageGroupId group})
    (vswap! current assoc-in [:sqs-observation :send-end] (observe! observer))
    (assoc @current :type :ok)))

(defn- raw-delivery [index msg]
  (let [decoded (decode-body (:Body msg))]
    {:delivery-index index
     :tuple (when decoded [(:group decoded) (:seq decoded)])
     :message-hash (identity-hash (:MessageId msg))
     :receipt-hash (identity-hash (:ReceiptHandle msg))}))

(defn- receive-batch [response]
  (when-not (map? response)
    (throw (ex-info "Malformed SQS receive response" {})))
  (let [messages (:Messages response)]
    (when-not (or (nil? messages) (sequential? messages))
      (throw (ex-info "Malformed SQS receive batch" {})))
    (vec messages)))

(defn- observe-receive! [sqs queue-url observer current]
  (vswap! current assoc :value [] :sqs-observation
          (assoc (observation observer) :receive-start (observe! observer)))
  (let [resp (sqs/invoke! sqs :ReceiveMessage
                          {:QueueUrl @queue-url
                           :MaxNumberOfMessages receive-batch-size
                           :VisibilityTimeout visibility-timeout-seconds
                           :WaitTimeSeconds receive-wait-seconds})
        received-at (observe! observer)
        ;; Preserve the successful SDK return even if batch conversion fails:
        ;; it must not become an unobserved network-unknown operation.
        _ (vswap! current assoc-in [:sqs-observation :received-at] received-at)
        msgs (receive-batch resp)
        deliveries (mapv raw-delivery (range) msgs)]
    ;; Persist the whole raw batch before the first delete can unlock a group.
    (vswap! current assoc-in [:sqs-observation :deliveries] deliveries)
    (doseq [[i msg] (map-indexed vector msgs)
            :let [delivery (nth deliveries i)]
            :when (:tuple delivery)]
      (let [path [:sqs-observation :deliveries i :delete]
            deletion {:delivery-index i :receipt-hash (:receipt-hash delivery)
                      :start (observe! observer)}]
        (vswap! current assoc-in path deletion)
        (let [result (try
                       (sqs/invoke! sqs :DeleteMessage
                                    {:QueueUrl @queue-url :ReceiptHandle (:ReceiptHandle msg)})
                       {:type :ok}
                       (catch clojure.lang.ExceptionInfo e
                         ;; No exception text, request, body or receipt enters
                         ;; delete metadata or logs. Preserve outcome classes.
                         (warn "DeleteMessage failed; delivery remains unacknowledged")
                         (select-keys (classify-error {} e) [:type])))]
          (vswap! current assoc-in path
                  (merge deletion result {:end (observe! observer)}))
          (when (= :ok (:type result))
            (vswap! current update :value conj (:tuple delivery))))))
    (assoc @current :type :ok)))

(defrecord HTFIFOClient [node->port region groups seq-counters queue-name sqs queue-url partition-count observer]
  client/Client

  (open! [this test node]
    (let [port (get node->port node default-sqs-port)
          host (or (:sqs-host test) (name node))]
      (assoc this :sqs (sqs/make-client host port region))))

  (setup! [this _test]
    (let [url (create-htfifo-queue! sqs queue-name partition-count)]
      (info "HT-FIFO test queue ready" url "partitions=" partition-count)
      (reset! queue-url url)
      this))

  (teardown! [_this _test]
    ;; Leave the queue around — the test cluster is torn down by db/teardown!.
    ;; A best-effort DeleteQueue here would race the partition-isolated nodes.
    nil)

  (close! [this _test]
    (when sqs (aws/stop sqs))
    (assoc this :sqs nil :queue-url nil))

  (invoke! [_this _test op]
    (let [current (volatile! op)]
      (try
        (case (:f op)
          :send (observe-send! sqs queue-url groups seq-counters observer current)
          :recv (observe-receive! sqs queue-url observer current))
        (catch Exception e
          (classify-error @current e))))))

;; ---------------------------------------------------------------------------
;; Checker — within-group ordering + no loss + no duplicates
;; ---------------------------------------------------------------------------

(defn- collect-sends
  "Return the set of (group, seq) tuples successfully :sent. :info sends
   are returned separately as the in-flight set (their commit status is
   unknown)."
  [history]
  (let [sends (filter #(= :send (:f %)) history)]
    {:committed (->> sends
                     (filter #(= :ok (:type %)))
                     (map :value)
                     set)
     :in-flight (->> sends
                     (filter #(= :info (:type %)))
                     (map :value)
                     set)}))

(defn- timestamp? [t] (and (integer? t) (<= 0 t)))

(defn- tuple? [v]
  (and (vector? v) (= 2 (count v))
       (string? (first v)) (not (str/blank? (first v)))
       (timestamp? (second v))))

(defn- hash? [v] (and (string? v) (boolean (re-matches #"[0-9a-f]{64}" v))))

(defn- delivery-error [received-at previous-end i delivery]
  (let [{:keys [tuple message-hash receipt-hash delete delivery-index]} delivery]
    (cond
      (not= i delivery-index) :bad-delivery-index
      (not (tuple? tuple)) :malformed-body
      (not (hash? message-hash)) :missing-message-identity
      (not (hash? receipt-hash)) :missing-receipt-identity
      (not (map? delete)) :missing-delete-result
      (or (not= i (:delivery-index delete))
          (not= receipt-hash (:receipt-hash delete))) :delete-identity-mismatch
      (not (#{:ok :info :fail} (:type delete))) :bad-delete-result
      (not (and (timestamp? (:start delete)) (timestamp? (:end delete))
                (<= received-at (:start delete) (:end delete))
                (or (nil? previous-end)
                    (and (timestamp? previous-end) (<= previous-end (:start delete))))))
      :bad-delete-interval)))

(defn- receive-error [op obs]
  (let [{:keys [receive-start received-at deliveries]} obs]
    (cond
      (not (timestamp? receive-start)) :missing-receive-start
      (nil? received-at)
      (when (or (= :ok (:type op)) (some? deliveries)
                (not (or (nil? (:value op)) (= [] (:value op)))))
        :missing-raw-response)
      (not (and (timestamp? received-at) (<= receive-start received-at))) :bad-response-time
      (not (vector? deliveries)) :missing-raw-batch
      (not (vector? (:value op))) :bad-acknowledgment-shape
      :else
      (or (some identity
                (map-indexed
                  (fn [i delivery]
                    (delivery-error received-at
                                    (when (pos? i) (get-in deliveries [(dec i) :delete :end]))
                                    i delivery))
                  deliveries))
          (when (not= (or (:value op) [])
                      (mapv :tuple (filter #(= :ok (get-in % [:delete :type])) deliveries)))
            :acknowledgment-mismatch)))))

(defn- observation-error [test op]
  (let [obs (:sqs-observation op)]
    (cond
      (not (#{:ok :fail :info} (:type op))) :invalid-operation-outcome
      (not (map? obs)) :missing-observation
      (not= 1 (:version obs)) :unsupported-observation-version
      (or (not (string? (:sqs-observation-clock-id test)))
          (str/blank? (:sqs-observation-clock-id test))
          (not= (:sqs-observation-clock-id test) (:clock-id obs))) :foreign-observation-clock
      (= :send (:f op))
      (cond
        (not (tuple? (:value op))) :bad-send-identity
        (not (timestamp? (:send-start obs))) :missing-send-start
        (= :ok (:type op))
        (when-not (and (timestamp? (:send-end obs))
                       (<= (:send-start obs) (:send-end obs))) :bad-send-interval)
        (contains? obs :send-end) :ambiguous-send-has-end)
      :else (receive-error op obs))))

(defn- send-intervals
  "Pair framework invokes/completions, not client sequence numbers. Only
   successful completions bound a send's end; :info remains open-ended."
  [history]
  (reduce
    (fn [{:keys [pending intervals] :as state} op]
      (if (not= :send (:f op))
        state
        (let [p (:process op) tuple (:value op)
              invocation (get pending p)]
          (if (= :invoke (:type op))
            (cond-> (assoc-in state [:pending p] op)
              invocation (update :errors conj :overlapping-worker-send))
            (let [start (:time invocation)
                  end (when (= :ok (:type op)) (:time op))
                  error (cond
                          (not (and (timestamp? start) (timestamp? (:time op))
                                    (<= start (:time op)))) :missing-send-invocation
                          (and end (< end start)) :bad-framework-send-interval
                          (contains? intervals tuple) :reused-send-identity)]
              (cond-> (-> state
                          (update :pending dissoc p)
                          (assoc-in [:intervals tuple]
                                    {:start start :end end :type (:type op)
                                     :observed-start (get-in op [:sqs-observation :send-start])}))
                error (update :errors conj error)))))))
    {:pending {} :intervals {} :errors []}
    history))

(defn- raw-deliveries [ops]
  (mapcat (fn [op]
            (let [obs (:sqs-observation op)]
              (when (vector? (:deliveries obs))
                (map #(assoc % :received-at (:received-at obs)) (:deliveries obs)))))
          (filter #(= :recv (:f %)) ops)))

(defn- identity-errors [deliveries intervals]
  (:errors
    (reduce
      (fn [{:keys [messages tuples receipts] :as state} d]
        (let [{:keys [tuple message-hash receipt-hash]} d
              send (get intervals tuple)
              reason (cond
                       (or (nil? send) (= :fail (:type send))) :unmatched-delivery
                       (< (:received-at d) (:observed-start send)) :delivery-before-send
                       (and (contains? messages message-hash)
                            (not= tuple (get messages message-hash))) :message-identity-reused
                       (and (contains? tuples tuple)
                            (not= message-hash (get tuples tuple))) :tuple-identity-changed
                       (contains? receipts receipt-hash) :receipt-identity-reused)]
          (cond-> (-> state
                      (assoc-in [:messages message-hash] tuple)
                      (assoc-in [:tuples tuple] message-hash)
                      (update :receipts conj receipt-hash))
            reason (update :errors conj reason))))
      {:messages {} :tuples {} :receipts #{} :errors []}
      deliveries)))

(defn- ordering-violations [deliveries intervals]
  ;; If an earlier-observed send began after this send successfully ended,
  ;; that delivery is an inversion. One maximum per group is a sufficient
  ;; witness, including nonadjacent inversions and every raw redelivery.
  (:violations
    (reduce
      (fn [{:keys [latest] :as state} {:keys [tuple]}]
        (let [[group seq-num] tuple
              {:keys [start end]} (get intervals tuple)
              previous (get latest group)]
          (cond-> state
            (and end previous (< end (:start previous)))
            (update-in [:violations group] (fnil conj [])
                       {:prev (second (:tuple previous)) :curr seq-num})
            (or (nil? previous) (> start (:start previous)))
            (assoc-in [:latest group] {:tuple tuple :start start}))))
      {:latest {} :violations {}}
      (sort-by (juxt :received-at :delivery-index) deliveries))))

(defn ht-fifo-checker
  "Custom Jepsen checker for the HT-FIFO contract. Returns
   {:valid? bool :sent N :received N :lost #{...} :duplicates #{...}
    :ordering-violations {...}}."
  []
  (reify checker/Checker
    (check [_ test history _opts]
      (let [{:keys [committed in-flight]} (collect-sends history)
            ops (filter #(and (#{:send :recv} (:f %))
                               (not= :invoke (:type %))) history)
            recvs (filter #(= :recv (:f %)) ops)
            ;; Preserve legacy acknowledged-consumption diagnostics even if
            ;; the trace predates raw observations. Never retrofit a pass.
            acknowledged (mapcat #(when (vector? (:value %)) (:value %)) recvs)
            received-tuples (set acknowledged)
            lost (cset/difference committed received-tuples)
            dups (set (keep (fn [[tuple n]] (when (> n 1) tuple)) (frequencies acknowledged)))
            op-errors (keep (fn [op]
                              (when-let [reason (observation-error test op)]
                                {:process (:process op) :index (:index op) :reason reason})) ops)
            {:keys [pending intervals errors]} (send-intervals history)
            deliveries (when (empty? op-errors) (vec (raw-deliveries ops)))
            response-times (keep #(get-in % [:sqs-observation :received-at]) recvs)
            trace-errors (concat op-errors errors
                                 (when (seq pending) [:incomplete-send-invocation])
                                 (when (not= (count response-times) (count (set response-times)))
                                   [:reused-response-time])
                                 (when deliveries (identity-errors deliveries intervals)))
            ord (if (empty? trace-errors) (ordering-violations deliveries intervals) {})]
        {:valid?              (and (pos? (count committed))
                                   (empty? trace-errors)
                                   (empty? lost)
                                   (empty? dups)
                                   (empty? ord))
         :committed-sends     (count committed)
         :in-flight-sends     (count in-flight)
         :received            (count received-tuples)
         :lost                lost
         :duplicates          dups
         :raw-deliveries      (reduce + 0 (map #(let [v (get-in % [:sqs-observation :deliveries])]
                                                (if (vector? v) (count v) 0)) recvs))
         :ordering-checked?   (empty? trace-errors)
         :observation-errors (vec trace-errors)
         :ordering-violations ord}))))

;; ---------------------------------------------------------------------------
;; Generator
;; ---------------------------------------------------------------------------

(defn- send-op    [_t _p] {:f :send})
(defn- recv-op    [_t _p] {:f :recv})

(defn- mixed-generator
  "Mix sends and receives. send-fraction in [0, 1] picks a :send with
   that probability. Default 0.5. Receives are essential to drain the
   queue; too low a fraction starves consumers and inflates the
   in-flight count."
  [send-fraction]
  (gen/mix
    (concat (repeat (max 1 (Math/round (* 10.0 (double send-fraction)))) send-op)
            (repeat (max 1 (- 10 (Math/round (* 10.0 (double send-fraction))))) recv-op))))

;; ---------------------------------------------------------------------------
;; Workload & Test builders
;; ---------------------------------------------------------------------------

(def default-nodes ["n1" "n2" "n3" "n4" "n5"])

(defn- group-ids
  "Return [g0 g1 ... g(n-1)] used as MessageGroupId values."
  [n]
  (mapv #(str "g" %) (range n)))

(defn sqs-htfifo-workload
  "Builds the HT-FIFO workload map with custom client, generator, and
   checker. Shared seq-counters atom and per-run queue name are
   constructed here once so every client worker sees the same values
   (workers fan out via Jepsen's open!/setup! lifecycle, all reading
   the same record fields)."
  [opts]
  (let [partition-count (or (:partition-count opts) default-partition-count)
        group-count     (or (:group-count opts) default-group-count)
        send-fraction   (or (:send-fraction opts) 0.5)
        groups          (group-ids group-count)
        seq-counters    (fresh-seq-counters groups)
        observer        (fresh-observer)
        queue-name      (or (:queue-name opts) (fresh-queue-name))
        client          (->HTFIFOClient (or (:node->port opts)
                                            (zipmap default-nodes (repeat default-sqs-port)))
                                        (:sqs-region opts)
                                        groups
                                        seq-counters
                                        queue-name
                                        nil  ; sqs (per-worker)
                                        (atom nil)  ; URL shared by setup and execution clients
                                        partition-count
                                        observer)]
    {:client    client
     :sqs-observation-clock-id (:clock-id @observer)
     :generator (mixed-generator send-fraction)
     :checker   (ht-fifo-checker)}))

(defn elastickv-sqs-htfifo-test
  "Build a partitioned FIFO queue test."
  ([] (elastickv-sqs-htfifo-test {}))
  ([opts]
   (sqs/build-test (assoc opts :name (or (:name opts) "elastickv-sqs-htfifo"))
                   sqs-htfifo-workload)))

;; ---------------------------------------------------------------------------
;; CLI
;; ---------------------------------------------------------------------------

(def sqs-cli-opts
  "SQS-specific CLI options, appended to common opts."
  [[nil "--sqs-ports PORTS" "Comma-separated SQS ports (one per node)."
    :default nil
    :parse-fn (fn [s]
                (->> (str/split s #",")
                     (remove str/blank?)
                     (mapv #(Integer/parseInt %))))]
   [nil "--sqs-port PORT" "SQS port (applied to all nodes)."
    :default default-sqs-port
    :parse-fn #(Integer/parseInt %)]
   [nil "--sqs-region REGION" "AWS region the SDK signs against."
    :default "us-east-1"]
   [nil "--redis-port PORT" "Redis port."
    :default 6379
    :parse-fn #(Integer/parseInt %)]
   [nil "--partition-count N" "PartitionCount for the test queue (1, 2, 4, 8, 16, 32)."
    :default default-partition-count
    :parse-fn #(Integer/parseInt %)]
   [nil "--group-count N" "Number of distinct MessageGroupId values to spread sends across."
    :default default-group-count
    :parse-fn #(Integer/parseInt %)]
   [nil "--send-fraction F" "Probability a generator op is :send (rest are :recv). 0.0 and 1.0 still emit at least 1 op of each kind to prevent generator starvation; pass values in (0, 1) for a true mix."
    :default 0.5
    :parse-fn #(Double/parseDouble %)]
   [nil "--drain-time SECONDS" "Receive-only drain phase after the main generator finishes (default: visibility-timeout + 10s = 40s)."
    :default nil
    :parse-fn #(Integer/parseInt %)]])

(defn- prepare-sqs-opts
  "Translate parsed CLI options into the map elastickv-sqs-htfifo-test wants."
  [options]
  (let [sqs-ports (:sqs-ports options)
        options   (cli/parse-common-opts options sqs-ports)
        node->port (if sqs-ports
                     (cli/ports->node-map sqs-ports (:nodes options))
                     (zipmap (:nodes options) (repeat (:sqs-port options))))]
    (assoc options
      :sqs-host        (:host options)
      :node->port      node->port
      :sqs-port        (:sqs-port options)
      :sqs-region      (:sqs-region options)
      :redis-port      (:redis-port options)
      :partition-count (:partition-count options)
      :group-count     (:group-count options)
      :send-fraction   (:send-fraction options)
      :drain-time      (:drain-time options))))

(defn -main
  [& args]
  (cli/run-workload! args
                     (into cli/common-cli-opts sqs-cli-opts)
                     prepare-sqs-opts
                     elastickv-sqs-htfifo-test))
