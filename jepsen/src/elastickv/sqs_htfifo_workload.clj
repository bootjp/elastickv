(ns elastickv.sqs-htfifo-workload
  "Jepsen workload for elastickv's HT-FIFO (high-throughput FIFO) SQS-compatible
   queues — partitioned FIFO queues created with PartitionCount > 1.

   Pattern follows aphyr's classic Jepsen RabbitMQ analysis: track every
   :send and :recv in the history, then a custom checker verifies three
   contracts that AWS HT-FIFO is supposed to honour even under partition
   and node-loss faults:

   1. Within-group ordering — for any MessageGroupId, the sequence of
      received seq values (sorted by global completion time across all
      consumers) is strictly increasing. Strict (rather than merely
      non-decreasing) is what the checker enforces, since seqs are
      assigned monotonically by next-seq! and equal seqs would already
      be flagged as duplicates by contract 3.
   2. No loss — every (group, seq) successfully :sent eventually appears
      in the :recv history. Sends with :info status are treated as
      possibly-committed and not counted as lost.
   3. No duplicates — every (group, seq) appears at most once in the
      :recv history. ContentBasedDeduplication on the queue + a unique
      (group, seq) body is what enforces this server-side, so a duplicate
      here is a real bug (e.g. a deletion that did not commit).

   Each MessageGroupId is hashed by partitionFor (FNV-1a) onto one of N
   partitions; with several distinct groups the workload exercises
   cross-partition delivery, while ContentBasedDeduplication + per-group
   monotonic seqs keeps the assertions checkable from the client side."
  (:gen-class)
  (:require [clojure.set :as cset]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [cognitect.aws.client.api :as aws]
            [cognitect.aws.credentials :as creds]
            [elastickv.cli :as cli]
            [elastickv.db :as ekdb]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [net :as net]]
            [jepsen.control :as control]
            [jepsen.db :as jdb]
            [jepsen.nemesis :as nemesis]
            [jepsen.nemesis.combined :as combined]
            [jepsen.os :as os]
            [jepsen.os.debian :as debian]))

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
;; SQS client construction
;; ---------------------------------------------------------------------------

(defn- make-sqs-client
  "Returns a cognitect/aws-api SQS client pointed at http://host:port.
   Dummy credentials match the elastickv server's open-endpoint mode (no
   --sqsCredentialsFile passed → adapter accepts any signed request)."
  [host port region]
  (aws/client
    {:api                  :sqs
     :region               (or region "us-east-1")
     :credentials-provider (creds/basic-credentials-provider
                             {:access-key-id     "dummy"
                              :secret-access-key "dummy"})
     :endpoint-override    {:protocol :http
                            :hostname  host
                            :port      port}}))

(defn- anomaly? [resp]
  (contains? resp :cognitect.anomalies/category))

(defn- sqs-invoke!
  "Invoke op against sqs-client, returning the parsed response.
   Throws ex-info on any error (SQS API error or transport failure).
   ex-data: :type (SQS __type), :category (anomaly), :resp (raw)."
  [sqs op request]
  (let [resp (aws/invoke sqs {:op op :request request})]
    (if (anomaly? resp)
      (let [err-type (:__type resp)
            category (:cognitect.anomalies/category resp)
            msg      (or (:message resp)
                         (:Message resp)
                         (:cognitect.anomalies/message resp)
                         "")]
        (throw (ex-info (str "SQS " (or err-type category) ": " msg)
                        {:type     err-type
                         :category category
                         :resp     resp})))
      resp)))

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
                (sqs-invoke! sqs :CreateQueue
                             {:QueueName  queue-name
                              :Attributes attrs})
                (catch clojure.lang.ExceptionInfo e
                  (let [err-type (:type (ex-data e))]
                    (if (or (= "QueueAlreadyExists" err-type)
                            (= "QueueNameExists" err-type))
                      (sqs-invoke! sqs :GetQueueUrl {:QueueName queue-name})
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

(defrecord HTFIFOClient [node->port region groups seq-counters queue-name sqs queue-url partition-count]
  client/Client

  (open! [this test node]
    (let [port (get node->port node default-sqs-port)
          host (or (:sqs-host test) (name node))]
      (assoc this :sqs (make-sqs-client host port region))))

  (setup! [this _test]
    (let [url (create-htfifo-queue! sqs queue-name partition-count)]
      (info "HT-FIFO test queue ready" url "partitions=" partition-count)
      (assoc this :queue-url url)))

  (teardown! [_this _test]
    ;; Leave the queue around — the test cluster is torn down by db/teardown!.
    ;; A best-effort DeleteQueue here would race the partition-isolated nodes.
    nil)

  (close! [this _test]
    (when sqs (aws/stop sqs))
    (assoc this :sqs nil :queue-url nil))

  (invoke! [_this _test op]
    (try
      (case (:f op)
        :send
        ;; Compute (group, seq, body) BEFORE the SQS call so the
        ;; op carries :value [group seq-num] regardless of whether
        ;; the call succeeded — :info sends with their tuple intact
        ;; let a future checker reason about in-flight messages
        ;; (today's "lost" formula is committed-only, but matching
        ;; Jepsen's standard convention keeps the history
        ;; interpretable). Re-throw on send failure so the outer
        ;; catch performs the existing error classification, but
        ;; with the enriched op as context (passed via ex-data so
        ;; the outer catch can pull it out).
        (let [group   (rand-nth groups)
              seq-num (next-seq! seq-counters group)
              body    (encode-body group seq-num)
              op'     (assoc op :value [group seq-num])]
          (try
            (sqs-invoke! sqs :SendMessage
                         {:QueueUrl      queue-url
                          :MessageBody   body
                          :MessageGroupId group})
            (assoc op' :type :ok)
            (catch clojure.lang.ExceptionInfo e
              ;; Re-raise with the enriched op stashed so the outer
              ;; catch returns it instead of the bare op. The outer
              ;; catch checks for :enriched-op in ex-data first.
              (throw (ex-info (.getMessage e)
                              (merge (ex-data e) {:enriched-op op'})
                              e)))))

        :recv
        (let [resp (sqs-invoke! sqs :ReceiveMessage
                                {:QueueUrl            queue-url
                                 :MaxNumberOfMessages receive-batch-size
                                 :VisibilityTimeout   visibility-timeout-seconds
                                 :WaitTimeSeconds     receive-wait-seconds})
              msgs (or (:Messages resp) [])
              parsed (keep (fn [m]
                             (when-let [decoded (decode-body (:Body m))]
                               (assoc decoded
                                      :receipt-handle (:ReceiptHandle m)
                                      :message-id     (:MessageId m))))
                           msgs)]
          ;; SQS contract: a message is "received" (and the duplicate-
          ;; detection contract activates) only after a successful
          ;; DeleteMessage acks it. A failed delete (transport fault,
          ;; partition mid-ack) leaves the message visible — the next
          ;; receive WILL see it again, and that re-delivery is correct
          ;; SQS behaviour, not a duplicate the checker should flag.
          ;; So: include only successfully-deleted tuples in :value.
          ;; Tuples whose delete failed are dropped here and naturally
          ;; reappear in a subsequent :recv. We log the failure so a
          ;; spike in the warn rate (vs duplicate signal in the report)
          ;; is the right triage cue.
          (let [acked (volatile! [])]
            (doseq [{:keys [group seq receipt-handle]} parsed]
              (try
                (sqs-invoke! sqs :DeleteMessage
                             {:QueueUrl      queue-url
                              :ReceiptHandle receipt-handle})
                (vswap! acked conj [group seq])
                (catch clojure.lang.ExceptionInfo e
                  (warn e "DeleteMessage failed; tuple will be redelivered"
                        {:group group :seq seq}))))
            (assoc op :type :ok :value @acked))))

      (catch clojure.lang.ExceptionInfo e
        (let [data     (ex-data e)
              err-type (:type data)
              category (:category data)
              ;; If the :send branch attached an enriched op (with
              ;; [group seq-num] :value), use it so the resulting :info
              ;; / :fail op still carries the tuple the checker can see.
              base     (or (:enriched-op data) op)]
          (cond
            ;; Transport faults (network partition, kill, peer down).
            ;; :info: the operation may or may not have committed.
            (and (nil? err-type)
                 (#{:cognitect.anomalies/fault
                    :cognitect.anomalies/unavailable
                    :cognitect.anomalies/interrupted} category))
            (assoc base :type :info :error :network-error)

            ;; Server-side InternalFailure / 5xx — possibly committed.
            (#{"InternalFailure" "InternalServerError" "ServiceUnavailable"} err-type)
            (assoc base :type :info :error (str err-type))

            ;; Definite client-side rejection — operation did not commit.
            (#{"InvalidParameterValue" "QueueDoesNotExist"
               "ReceiptHandleIsInvalid" "InvalidIdFormat"} err-type)
            (assoc base :type :fail :error (str err-type))

            :else
            (assoc base :type :info :error (or err-type
                                                category
                                                (.getMessage e))))))

      (catch Exception e
        (assoc op :type :info :error (.getMessage e))))))

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

(defn- collect-receives
  "Return a list of {:group g :seq s :time t :process p :index i} maps
   in completion-time order, one per (group, seq) tuple actually
   surfaced by a successful :recv op. Each tuple carries the op's
   :time / :process / its position within the batch so per-group
   ordering can be checked against a globally-consistent timeline.

   Sort key is (juxt :time :process :index) — :time is the primary,
   :process tiebreaks two workers polling at the same nanosecond
   (rare but possible), and :index preserves the within-batch order
   the server returned (which is the FIFO order for messages in the
   same batch). Without :index, sort-by would only be stable across
   the input ordering and the per-group seqs from one batch could
   appear out-of-order in the sorted output."
  [history]
  (->> history
       (filter #(and (= :recv (:f %)) (= :ok (:type %))))
       (mapcat (fn [op]
                 (map-indexed
                   (fn [i [g s]]
                     {:group g :seq s
                      :time (:time op)
                      :process (:process op)
                      :index i})
                   (:value op))))
       (sort-by (juxt :time :process :index))))

(defn- ordering-violations
  "For each group, return the list of out-of-order pairs in the
   completion-time-ordered receive sequence. Returns a map of
   group → [{:prev p :curr c} ...] (empty if no violation)."
  [received-events]
  (let [per-group (group-by :group received-events)]
    (->> per-group
         (keep (fn [[group events]]
                 (let [seqs   (mapv :seq events)
                       pairs  (map vector seqs (rest seqs))
                       breaks (filter (fn [[p c]] (>= p c)) pairs)]
                   (when (seq breaks)
                     [group (mapv (fn [[p c]] {:prev p :curr c}) breaks)]))))
         (into {}))))

(defn- duplicate-receives
  "Return the set of (group, seq) tuples that appeared more than once in
   the receive history (FIFO contract violation)."
  [received-events]
  (->> received-events
       (group-by (juxt :group :seq))
       (keep (fn [[k events]] (when (> (count events) 1) k)))
       set))

(defn ht-fifo-checker
  "Custom Jepsen checker for the HT-FIFO contract. Returns
   {:valid? bool :sent N :received N :lost #{...} :duplicates #{...}
    :ordering-violations {...}}."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [{:keys [committed in-flight]} (collect-sends history)
            ;; in-flight is reported in the result map for diagnostics
            ;; (operators want to see how many sends were ambiguous);
            ;; the loss formula does not subtract it because
            ;; committed/in-flight are disjoint by construction.
            received-events (collect-receives history)
            received-tuples (set (map (fn [{:keys [group seq]}] [group seq])
                                      received-events))
            ;; "lost" = committed sends that never arrived. :info sends
            ;; (in-flight) are excluded from `committed` at collection
            ;; time, not subtracted here, so committed and in-flight are
            ;; always disjoint.
            lost (cset/difference committed received-tuples)
            dups (duplicate-receives received-events)
            ord  (ordering-violations received-events)]
        {:valid?              (and (empty? lost)
                                   (empty? dups)
                                   (empty? ord))
         :committed-sends     (count committed)
         :in-flight-sends     (count in-flight)
         :received            (count received-tuples)
         :lost                lost
         :duplicates          dups
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

(defn- drain-generator
  "Drain phase: only :recv ops, run after the main generator finishes so
   the checker sees the in-flight messages get delivered."
  []
  (gen/repeat {:f :recv}))

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
        queue-name      (or (:queue-name opts) (fresh-queue-name))
        client          (->HTFIFOClient (or (:node->port opts)
                                            (zipmap default-nodes (repeat default-sqs-port)))
                                        (:sqs-region opts)
                                        groups
                                        seq-counters
                                        queue-name
                                        nil  ; sqs (per-worker)
                                        nil  ; queue-url
                                        partition-count)]
    {:client    client
     :generator (mixed-generator send-fraction)
     :checker   (ht-fifo-checker)}))

(defn elastickv-sqs-htfifo-test
  "Builds a Jepsen test map that drives elastickv's HT-FIFO SQS endpoint."
  ([] (elastickv-sqs-htfifo-test {}))
  ([opts]
   (let [nodes      (or (:nodes opts) default-nodes)
         sqs-ports  (or (:sqs-ports opts)
                        (repeat (count nodes) (or (:sqs-port opts) default-sqs-port)))
         node->port (or (:node->port opts) (cli/ports->node-map sqs-ports nodes))
         sqs-region (or (:sqs-region opts) "us-east-1")
         local?     (:local opts)
         db         (if local?
                      jdb/noop
                      (ekdb/db {:grpc-port    (or (:grpc-port opts) 50051)
                                :redis-port   (or (:redis-port opts) 6379)
                                :sqs-port     node->port
                                :sqs-region   sqs-region
                                :raft-groups  (:raft-groups opts)
                                :shard-ranges (:shard-ranges opts)}))
         rate       (double (or (:rate opts) 5))
         time-limit (or (:time-limit opts) 30)
         ;; Drain must outlast the visibility-timeout window plus a
         ;; safety buffer. Otherwise a message that becomes invisible
         ;; right at drain start reappears at exactly drain-end (or
         ;; later) and gen/time-limit can fire before the next :recv
         ;; surfaces it — the checker would then report a :lost that
         ;; the server still owns. visibility-timeout + 10s closes
         ;; that race. The +10s also dominates the (max 5,
         ;; time-limit/6) short-test floor at any reasonable
         ;; time-limit, so the floor is no longer needed.
         drain-time (or (:drain-time opts)
                        (+ visibility-timeout-seconds 10))
         faults     (if local?
                      []
                      (cli/normalize-faults (or (:faults opts) [:partition :kill])))
         nemesis-p  (when-not local?
                      (combined/nemesis-package {:db       db
                                                 :faults   faults
                                                 :interval (or (:fault-interval opts) 40)}))
         nemesis-gen (if nemesis-p
                       (:generator nemesis-p)
                       (gen/once {:type :info :f :noop}))
         workload   (sqs-htfifo-workload (assoc opts :node->port node->port))
         main-gen   (->> (:generator workload)
                         (gen/nemesis nemesis-gen)
                         (gen/stagger (/ rate))
                         (gen/time-limit time-limit))
         drain-gen  (->> (drain-generator)
                         (gen/stagger (/ rate))
                         (gen/time-limit drain-time))]
     (merge workload
            {:name            (or (:name opts) "elastickv-sqs-htfifo")
             :nodes           nodes
             :db              db
             :sqs-host        (:sqs-host opts)
             :os              (if local? os/noop debian/os)
             :net             (if local? net/noop net/iptables)
             :ssh             (merge {:username                  "vagrant"
                                      :private-key-path          "/home/vagrant/.ssh/id_rsa"
                                      :strict-host-key-checking  false}
                                     (when local? {:dummy true})
                                     (:ssh opts))
             :remote          control/ssh
             :nemesis         (if nemesis-p (:nemesis nemesis-p) nemesis/noop)
             :final-generator nil
             :concurrency     (or (:concurrency opts) 8)
             :generator       (gen/phases main-gen drain-gen)}))))

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
   [nil "--send-fraction F" "Probability a generator op is :send (rest are :recv)."
    :default 0.5
    :parse-fn #(Double/parseDouble %)]
   [nil "--drain-time SECONDS" "Receive-only drain phase after the main generator finishes."
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
