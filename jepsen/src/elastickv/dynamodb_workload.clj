(ns elastickv.dynamodb-workload
  "Jepsen workload for elastickv's DynamoDB-compatible API.
   Uses the list-append consistency model: each key maps to a DynamoDB item
   whose 'val' attribute is a list of integers.

   Writes append to the list via UpdateItem (single-op) or TransactWriteItems
   (multi-op).  Reads fetch the list via GetItem with ConsistentRead=true.

   Uses cognitect/aws-api as the DynamoDB client so that the full SDK wire
   protocol (auth headers, error parsing, retry classification) is exercised
   against elastickv rather than a hand-rolled HTTP layer."
  (:gen-class)
  (:require [clojure.string :as str]
            [cognitect.aws.client.api :as aws]
            [cognitect.aws.credentials :as creds]
            [elastickv.cli :as cli]
            [elastickv.db :as ekdb]
            [jepsen [client :as client]
                    [core :as jepsen]
                    [generator :as gen]
                    [net :as net]]
            [jepsen.control :as control]
            [jepsen.db :as jdb]
            [jepsen.nemesis :as nemesis]
            [jepsen.nemesis.combined :as combined]
            [jepsen.os :as os]
            [jepsen.os.debian :as debian]
            [jepsen.tests.cycle.append :as append]))

(def ^:private table-name "jepsen_append")
(def ^:private pk-attr    "pk")
(def ^:private val-attr   "val")

;; ---------------------------------------------------------------------------
;; Client construction
;; ---------------------------------------------------------------------------

(defn- make-ddb-client
  "Returns a cognitect/aws-api DynamoDB client pointed at http://host:port.
   Dummy credentials and a fixed region are provided explicitly so the SDK
   never attempts credential or region resolution from the environment
   (which would fail in CI or local runs without AWS config)."
  [host port]
  (aws/client
    {:api                  :dynamodb
     :region               "us-east-1"
     :credentials-provider (creds/basic-credentials-provider
                             {:access-key-id     "dummy"
                              :secret-access-key "dummy"})
     :endpoint-override    {:protocol :http
                            :hostname  host
                            :port      port}}))

;; ---------------------------------------------------------------------------
;; Low-level DynamoDB helpers
;; ---------------------------------------------------------------------------

(defn- anomaly? [resp]
  (contains? resp :cognitect.anomalies/category))

(defn- ddb-invoke!
  "Invoke op against ddb-client, returning the parsed response map.
   Throws ex-info on any error (DynamoDB API error or network failure).
   ex-data keys: :type (DynamoDB __type string or nil), :category (anomaly
   keyword), :resp (raw cognitect/aws-api response)."
  [ddb-client op request]
  (let [resp (aws/invoke ddb-client {:op op :request request})]
    (if (anomaly? resp)
      (let [err-type (:__type resp)
            category (:cognitect.anomalies/category resp)
            msg      (or (:message resp)
                         (:Message resp)
                         (:cognitect.anomalies/message resp)
                         "")]
        (throw (ex-info (str "DynamoDB " (or err-type category) ": " msg)
                        {:type     err-type
                         :category category
                         :resp     resp})))
      resp)))

(defn- create-table!
  "Create the jepsen_append table; ignore ResourceInUseException."
  [ddb]
  (try
    (ddb-invoke! ddb :CreateTable
                 {:TableName             table-name
                  :KeySchema             [{:AttributeName pk-attr :KeyType "HASH"}]
                  :AttributeDefinitions  [{:AttributeName pk-attr :AttributeType "S"}]
                  :ProvisionedThroughput {:ReadCapacityUnits 5 :WriteCapacityUnits 5}})
    (catch clojure.lang.ExceptionInfo e
      (when-not (= "ResourceInUseException" (:type (ex-data e)))
        (throw e)))))

(defn- dynamo-append!
  "Append single value v to the list stored at key k."
  [ddb k v]
  (ddb-invoke! ddb :UpdateItem
               {:TableName                 table-name
                :Key                       {pk-attr {:S (str k)}}
                :UpdateExpression          "SET #v = list_append(if_not_exists(#v, :empty), :val)"
                :ExpressionAttributeNames  {"#v" val-attr}
                :ExpressionAttributeValues {":empty" {:L []}
                                            ":val"   {:L [{:N (str v)}]}}})
  nil)

(defn- dynamo-read
  "Read the list stored at key k.  Returns a vector of longs, or nil if the
   item does not exist.  ConsistentRead ensures we read from the leader."
  [ddb k]
  (let [resp (ddb-invoke! ddb :GetItem
                          {:TableName      table-name
                           :Key            {pk-attr {:S (str k)}}
                           :ConsistentRead true})]
    ;; cognitect/aws-api: Item is map[String,AttributeValue]; member keys
    ;; of AttributeValue union (S, N, L …) are Clojure keywords.
    (when-let [lv (get-in resp [:Item val-attr :L])]
      (mapv #(Long/parseLong (:N %)) lv))))

(defn- dynamo-transact-write!
  "Atomically write all append micro-ops as a single TransactWriteItems call.
   Multiple appends to the same key within one transaction are merged into one
   UpdateItem entry so they are applied atomically — real DynamoDB rejects
   requests with two operations on the same item (ValidationException), and
   elastickv enforces the same constraint server-side."
  [ddb writes]
  ;; Group values by key, preserving append order within each key.
  (let [by-key (reduce (fn [acc [_ k v]]
                         (update acc k (fnil conj []) v))
                       (array-map)
                       writes)]
    (ddb-invoke! ddb :TransactWriteItems
                 {:TransactItems
                  (mapv (fn [[k vs]]
                          {:Update
                           {:TableName                 table-name
                            :Key                       {pk-attr {:S (str k)}}
                            :UpdateExpression          "SET #v = list_append(if_not_exists(#v, :empty), :val)"
                            :ExpressionAttributeNames  {"#v" val-attr}
                            :ExpressionAttributeValues {":empty" {:L []}
                                                        ":val"   {:L (mapv (fn [v] {:N (str v)}) vs)}}}})
                        by-key)})))

;; ---------------------------------------------------------------------------
;; Jepsen client
;; ---------------------------------------------------------------------------

(defrecord DynamoDBClient [node->port ddb]
  client/Client

  (open! [this test node]
    (let [port (get node->port node 8000)
          host (or (:dynamo-host test) (name node))]
      (assoc this :ddb (make-ddb-client host port))))

  (setup! [this _test]
    (create-table! ddb))

  (teardown! [_this _test])

  (close! [this _test]
    (when ddb (aws/stop ddb))
    (assoc this :ddb nil))

  (invoke! [_this _test op]
    (try
      (case (:f op)
        :txn
        (let [mops (:value op)]
          (if (= 1 (count mops))
            ;; ---- Single micro-op: individual UpdateItem / GetItem ----
            (let [[f k v :as mop] (first mops)]
              (assoc op :type :ok
                        :value [(case f
                                  :append (do (dynamo-append! ddb k v) mop)
                                  :r      [f k (dynamo-read ddb k)])]))

            ;; ---- Multi-op: pre-read + read-your-own-writes ----
            ;; 1. Pre-read every key before applying any writes.  Each read is
            ;;    an independent ConsistentRead GetItem; they do NOT form an
            ;;    atomic multi-key snapshot (timestamps may differ per key).
            ;; 2. Simulate micro-ops in order; track local appends so that
            ;;    a :r after an :append in the same txn sees the append (RYOW).
            ;; 3. Dispatch all writes atomically via TransactWriteItems,
            ;;    merging same-key appends into one UpdateItem entry.
            (let [all-keys      (into #{} (map second mops))
                  snapshot      (into {} (map (fn [k] [k (dynamo-read ddb k)]) all-keys))
                  local-appends (volatile! {})
                  value' (mapv (fn [[f k v :as mop]]
                                 (case f
                                   :append (do (vswap! local-appends update k (fnil conj []) v)
                                               mop)
                                   :r      (let [base    (get snapshot k)
                                                 pending (get @local-appends k [])]
                                             [f k (if (and (nil? base) (empty? pending))
                                                    nil
                                                    (into (or base []) pending))])))
                               mops)
                  writes (filterv (fn [[f _ _]] (= f :append)) mops)]
              (when (seq writes) (dynamo-transact-write! ddb writes))
              (assoc op :type :ok :value value')))))

      (catch clojure.lang.ExceptionInfo e
        (let [data     (ex-data e)
              err-type (:type data)
              category (:category data)]
          (cond
            ;; Network / transport error (no DynamoDB __type)
            (and (nil? err-type)
                 (#{:cognitect.anomalies/fault
                    :cognitect.anomalies/unavailable} category))
            (assoc op :type :info :error :network-error)

            ;; Retryable DynamoDB server-side errors
            (contains? #{"ConditionalCheckFailedException"
                         "InternalServerError"
                         "TransactionCanceledException"} err-type)
            (assoc op :type :info :error (str err-type))

            ;; Definite API-level failures
            (= "ValidationException" err-type)
            (assoc op :type :fail
                      :error (str err-type ": "
                                  (get-in data [:resp :message]
                                    (get-in data [:resp :Message] ""))))

            :else
            (assoc op :type :info :error (.getMessage e)))))

      (catch Exception e
        (assoc op :type :info :error (.getMessage e))))))

;; ---------------------------------------------------------------------------
;; Workload & Test builders
;; ---------------------------------------------------------------------------

(def default-nodes ["n1" "n2" "n3" "n4" "n5"])

(defn dynamodb-append-workload
  "Builds the list-append workload targeting the DynamoDB endpoint.
   Multi-op write transactions are executed atomically via TransactWriteItems.
   Reads within a transaction are ConsistentRead GetItem calls; RYOW semantics
   are implemented client-side for reads that follow a write on the same key."
  [opts]
  (let [workload (append/test {:key-count          (or (:key-count opts) 12)
                               :min-txn-length     1
                               :max-txn-length     (or (:max-txn-length opts) 4)
                               :max-writes-per-key (or (:max-writes-per-key opts) 128)
                               :consistency-models [:strict-serializable]})
        client   (->DynamoDBClient (or (:node->port opts)
                                       (zipmap default-nodes (repeat 8000)))
                                   nil)]
    (assoc workload :client client)))

(defn elastickv-dynamodb-test
  "Builds a Jepsen test map that drives elastickv's DynamoDB-compatible API
   with the list-append workload."
  ([] (elastickv-dynamodb-test {}))
  ([opts]
   (let [nodes        (or (:nodes opts) default-nodes)
         dynamo-ports (or (:dynamo-ports opts) (repeat (count nodes) (or (:dynamo-port opts) 8000)))
         node->port   (or (:node->port opts) (cli/ports->node-map dynamo-ports nodes))
         local?       (:local opts)
         db           (if local?
                        jdb/noop
                        (ekdb/db {:grpc-port    (or (:grpc-port opts) 50051)
                                  :redis-port   (or (:redis-port opts) 6379)
                                  :dynamo-port  node->port
                                  :raft-groups  (:raft-groups opts)
                                  :shard-ranges (:shard-ranges opts)}))
         rate         (double (or (:rate opts) 5))
         time-limit   (or (:time-limit opts) 30)
         faults       (if local?
                        []
                        (cli/normalize-faults (or (:faults opts) [:partition :kill])))
         nemesis-p    (when-not local?
                        (combined/nemesis-package {:db       db
                                                   :faults   faults
                                                   :interval (or (:fault-interval opts) 40)}))
         nemesis-gen  (if nemesis-p
                        (:generator nemesis-p)
                        (gen/once {:type :info :f :noop}))
         workload     (dynamodb-append-workload (assoc opts :node->port node->port))]
     (merge workload
            {:name            (or (:name opts) "elastickv-dynamodb-append")
             :nodes           nodes
             :db              db
             :dynamo-host     (:dynamo-host opts)
             :os              (if local? os/noop debian/os)
             :net             (if local? net/noop net/iptables)
             :ssh             (merge {:username               "vagrant"
                                      :private-key-path       "/home/vagrant/.ssh/id_rsa"
                                      :strict-host-key-checking false}
                                     (when local? {:dummy true})
                                     (:ssh opts))
             :remote          control/ssh
             :nemesis         (if nemesis-p (:nemesis nemesis-p) nemesis/noop)
             :final-generator nil
             :concurrency     (or (:concurrency opts) 5)
             :generator       (->> (:generator workload)
                                   (gen/nemesis nemesis-gen)
                                   (gen/stagger (/ rate))
                                   (gen/time-limit time-limit))}))))

;; ---------------------------------------------------------------------------
;; CLI
;; ---------------------------------------------------------------------------

(def dynamo-cli-opts
  "DynamoDB-specific CLI options, appended to common opts."
  [[nil "--dynamo-ports PORTS" "Comma-separated DynamoDB ports (one per node)."
    :default nil
    :parse-fn (fn [s]
                (->> (str/split s #",")
                     (remove str/blank?)
                     (mapv #(Integer/parseInt %))))]
   [nil "--dynamo-port PORT" "DynamoDB port (applied to all nodes)."
    :default 8000
    :parse-fn #(Integer/parseInt %)]
   [nil "--redis-port PORT" "Redis port."
    :default 6379
    :parse-fn #(Integer/parseInt %)]
   [nil "--max-txn-length N" "Maximum number of micro-ops per transaction."
    :default nil
    :parse-fn #(Integer/parseInt %)]])

(defn- prepare-dynamo-opts
  "Transform parsed CLI options into the map expected by elastickv-dynamodb-test."
  [options]
  (let [dynamo-ports (:dynamo-ports options)
        options      (cli/parse-common-opts options dynamo-ports)
        node->port   (if dynamo-ports
                       (cli/ports->node-map dynamo-ports (:nodes options))
                       (zipmap (:nodes options) (repeat (:dynamo-port options))))]
    (assoc options
      :dynamo-host (:host options)
      :node->port  node->port
      :dynamo-port (:dynamo-port options)
      :redis-port  (:redis-port options))))

(defn -main
  [& args]
  (cli/run-workload! args
                     (into cli/common-cli-opts dynamo-cli-opts)
                     prepare-dynamo-opts
                     elastickv-dynamodb-test))
