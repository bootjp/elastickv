(ns elastickv.dynamodb-multi-table-workload
  "Composed-1 M5a multi-table variant of the DynamoDB list-append workload.

   Why a separate workload exists (see
   docs/design/2026_06_02_proposed_composed1_m5_jepsen_route_shuffle.md §3.3):
   the single-table elastickv.dynamodb-workload cannot exercise the 2PC
   path because kv/shard_key.go normalises every DynamoDB table-meta,
   item, and GSI key for one table to a single per-table route key
   (`!ddb|route|table|<base64(name)>`).  Routing always lands on one
   shard regardless of partition-key value, so dispatchMultiShardTxn /
   commitSecondaryTxns / the ErrTxnSecondaryRouteShiftedAfterPrimaryCommit
   sentinel never fire.

   This workload creates N=4 tables (jepsen_append_t1 … jepsen_append_t4)
   and uses a deterministic Elle-integer-key → (table, pk) mapping so:

   - each integer key k always lands at the same (table, pk) pair
     (Elle's append checker still sees a single key namespace);
   - default Elle txns of up to 4 mops with key-count=12 span all 4
     tables on every multi-op txn (k mod 4 distributes keys evenly);
   - the launch script's --shardRanges places t1-t2 in group 1 and
     t3-t4 in group 2, so any txn touching a key with k mod 4 in
     {0,1} AND a key with k mod 4 in {2,3} fans out to BOTH Raft
     groups and exercises dispatchMultiShardTxn.

   The TransactGetItems pre-read + TransactWriteItems atomic write
   pattern is intentionally identical to elastickv.dynamodb-workload so
   any G1c or G-single anomaly is attributable to the multi-shard 2PC
   path, not to a workload-shape difference.  The two files
   intentionally duplicate small helpers; refactoring shared helpers
   into a third namespace would couple the workloads through a
   maintained API and obscure the side-by-side comparability that is
   the point of running both."
  (:gen-class)
  (:require [clojure.string :as str]
            [cognitect.aws.client.api :as aws]
            [cognitect.aws.credentials :as creds]
            [elastickv.cli :as cli]
            [elastickv.db :as ekdb]
            [jepsen [client :as client]
                    [generator :as gen]
                    [net :as net]]
            [jepsen.control :as control]
            [jepsen.db :as jdb]
            [jepsen.nemesis :as nemesis]
            [jepsen.nemesis.combined :as combined]
            [jepsen.os :as os]
            [jepsen.os.debian :as debian]
            [jepsen.tests.cycle.append :as append]))

;; ---------------------------------------------------------------------------
;; Multi-table layout
;; ---------------------------------------------------------------------------

(def ^:private num-tables   4)
(def ^:private table-prefix "jepsen_append_t")
(def ^:private pk-attr      "pk")
(def ^:private val-attr     "val")

(def ^:private table-names
  "The N table names this workload owns.  1-indexed to match the
   launch-script's --shardRanges layout (tables 1-2 → group 1,
   3-4 → group 2).  Used by create-all-tables! and key-routing."
  (mapv (fn [i] (str table-prefix i)) (range 1 (inc num-tables))))

(defn- key->table-idx
  "Routes Elle's integer key k to one of 1..N tables.  Pure (mod k N)+1
   so the routing is stable across runs and the Elle checker sees a
   single key namespace.  With N=4 and Elle's default key-count=12,
   keys [0..11] distribute evenly across tables [1,2,3,4,…]; a 4-mop
   txn naturally spans all 4 tables."
  [k]
  (inc (mod k num-tables)))

(defn- key->table-name [k]
  (str table-prefix (key->table-idx k)))

(defn- key->pk
  "Returns the DynamoDB partition-key string for Elle's integer key k.
   Different k values that collide on the same table get distinct pks
   (e.g. k=0 → (t1,\"0\"); k=4 → (t1,\"1\")), so each Elle key still
   maps to a unique (table, pk) storage location."
  [k]
  (str (quot k num-tables)))

;; ---------------------------------------------------------------------------
;; Client construction
;; ---------------------------------------------------------------------------

(defn- make-ddb-client
  "See elastickv.dynamodb-workload/make-ddb-client — the rationale (dummy
   creds + fixed region to keep the SDK away from environment lookups)
   is identical.  Duplicated rather than imported so the two workloads
   stay side-by-side comparable."
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

(defn- create-one-table!
  "Create one of the N tables; ignore ResourceInUseException."
  [ddb tname]
  (try
    (ddb-invoke! ddb :CreateTable
                 {:TableName             tname
                  :KeySchema             [{:AttributeName pk-attr :KeyType "HASH"}]
                  :AttributeDefinitions  [{:AttributeName pk-attr :AttributeType "S"}]
                  :ProvisionedThroughput {:ReadCapacityUnits 5 :WriteCapacityUnits 5}})
    (catch clojure.lang.ExceptionInfo e
      (when-not (= "ResourceInUseException" (:type (ex-data e)))
        (throw e)))))

(defn- create-all-tables!
  "Create every table this workload owns.  Idempotent."
  [ddb]
  (doseq [t table-names]
    (create-one-table! ddb t)))

(defn- list-attr
  "Convert a Clojure vector of longs to a DynamoDB {:L [...]} attribute."
  [vs]
  {:L (mapv (fn [n] {:N (str n)}) vs)})

(defn- read-list-attr
  "Extract the :val list from a DynamoDB Item map (or nil if absent)."
  [item]
  (when-let [lv (get-in item [(keyword val-attr) :L])]
    (mapv #(Long/parseLong (:N %)) lv)))

(defn- dynamo-append!
  "Append single value v to the list stored at Elle key k.  Routes to
   the table for k and writes at (table, pk)."
  [ddb k v]
  (ddb-invoke! ddb :UpdateItem
               {:TableName                 (key->table-name k)
                :Key                       {pk-attr {:S (key->pk k)}}
                :UpdateExpression          "SET #v = list_append(if_not_exists(#v, :empty), :val)"
                :ExpressionAttributeNames  {"#v" val-attr}
                :ExpressionAttributeValues {":empty" {:L []}
                                            ":val"   {:L [{:N (str v)}]}}})
  nil)

(defn- dynamo-read
  "ConsistentRead from k's table.  Returns vector of longs, or nil if
   the item does not exist."
  [ddb k]
  (let [resp (ddb-invoke! ddb :GetItem
                          {:TableName      (key->table-name k)
                           :Key            {pk-attr {:S (key->pk k)}}
                           :ConsistentRead true})]
    (read-list-attr (:Item resp))))

(defn- dynamo-transact-get!
  "Atomically read all Elle keys in ks via TransactGetItems, with each
   Get pointing at its key's owning table.  TransactGetItems supports
   cross-table reads in a single call, which is exactly the multi-shard
   2PC pre-read path this workload is designed to exercise."
  [ddb ks]
  (let [ks-vec (vec ks)
        items  (mapv (fn [k]
                       {:Get {:TableName (key->table-name k)
                              :Key       {pk-attr {:S (key->pk k)}}}})
                     ks-vec)
        resp   (ddb-invoke! ddb :TransactGetItems {:TransactItems items})]
    (into {}
          (map (fn [k entry] [k (read-list-attr (:Item entry))])
               ks-vec
               (:Responses resp)))))

(defn- dynamo-transact-write!
  "Cross-table TransactWriteItems.  Mirrors the single-table workload's
   `dynamo-transact-write!`:

   - one Update action per written key (snapshot-conditioned);
   - one ConditionCheck action per read-but-not-written key.

   The only delta is TableName: each action carries the table that
   the key routes to, so a single TransactWriteItems call may span
   1-N tables and (with the launch-script's --shardRanges) up to two
   Raft groups → dispatchMultiShardTxn + commitSecondaryTxns + the
   ErrTxnSecondaryRouteShiftedAfterPrimaryCommit sentinel."
  [ddb writes snapshot]
  (let [by-key     (reduce (fn [acc [_ k v]]
                             (update acc k (fnil conj []) v))
                           (array-map)
                           writes)
        write-keys (set (keys by-key))
        read-only  (remove write-keys (keys snapshot))
        checks
        (mapv (fn [k]
                (let [v (get snapshot k)]
                  {:ConditionCheck
                   (merge {:TableName (key->table-name k)
                           :Key       {pk-attr {:S (key->pk k)}}}
                          (if (nil? v)
                            {:ConditionExpression      "attribute_not_exists(#pk)"
                             :ExpressionAttributeNames {"#pk" pk-attr}}
                            {:ConditionExpression       "#v = :cv"
                             :ExpressionAttributeNames  {"#v" val-attr}
                             :ExpressionAttributeValues {":cv" (list-attr v)}}))}))
              read-only)
        updates
        (mapv (fn [[k vs]]
                (let [v (get snapshot k)
                      attr-vals (merge {":empty" {:L []}
                                        ":val"   (list-attr vs)}
                                       (when-not (nil? v) {":cv" (list-attr v)}))]
                  {:Update
                   (merge {:TableName                 (key->table-name k)
                           :Key                       {pk-attr {:S (key->pk k)}}
                           :UpdateExpression          "SET #v = list_append(if_not_exists(#v, :empty), :val)"
                           :ExpressionAttributeNames  {"#v" val-attr}
                           :ExpressionAttributeValues attr-vals}
                          (if (nil? v)
                            {:ConditionExpression "attribute_not_exists(#v)"}
                            {:ConditionExpression "#v = :cv"}))}))
              by-key)]
    (ddb-invoke! ddb :TransactWriteItems
                 {:TransactItems (into checks updates)})))

;; ---------------------------------------------------------------------------
;; Jepsen client
;; ---------------------------------------------------------------------------

(defrecord DynamoDBMultiTableClient [node->port ddb]
  client/Client

  (open! [this test node]
    (let [port (get node->port node 8000)
          host (or (:dynamo-host test) (name node))]
      (assoc this :ddb (make-ddb-client host port))))

  (setup! [this _test]
    (create-all-tables! ddb))

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
            ;; Single micro-op: individual UpdateItem / GetItem on the
            ;; key's owning table.
            (let [[f k v :as mop] (first mops)]
              (assoc op :type :ok
                        :value [(case f
                                  :append (do (dynamo-append! ddb k v) mop)
                                  :r      [f k (dynamo-read ddb k)])]))

            ;; Multi-op: atomic cross-table TransactGetItems pre-read,
            ;; then RYOW simulation, then cross-table TransactWriteItems.
            (let [all-keys      (into #{} (map second mops))
                  snapshot      (dynamo-transact-get! ddb all-keys)
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
              (when (seq writes) (dynamo-transact-write! ddb writes snapshot))
              (assoc op :type :ok :value value')))))

      (catch clojure.lang.ExceptionInfo e
        (let [data     (ex-data e)
              err-type (:type data)
              category (:category data)]
          (cond
            (and (nil? err-type)
                 (#{:cognitect.anomalies/fault
                    :cognitect.anomalies/unavailable} category))
            (assoc op :type :info :error :network-error)

            (contains? #{"ConditionalCheckFailedException"
                         "InternalServerError"} err-type)
            (assoc op :type :info :error (str err-type))

            (= "TransactionCanceledException" err-type)
            (assoc op :type :fail :error (str err-type))

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

(defn dynamodb-append-multi-table-workload
  "Builds the multi-table list-append workload.  The append/test
   generator's integer keys are routed through key->table-name +
   key->pk to land deterministically on one of N=4 tables, exercising
   cross-table TransactGetItems / TransactWriteItems."
  [opts]
  (let [workload (append/test {:key-count          (or (:key-count opts) 12)
                               :min-txn-length     1
                               :max-txn-length     (or (:max-txn-length opts) 4)
                               :max-writes-per-key (or (:max-writes-per-key opts) 128)
                               :consistency-models [:strict-serializable]})
        client   (->DynamoDBMultiTableClient (or (:node->port opts)
                                                 (zipmap default-nodes (repeat 8000)))
                                             nil)]
    (assoc workload :client client)))

(defn elastickv-dynamodb-multi-table-test
  "Builds a Jepsen test map driving the multi-table list-append workload
   against an elastickv cluster.  The cluster is expected to launch with
   --raftGroups (≥2) and --shardRanges placing the table-route keys
   across groups so cross-table txns actually fan out to multiple Raft
   groups — see the design doc §3.3 and the M5a launch script."
  ([] (elastickv-dynamodb-multi-table-test {}))
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
         workload     (dynamodb-append-multi-table-workload (assoc opts :node->port node->port))]
     (merge workload
            {:name            (or (:name opts) "elastickv-dynamodb-append-multi-table")
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
  "DynamoDB-specific CLI options, appended to common opts.  Same
   surface as elastickv.dynamodb-workload's CLI so the same shell
   invocations work against either workload."
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
  "Transform parsed CLI options into the map expected by
   elastickv-dynamodb-multi-table-test."
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
                     elastickv-dynamodb-multi-table-test))
