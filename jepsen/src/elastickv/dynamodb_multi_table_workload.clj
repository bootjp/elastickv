(ns elastickv.dynamodb-multi-table-workload
  "Composed-1 M5a multi-table variant of the DynamoDB list-append workload.

   Why a separate workload exists (see
   docs/design/2026_06_02_implemented_composed1_m5_jepsen_route_shuffle.md §3.3):
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
  (:require [clojure.java.shell :as shell]
            [clojure.string :as str]
            [cognitect.aws.client.api :as aws]
            [cognitect.aws.credentials :as creds]
            [elastickv.cli :as cli]
            [elastickv.composed1-nemesis :as composed1]
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

(defn- delete-one-table!
  "Delete one of the N tables; ignore ResourceNotFoundException so
   teardown! is idempotent across cluster states (e.g. a re-run after
   a partial setup failure)."
  [ddb tname]
  (try
    (ddb-invoke! ddb :DeleteTable {:TableName tname})
    (catch clojure.lang.ExceptionInfo e
      (when-not (= "ResourceNotFoundException" (:type (ex-data e)))
        (throw e)))))

(defn- delete-all-tables!
  "Delete every table this workload owns.  Idempotent.

   Without this, leftover Items from a previous run remain in the four
   tables across runs; the next run's TransactGetItems pre-read picks
   them up as snapshot values, the txn's :r mops report those values
   to Jepsen, and Elle sees a history that contains writes it never
   generated — leading to spurious G-single / G1c false positives
   (review feedback on PR #916).  Cleaning up here keeps each run's
   history closed under the writes Elle actually generated."
  [ddb]
  (doseq [t table-names]
    (delete-one-table! ddb t)))

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
    ;; DynamoDB guarantees :Responses is positionally aligned with the
    ;; :TransactItems we sent.  Assert it so a future cognitect SDK
    ;; that ever normalises / re-orders the response surfaces loudly
    ;; rather than silently corrupting the snapshot map (review feedback
    ;; on PR #916).
    (assert (= (count ks-vec) (count (:Responses resp)))
            "TransactGetItems :Responses must be positionally aligned with the requested items")
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
;; M5a setup-hook verification (design doc §3.3)
;; ---------------------------------------------------------------------------

(def ^:private default-list-routes-bin
  "Default path to the cmd/elastickv-list-routes Go helper.  Tunable
   via (:list-routes-bin opts) so the binary can sit anywhere on disk
   when the launch script doesn't put it on PATH."
  "elastickv-list-routes")

(def ^:private default-grpc-port
  "Default elastickv server gRPC port.  Combined with the test's
   first node hostname (or 127.0.0.1 when --local) to form the
   --address argument."
  50051)

(defn- default-grpc-host-port-for
  "Returns the default --address for elastickv-list-routes when the
   test does NOT pass an explicit :grpc-host-port.  Resolves to the
   first node's hostname + default port — works in both local mode
   (first node is typically 127.0.0.1 or 'n1') and distributed Jepsen
   runs where database nodes live on separate hosts (review feedback
   on PR #925).  Falls back to 127.0.0.1:50051 only when :nodes is
   missing entirely."
  [test]
  (let [node (first (:nodes test))]
    (if node
      (str (name node) ":" default-grpc-port)
      (str "127.0.0.1:" default-grpc-port))))

(defn- distinct-group-ids
  "Parses elastickv-list-routes' JSON output and returns the set of
   distinct raft_group_id values present.  Uses a regex rather than
   pulling in a JSON dependency: the CLI emits a stable JSON shape
   tested in cmd/elastickv-list-routes/main_test.go, and this hook
   only needs a coarse-grained 'how many groups own routes' check.
   If a future ListRoutes change introduces a different field name,
   the regex returns the empty set and the assertion below fails
   loudly — strictly better than silently passing on an unexpected
   shape."
  [json-str]
  (->> (re-seq #"\"raft_group_id\"\s*:\s*(\d+)" json-str)
       (map (comp #(Long/parseLong %) second))
       set))

(defn- verify-multi-group-routing!
  "Asserts the cluster reports >=2 distinct Raft groups in its route
   catalog.  Shells out to cmd/elastickv-list-routes (the JSON
   contract is stable; design doc §3.3).  Throws ex-info on any
   failure so the Jepsen setup-hook fails fast with a clear error
   pointing the operator at the launch-script flag the cluster is
   missing.

   opts (read from the test map):
     :list-routes-bin — absolute path to the CLI (default \"elastickv-list-routes\";
                        assumes PATH or matching launch-script PWD).
     :grpc-host-port  — --address arg to the CLI; defaults to the
                        first node's hostname + 50051 so distributed
                        Jepsen runs work without flag plumbing
                        (review feedback on PR #925)."
  [test]
  (let [bin    (or (:list-routes-bin test) default-list-routes-bin)
        addr   (or (:grpc-host-port test)  (default-grpc-host-port-for test))
        result (shell/sh bin "--address" addr)]
    (when-not (zero? (:exit result))
      (throw (ex-info (str bin " --address " addr " failed: " (:err result))
                      {:exit   (:exit result)
                       :stdout (:out result)
                       :stderr (:err result)})))
    (let [groups (distinct-group-ids (:out result))]
      (when (< (count groups) 2)
        (throw (ex-info
                 (str "M5a multi-group routing precondition failed: only "
                      (count groups) " distinct Raft group(s) observed in the catalog "
                      "(expected >=2).  Re-launch the cluster with both --raftGroups "
                      "AND --shardRanges (see scripts/run-jepsen-m5-local.sh) — "
                      "without both flags --shardRanges collapses every range into "
                      "the default group 1 and dispatchMultiShardTxn never fires.")
                 {:groups   groups
                  :bin      bin
                  :address  addr
                  :raw-out  (:out result)})))
      groups)))

;; ---------------------------------------------------------------------------
;; Jepsen client
;; ---------------------------------------------------------------------------

(defrecord DynamoDBMultiTableClient [node->port ddb]
  client/Client

  (open! [this test node]
    (let [port (get node->port node 8000)
          host (or (:dynamo-host test) (name node))]
      (assoc this :ddb (make-ddb-client host port))))

  (setup! [_this test]
    ;; M5a setup-hook verification per design doc §3.3.  Asserts the
    ;; launch script's --raftGroups / --shardRanges actually placed
    ;; the M5 table-route keys on >=2 distinct Raft groups before any
    ;; workload op runs.  Fails fast with a clear error so the operator
    ;; knows the cluster needs to be relaunched — strictly better than
    ;; silently running the workload on a single-shard layout and
    ;; reporting "zero G1c" without ever exercising dispatchMultiShardTxn.
    ;;
    ;; jepsen.client/setup! is invoked exactly once per test (not
    ;; per-node like jepsen.db/setup!), so no first-node gating is
    ;; required.
    (verify-multi-group-routing! test)
    ;; No DescribeTable poll loop is needed: elastickv's adapter
    ;; returns TableStatus=ACTIVE synchronously in the CreateTable
    ;; response (adapter/dynamodb.go:779-783), so the table is
    ;; queryable as soon as create-one-table! returns.  Real DynamoDB's
    ;; CreateTable is async and would need a poll; if this workload is
    ;; ever pointed at a real DynamoDB endpoint, add a DescribeTable
    ;; loop here (review note on PR #916 c46b2b5e).
    (create-all-tables! ddb))

  (teardown! [_this _test]
    ;; Delete every table this workload created so the next run starts
    ;; with empty Items.  See delete-all-tables! for the Elle-history
    ;; rationale (review feedback on PR #916).
    (delete-all-tables! ddb))

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
                         "InternalServerError"
                         "TransactionCanceledException"} err-type)
            (assoc op :type :info :error (str err-type))

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
   cross-table TransactGetItems / TransactWriteItems.

   Refuses to build when (:key-count opts) is not a multiple of
   num-tables: an uneven split silently gives some tables fewer
   keys (so 1-key txns on those tables never trigger multi-group
   dispatch — review feedback on PR #916).  The default 12 satisfies
   this; tunable via --key-count from the CLI."
  [opts]
  (let [key-count (or (:key-count opts) 12)]
    (assert (zero? (mod key-count num-tables))
            (str "key-count (" key-count
                 ") must be a multiple of num-tables (" num-tables
                 ") so keys distribute evenly across all tables"))
    (let [workload (append/test {:key-count          key-count
                                 :min-txn-length     1
                                 :max-txn-length     (or (:max-txn-length opts) 4)
                                 :max-writes-per-key (or (:max-writes-per-key opts) 128)
                                 :consistency-models [:strict-serializable]})
          client   (->DynamoDBMultiTableClient (or (:node->port opts)
                                                   (zipmap default-nodes (repeat 8000)))
                                               nil)]
      (assoc workload :client client))))

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
                                  :shard-ranges (:shard-ranges opts)
                                  :server-env   (:server-env opts)}))
         rate         (double (or (:rate opts) 5))
         time-limit   (or (:time-limit opts) 30)
         faults       (if local?
                        []
                        (cli/normalize-faults (or (:faults opts) [:partition :kill])))
         nemesis-packages
         (cond-> []
           (not local?)
           (conj (combined/nemesis-package {:db       db
                                            :faults   faults
                                            :interval (or (:fault-interval opts) 40)}))

           (:composed1-route-shuffle opts)
           (conj (composed1/route-shuffle-package opts)))
         nemesis-p    (combined/compose-packages nemesis-packages)
         nemesis-gen  (if nemesis-p
                        (:generator nemesis-p)
                        (gen/once {:type :info :f :noop}))
         workload     (dynamodb-append-multi-table-workload (assoc opts :node->port node->port))]
     (merge workload
            {:name            (or (:name opts) "elastickv-dynamodb-append-multi-table")
             :nodes           nodes
             :db              db
             ;; Setup-hook verification keys — read by
             ;; verify-multi-group-routing! at workload setup! time.
             ;; Threaded into the test map (not the workload map)
             ;; because jepsen.client/setup! receives the test, not
             ;; opts; the M5a launch script passes these via the
             ;; --list-routes-bin and --grpc-host-port CLI flags
             ;; defined in dynamo-cli-opts below.
             :list-routes-bin (:list-routes-bin opts)
             :split-bin       (:split-bin opts)
             :grpc-host-port  (:grpc-host-port opts)
             :dynamo-host     (:dynamo-host opts)
             :os              (if local? os/noop debian/os)
             :net             (if local? net/noop net/iptables)
             :ssh             (merge {:username               "vagrant"
                                      :private-key-path       "/home/vagrant/.ssh/id_rsa"
                                      :strict-host-key-checking false}
                                     (when local? {:dummy true})
                                     (:ssh opts))
             :remote          control/ssh
             :nemesis         (or (:nemesis nemesis-p) nemesis/noop)
             :final-generator (when nemesis-p (:final-generator nemesis-p))
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
    :parse-fn #(Integer/parseInt %)]
   [nil "--list-routes-bin PATH" "Path to the cmd/elastickv-list-routes Go helper used by the workload's setup-hook verification.  Defaults to bare-name 'elastickv-list-routes' (assumes PATH lookup); pass an absolute path when invoking from a launch script that builds the binary into a tmp dir."
    :default nil]
   [nil "--split-bin PATH" "Path to the cmd/elastickv-split Go helper used by the route-shuffle nemesis.  Defaults to bare-name 'elastickv-split' (assumes PATH lookup); pass an absolute path when invoking from a launch script that builds the binary into a tmp dir."
    :default nil]
   [nil "--grpc-host-port HOST:PORT" "gRPC --address argument passed to elastickv-list-routes by the setup-hook verification.  Default 127.0.0.1:50051 matches scripts/run-jepsen-m5-local.sh's PROC_ADDR."
    :default nil]
   [nil "--composed1-route-shuffle" "Enable the Composed-1 M5b route-shuffle nemesis."
    :default false]
   [nil "--route-shuffle-interval SECONDS" "Seconds between route-shuffle nemesis operations."
    :default 30
    :parse-fn #(Double/parseDouble %)]
   [nil "--raft-snapshot-count N" "FSM snapshot threshold for transport soak runs."
    :default nil
    :parse-fn #(Long/parseLong %)]
   [nil "--raft-dispatcher-lanes" "Enable the four-lane Raft dispatcher during the soak."
    :default false]])

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
      :redis-port  (:redis-port options)
      :server-env  (cond-> {"ELASTICKV_RAFT_SEND_STREAM" "true"}
                     (:raft-snapshot-count options)
                     (assoc "ELASTICKV_RAFT_SNAPSHOT_COUNT" (str (:raft-snapshot-count options)))
                     (:raft-dispatcher-lanes options)
                     (assoc "ELASTICKV_RAFT_DISPATCHER_LANES" "true")))))

(defn -main
  [& args]
  (cli/run-workload! args
                     (into cli/common-cli-opts dynamo-cli-opts)
                     prepare-dynamo-opts
                     elastickv-dynamodb-multi-table-test))
