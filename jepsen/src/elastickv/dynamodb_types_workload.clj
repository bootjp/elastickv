(ns elastickv.dynamodb-types-workload
  "Jepsen workload that exercises every DynamoDB attribute type elastickv
   currently supports.

   The existing dynamodb-workload covers only the List type via the
   list-append (cycle/elle) consistency model.  This workload complements it
   with a per-type linearizable-register check for the remaining nine types:
   String (S), Number (N), Binary (B), Boolean (BOOL), Null (NULL), String
   Set (SS), Number Set (NS), Binary Set (BS), List (L) and Map (M).

   Each key is an independent register stored in its own DynamoDB item.
   Writes use PutItem (replacing the entire `val` attribute).  Reads use
   GetItem with ConsistentRead=true.  Test values are derived from the
   write index, so each write produces a distinct value the register
   model can disambiguate.

   A single test run targets one type (selected via --value-type).  Each
   type uses its own table so concurrent or sequential runs do not
   interfere."
  (:gen-class)
  (:require [clojure.string :as str]
            [cognitect.aws.client.api :as aws]
            [cognitect.aws.credentials :as creds]
            [elastickv.cli :as cli]
            [elastickv.db :as ekdb]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [independent :as independent]
                    [net :as net]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control :as control]
            [jepsen.db :as jdb]
            [jepsen.nemesis :as nemesis]
            [jepsen.nemesis.combined :as combined]
            [jepsen.os :as os]
            [jepsen.os.debian :as debian]
            [knossos.model :as model]))

(def ^:private pk-attr  "pk")
(def ^:private val-attr "val")

;; ---------------------------------------------------------------------------
;; Per-type specifications
;; ---------------------------------------------------------------------------
;;
;; Every spec defines:
;;   :table    DynamoDB table name (one per type, for isolation)
;;   :gen      (fn [i]) -> Clojure value used by the register model
;;   :encode   (fn [v]) -> DynamoDB attribute map (e.g. {:S "x"})
;;   :decode   (fn [a]) -> Clojure value (canonicalised — sets sorted, byte
;;             arrays converted to vectors of int) so equality is reliable
;;             across the Knossos register check.
;;
;; The encoder always builds a *single* DynamoDB attribute matching the type
;; under test, so each test exercises exactly one attribute kind.

(defn- bytes-of ^bytes [^String s]
  (.getBytes s "UTF-8"))

(defn- ->byte-array
  "Coerce a value returned by cognitect/aws-api for a Binary attribute into a
   Java byte[].  Different SDK versions return either byte[] or ByteBuffer,
   so we accept both (and pass through nil for missing attributes)."
  ^bytes [b]
  (cond
    (nil? b)                            nil
    (instance? java.nio.ByteBuffer b)   (let [^java.nio.ByteBuffer buf (.duplicate ^java.nio.ByteBuffer b)
                                              arr (byte-array (.remaining buf))]
                                          (.get buf arr)
                                          arr)
    :else                               b))

(defn- bytes->vec
  "Canonicalise binary data to a vector of unsigned ints so that equality
   works inside the Knossos register model (byte[] uses identity equality)."
  [b]
  (when-let [arr (->byte-array b)]
    (vec (map #(bit-and 0xff (int %)) arr))))

(def ^:private type-specs
  {:string  {:table  "jepsen_types_string"
             :gen    (fn [i] (str "v-" i))
             :encode (fn [v] {:S v})
             :decode (fn [a] (:S a))}

   :number  {:table  "jepsen_types_number"
             :gen    (fn [i] (long i))
             :encode (fn [v] {:N (str v)})
             :decode (fn [a] (Long/parseLong (:N a)))}

   :binary  {:table  "jepsen_types_binary"
             :gen    (fn [i] (bytes->vec (bytes-of (str "v-" i))))
             :encode (fn [v] {:B (byte-array (map unchecked-byte v))})
             :decode (fn [a] (bytes->vec (:B a)))}

   :bool    {:table  "jepsen_types_bool"
             :gen    (fn [i] (odd? i))
             :encode (fn [v] {:BOOL v})
             ;; Return (:BOOL a) directly, not (boolean ...): coercing to
             ;; boolean would turn a nil payload (missing or wrong-typed
             ;; attribute) into false, which is indistinguishable from a
             ;; legitimately-written false.  Preserving nil lets the
             ;; register checker surface shape/type regressions.
             :decode (fn [a] (:BOOL a))}

   ;; NULL has only one valid value.  The register check still verifies that
   ;; reads observe the written attribute (and never an absent / wrong-typed
   ;; one) under nemesis.
   :null    {:table  "jepsen_types_null"
             :gen    (fn [_i] :null)
             :encode (fn [_v] {:NULL true})
             :decode (fn [a] (when (:NULL a) :null))}

   :string-set
            {:table  "jepsen_types_string_set"
             :gen    (fn [i] (sort [(str "v-" i) (str "w-" i)]))
             :encode (fn [v] {:SS (vec v)})
             :decode (fn [a] (some-> (:SS a) sort vec))}

   :number-set
            {:table  "jepsen_types_number_set"
             :gen    (fn [i] (sort [(long i) (long (+ i 1000))]))
             :encode (fn [v] {:NS (mapv str v)})
             :decode (fn [a] (some->> (:NS a) (map #(Long/parseLong %)) sort vec))}

   :binary-set
            {:table  "jepsen_types_binary_set"
             :gen    (fn [i] (sort [(bytes->vec (bytes-of (str "v-" i)))
                                    (bytes->vec (bytes-of (str "w-" i)))]))
             :encode (fn [v] {:BS (mapv #(byte-array (map unchecked-byte %)) v)})
             :decode (fn [a] (some->> (:BS a) (map bytes->vec) sort vec))}

   :list    {:table  "jepsen_types_list"
             :gen    (fn [i] [(long i) (long (+ i 1))])
             :encode (fn [v] {:L (mapv (fn [n] {:N (str n)}) v)})
             :decode (fn [a] (some->> (:L a) (mapv #(Long/parseLong (:N %)))))}

   :map     {:table  "jepsen_types_map"
             :gen    (fn [i] {"a" (long i) "b" (long (+ i 1))})
             :encode (fn [v] {:M (into {} (map (fn [[k n]] [k {:N (str n)}]) v))})
             :decode (fn [a] (when-let [m (:M a)]
                               (into {} (map (fn [[k av]]
                                               [(name k) (Long/parseLong (:N av))])
                                             m))))}})

(def value-type-keys
  "All registered DynamoDB type keys, in display/run order."
  [:string :number :binary :bool :null
   :string-set :number-set :binary-set
   :list :map])

;; ---------------------------------------------------------------------------
;; AWS client
;; ---------------------------------------------------------------------------

(defn- make-ddb-client
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

(defn- anomaly? [resp]
  (contains? resp :cognitect.anomalies/category))

(defn- ddb-invoke!
  [ddb op request]
  (let [resp (aws/invoke ddb {:op op :request request})]
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

;; Default provisioned throughput for test tables.  elastickv does not
;; enforce these numbers today, but we pick something high enough that a
;; real DynamoDB endpoint would not throttle a stress run (which would
;; otherwise manifest as ProvisionedThroughputExceededException :info
;; operations and waste test time).  Overridable via --read-capacity /
;; --write-capacity.
(def ^:private default-read-capacity  100)
(def ^:private default-write-capacity 100)

(defn- create-table!
  "Create a table for the type under test; ignore ResourceInUseException."
  [ddb table read-capacity write-capacity]
  (try
    (ddb-invoke! ddb :CreateTable
                 {:TableName             table
                  :KeySchema             [{:AttributeName pk-attr :KeyType "HASH"}]
                  :AttributeDefinitions  [{:AttributeName pk-attr :AttributeType "S"}]
                  :ProvisionedThroughput {:ReadCapacityUnits  read-capacity
                                          :WriteCapacityUnits write-capacity}})
    (catch clojure.lang.ExceptionInfo e
      (when-not (= "ResourceInUseException" (:type (ex-data e)))
        (throw e)))))

(defn- dynamo-put!
  "PutItem with the encoded value.  Replaces the entire item."
  [ddb table k attr]
  (ddb-invoke! ddb :PutItem
               {:TableName table
                :Item      {pk-attr {:S (str k)}
                            val-attr attr}})
  nil)

(defn- dynamo-get
  "ConsistentRead GetItem; returns the raw attribute map at val, or nil."
  [ddb table k]
  (let [resp (ddb-invoke! ddb :GetItem
                          {:TableName      table
                           :Key            {pk-attr {:S (str k)}}
                           :ConsistentRead true})]
    (get-in resp [:Item (keyword val-attr)])))

;; ---------------------------------------------------------------------------
;; Jepsen client
;; ---------------------------------------------------------------------------

(defrecord DynamoDBTypesClient [node->port spec read-capacity write-capacity ddb]
  client/Client

  (open! [this test node]
    (let [port (get node->port node 8000)
          host (or (:dynamo-host test) (name node))]
      (assoc this :ddb (make-ddb-client host port))))

  (setup! [_this _test]
    (create-table! ddb (:table spec) read-capacity write-capacity))

  (teardown! [_this _test])

  (close! [this _test]
    (when ddb (aws/stop ddb))
    (assoc this :ddb nil))

  (invoke! [_this _test op]
    (try
      (let [[k v] (:value op)
            table (:table spec)]
        (case (:f op)
          :write
          (do (dynamo-put! ddb table k ((:encode spec) v))
              (assoc op :type :ok))

          :read
          (let [attr (dynamo-get ddb table k)
                decoded (when attr ((:decode spec) attr))]
            (assoc op :type :ok :value (independent/tuple k decoded)))))

      (catch clojure.lang.ExceptionInfo e
        (let [data     (ex-data e)
              err-type (:type data)
              category (:category data)]
          (cond
            (and (nil? err-type)
                 (#{:cognitect.anomalies/fault
                    :cognitect.anomalies/unavailable} category))
            (assoc op :type :info :error :network-error)

            ;; Transient server-side errors that may or may not have been
            ;; applied.  Mark them :info so Jepsen treats them as
            ;; indeterminate rather than a definite failure.
            (contains? #{"InternalServerError"
                         "ServiceUnavailableException"
                         "ThrottlingException"
                         "ProvisionedThroughputExceededException"}
                       err-type)
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

(defn- resolve-spec
  "Lookup the spec for the requested :value-type, defaulting to :string."
  [opts]
  (let [vt (or (:value-type opts) :string)]
    (or (get type-specs vt)
        (throw (ex-info (str "Unknown value-type " vt
                             "; valid: " (vec (keys type-specs)))
                        {:value-type vt})))))

(defn dynamodb-types-workload
  "Builds the linearizable-register workload for one DynamoDB attribute type."
  [opts]
  (let [spec            (resolve-spec opts)
        gen-fn          (:gen spec)
        key-count       (or (:key-count opts) 5)
        max-writes      (or (:max-writes-per-key opts) 50)
        threads-per-key (or (:threads-per-key opts) 2)
        read-capacity   (or (:read-capacity opts)  default-read-capacity)
        write-capacity  (or (:write-capacity opts) default-write-capacity)
        client          (->DynamoDBTypesClient
                          (or (:node->port opts)
                              (zipmap default-nodes (repeat 8000)))
                          spec
                          read-capacity
                          write-capacity
                          nil)]
    {:client    client
     :generator (independent/concurrent-generator
                  threads-per-key
                  (range key-count)
                  (fn [_k]
                    (->> (gen/mix [(map (fn [i] {:f :write :value (gen-fn i)})
                                        (range))
                                   (gen/repeat {:f :read})])
                         (gen/limit max-writes))))
     :checker   (independent/checker
                  (checker/compose
                    {:linear   (checker/linearizable
                                 {:model     (model/register)
                                  :algorithm :linear})
                     :timeline (timeline/html)}))}))

(defn elastickv-dynamodb-types-test
  "Builds a Jepsen test map for a single DynamoDB attribute type."
  ([] (elastickv-dynamodb-types-test {}))
  ([opts]
   (let [value-type   (or (:value-type opts) :string)
         nodes        (or (:nodes opts) default-nodes)
         dynamo-ports (or (:dynamo-ports opts)
                          (repeat (count nodes) (or (:dynamo-port opts) 8000)))
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
         workload     (dynamodb-types-workload (assoc opts :node->port node->port))]
     (merge workload
            {:name            (or (:name opts)
                                  (str "elastickv-dynamodb-type-" (name value-type)))
             :nodes           nodes
             :db              db
             :dynamo-host     (:dynamo-host opts)
             :os              (if local? os/noop debian/os)
             :net             (if local? net/noop net/iptables)
             :ssh             (merge {:username                 "vagrant"
                                      :private-key-path         "/home/vagrant/.ssh/id_rsa"
                                      :strict-host-key-checking false}
                                     (when local? {:dummy true})
                                     (:ssh opts))
             :remote          control/ssh
             :nemesis         (if nemesis-p (:nemesis nemesis-p) nemesis/noop)
             :final-generator nil
             :concurrency     (or (:concurrency opts) 10)
             :generator       (->> (:generator workload)
                                   (gen/nemesis nemesis-gen)
                                   (gen/stagger (/ rate))
                                   (gen/time-limit time-limit))}))))

;; ---------------------------------------------------------------------------
;; CLI
;; ---------------------------------------------------------------------------

(defn- parse-value-type [s]
  (let [k (keyword s)]
    (when-not (contains? type-specs k)
      (throw (IllegalArgumentException.
               (str "Unknown --value-type " s "; valid: "
                    (str/join "," (map name value-type-keys))))))
    k))

(def types-cli-opts
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
   [nil "--value-type TYPE" "DynamoDB attribute type to test (string,number,binary,bool,null,string-set,number-set,binary-set,list,map)."
    :default :string
    :parse-fn parse-value-type]
   [nil "--threads-per-key N" "Concurrent threads per register key."
    :default 2
    :parse-fn #(Integer/parseInt %)]
   [nil "--read-capacity N" "ProvisionedThroughput.ReadCapacityUnits for the test table."
    :default default-read-capacity
    :parse-fn #(Integer/parseInt %)]
   [nil "--write-capacity N" "ProvisionedThroughput.WriteCapacityUnits for the test table."
    :default default-write-capacity
    :parse-fn #(Integer/parseInt %)]])

(defn- prepare-types-opts
  [options]
  (let [dynamo-ports (:dynamo-ports options)
        options      (cli/parse-common-opts options dynamo-ports)
        node->port   (if dynamo-ports
                       (cli/ports->node-map dynamo-ports (:nodes options))
                       (zipmap (:nodes options) (repeat (:dynamo-port options))))]
    (assoc options
      :dynamo-host    (:host options)
      :node->port     node->port
      :dynamo-port    (:dynamo-port options)
      :redis-port     (:redis-port options)
      :read-capacity  (:read-capacity options)
      :write-capacity (:write-capacity options))))

(defn -main
  [& args]
  (cli/run-workload! args
                     (into cli/common-cli-opts types-cli-opts)
                     prepare-types-opts
                     elastickv-dynamodb-types-test))
