(ns elastickv.dynamodb-workload
  "Jepsen workload for elastickv's DynamoDB-compatible API.
   Uses the list-append consistency model: each key maps to a DynamoDB item
   whose 'val' attribute is a list of integers. Writes append to the list via
   UpdateItem; reads fetch the list via GetItem."
  (:gen-class)
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [warn]]
            [cheshire.core :as json]
            [clj-http.client :as http]
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
(def ^:private pk-attr "pk")
(def ^:private val-attr "val")

(defn- dynamo-url
  "Returns the base URL for the DynamoDB endpoint on a given node and port."
  [node port]
  (str "http://" (name node) ":" port))

(defn- dynamo-request
  "Send a DynamoDB JSON request. Returns the parsed response body or throws."
  [url target body]
  (let [resp (http/post url
               {:headers      {"X-Amz-Target" target
                               "Content-Type" "application/x-amz-json-1.0"}
                :body         (json/generate-string body)
                :as           :string
                :throw-exceptions false
                :conn-timeout 5000
                :socket-timeout 10000})]
    (if (< (:status resp) 400)
      (when (and (:body resp) (not (str/blank? (:body resp))))
        (json/parse-string (:body resp) true))
      (let [parsed (when (and (:body resp) (not (str/blank? (:body resp))))
                     (try (json/parse-string (:body resp) true)
                          (catch Exception _ nil)))
            err-type (get parsed :__type "UnknownError")
            message  (get parsed :message (or (:body resp) ""))]
        (throw (ex-info (str "DynamoDB error: " err-type ": " message)
                        {:type err-type :status (:status resp) :body parsed}))))))

(defn- create-table!
  "Create the jepsen_append table if it doesn't exist."
  [url]
  (try
    (dynamo-request url "DynamoDB_20120810.CreateTable"
                    {:TableName             table-name
                     :KeySchema             [{:AttributeName pk-attr :KeyType "HASH"}]
                     :AttributeDefinitions  [{:AttributeName pk-attr :AttributeType "S"}]
                     :ProvisionedThroughput {:ReadCapacityUnits 5 :WriteCapacityUnits 5}})
    (catch clojure.lang.ExceptionInfo e
      (when-not (= "ResourceInUseException" (:type (ex-data e)))
        (throw e)))))

(defn- dynamo-append!
  "Append a value to the list for the given key. Returns nil."
  [url k v]
  (dynamo-request url "DynamoDB_20120810.UpdateItem"
                  {:TableName                 table-name
                   :Key                       {pk-attr {:S (str k)}}
                   :UpdateExpression          "SET #v = list_append(if_not_exists(#v, :empty), :val)"
                   :ExpressionAttributeNames  {"#v" val-attr}
                   :ExpressionAttributeValues {":empty" {:L []}
                                               ":val"   {:L [{:N (str v)}]}}})
  nil)

(defn- dynamo-read
  "Read the list for the given key. Returns a vector of longs, or nil if the
   item doesn't exist."
  [url k]
  (let [resp (dynamo-request url "DynamoDB_20120810.GetItem"
                             {:TableName      table-name
                              :Key            {pk-attr {:S (str k)}}
                              :ConsistentRead true})]
    (when-let [item (:Item resp)]
      (let [list-val (get-in item [(keyword val-attr) :L])]
        (when list-val
          (mapv (fn [elem] (Long/parseLong (:N elem))) list-val))))))

(defrecord DynamoDBClient [node->port url]
  client/Client
  (open! [this test node]
    (let [port (get node->port node 8000)
          host (or (:dynamo-host test) (name node))]
      (assoc this :url (dynamo-url host port))))

  (setup! [this _test]
    (create-table! url))

  (teardown! [_this _test])

  (close! [this _test]
    (assoc this :url nil))

  (invoke! [_this _test op]
    (try
      (case (:f op)
        :txn
        (let [value' (mapv (fn [[f k v :as mop]]
                             (case f
                               :append (do (dynamo-append! url k v)
                                           mop)
                               :r      [f k (dynamo-read url k)]))
                           (:value op))]
          (assoc op :type :ok :value value')))
      (catch clojure.lang.ExceptionInfo e
        (let [data (ex-data e)
              err-type (:type data)]
          (cond
            ;; Condition check failures or internal retryable errors → :info
            (contains? #{"ConditionalCheckFailedException"
                         "InternalServerError"
                         "TransactionCanceledException"} err-type)
            (assoc op :type :info :error (str err-type))

            ;; Validation errors are definite failures
            (= "ValidationException" err-type)
            (assoc op :type :fail :error (str err-type ": " (get-in data [:body :message] "")))

            :else
            (assoc op :type :info :error (.getMessage e)))))
      (catch java.net.ConnectException _
        (assoc op :type :info :error :connection-refused))
      (catch java.net.SocketTimeoutException _
        (assoc op :type :info :error :socket-timeout))
      (catch java.net.SocketException e
        (assoc op :type :info :error (str "socket: " (.getMessage e))))
      (catch Exception e
        (assoc op :type :info :error (.getMessage e))))))

;; ---------------------------------------------------------------------------
;; Workload & Test builders
;; ---------------------------------------------------------------------------

(def default-nodes ["n1" "n2" "n3" "n4" "n5"])

(defn dynamodb-append-workload
  "Builds the list-append workload map targeting the DynamoDB endpoint.
   max-txn-length defaults to 1 because each micro-op (append/read) is sent
   as an independent HTTP UpdateItem/GetItem request without DynamoDB
   TransactWriteItems.  Multi-op transactions would be interleaved, producing
   false G0 (write cycle) anomalies."
  [opts]
  (let [workload (append/test {:key-count            (or (:key-count opts) 12)
                               :min-txn-length       1
                               :max-txn-length       (or (:max-txn-length opts) 1)
                               :max-writes-per-key   (or (:max-writes-per-key opts) 128)
                               :consistency-models   [:strict-serializable]})
        client   (->DynamoDBClient (or (:node->port opts)
                                       (zipmap default-nodes (repeat 8000)))
                                   nil)]
    (assoc workload :client client)))

(defn elastickv-dynamodb-test
  "Builds a Jepsen test map that drives elastickv's DynamoDB-compatible API
   with the list-append workload."
  ([] (elastickv-dynamodb-test {}))
  ([opts]
   (let [nodes       (or (:nodes opts) default-nodes)
         dynamo-ports (or (:dynamo-ports opts) (repeat (count nodes) (or (:dynamo-port opts) 8000)))
         node->port  (or (:node->port opts) (cli/ports->node-map dynamo-ports nodes))
         local?      (:local opts)
         db          (if local?
                       jdb/noop
                       (ekdb/db {:grpc-port   (or (:grpc-port opts) 50051)
                                 :redis-port  (or (:redis-port opts) 6379)
                                 :dynamo-port node->port
                                 :raft-groups  (:raft-groups opts)
                                 :shard-ranges (:shard-ranges opts)}))
         rate        (double (or (:rate opts) 5))
         time-limit  (or (:time-limit opts) 30)
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
         workload    (dynamodb-append-workload (assoc opts :node->port node->port))]
     (merge workload
            {:name            (or (:name opts) "elastickv-dynamodb-append")
             :nodes           nodes
             :db              db
             :dynamo-host     (:dynamo-host opts)
             :os              (if local? os/noop debian/os)
             :net             (if local? net/noop net/iptables)
             :ssh             (merge {:username              "vagrant"
                                      :private-key-path      "/home/vagrant/.ssh/id_rsa"
                                      :strict-host-key-checking false}
                                     (when local? {:dummy true})
                                     (:ssh opts))
             :remote          control/ssh
             :nemesis         (if nemesis-p
                                (:nemesis nemesis-p)
                                nemesis/noop)
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
  [[nil "--dynamo-ports PORTS" "Comma separated DynamoDB ports (per node)."
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
    :parse-fn #(Integer/parseInt %)]])

(defn- prepare-dynamo-opts
  "Transform parsed CLI options into the map expected by elastickv-dynamodb-test."
  [options]
  (let [dynamo-ports (:dynamo-ports options)
        options (cli/parse-common-opts options dynamo-ports)
        node->port (if dynamo-ports
                     (cli/ports->node-map dynamo-ports (:nodes options))
                     (zipmap (:nodes options) (repeat (:dynamo-port options))))]
    (assoc options
      :dynamo-host (:host options)
      :node->port node->port
      :dynamo-port (:dynamo-port options)
      :redis-port (:redis-port options))))

(defn -main
  [& args]
  (cli/run-workload! args
                     (into cli/common-cli-opts dynamo-cli-opts)
                     prepare-dynamo-opts
                     elastickv-dynamodb-test))
