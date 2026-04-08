(ns elastickv.s3-workload
  "Jepsen workload for elastickv's S3-compatible API.
   Uses a linearizable register model: each S3 object is an independent
   register.  Writes PUT a value as the object body; reads GET the object
   body.  The checker verifies that every read is consistent with a
   linearizable history of writes."
  (:gen-class)
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [warn]]
            [clj-http.client :as http]
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
            [knossos.model :as model]
            [jepsen.nemesis.combined :as combined]
            [jepsen.os :as os]
            [jepsen.os.debian :as debian]))

;; ---------------------------------------------------------------------------
;; Constants
;; ---------------------------------------------------------------------------

(def ^:private bucket-name "jepsen-test")

;; ---------------------------------------------------------------------------
;; S3 HTTP helpers
;; ---------------------------------------------------------------------------

(defn- s3-base-url
  "Returns the base URL for the S3 endpoint on a given node and port."
  [node port]
  (str "http://" (name node) ":" port))

(defn- object-url
  "URL for an individual object in the test bucket."
  [base-url k]
  (str base-url "/" bucket-name "/key-" k))

(defn- create-bucket!
  "Create the test bucket. Ignores BucketAlreadyOwnedByYou (409)."
  [base-url]
  (let [resp (http/put (str base-url "/" bucket-name)
                       {:throw-exceptions false
                        :conn-timeout     5000
                        :socket-timeout   10000})]
    (when (and (>= (:status resp) 400)
               (not= 409 (:status resp)))
      (throw (ex-info (str "Failed to create bucket: HTTP " (:status resp))
                      {:status (:status resp) :body (:body resp)})))))

(defn- s3-put!
  "PUT an object with the given integer value as the body. Returns nil."
  [base-url k v]
  (http/put (object-url base-url k)
            {:body              (str v)
             :content-type      "text/plain"
             :throw-exceptions  true
             :conn-timeout      5000
             :socket-timeout    10000})
  nil)

(defn- s3-get
  "GET an object and parse the body as a long. Returns nil when the object
   does not exist (404). Throws on unexpected status codes."
  [base-url k]
  (let [resp (http/get (object-url base-url k)
                       {:throw-exceptions false
                        :as               :string
                        :conn-timeout     5000
                        :socket-timeout   10000})]
    (case (int (:status resp))
      200 (Long/parseLong (str/trim (:body resp)))
      404 nil
      (throw (ex-info (str "S3 GET error: HTTP " (:status resp))
                      {:status (:status resp) :body (:body resp)})))))

(defn- s3-delete!
  "DELETE an object. Returns nil."
  [base-url k]
  (http/delete (object-url base-url k)
               {:throw-exceptions false
                :conn-timeout     5000
                :socket-timeout   10000})
  nil)

(defn- s3-list
  "List object keys in the test bucket. Returns a set of key strings."
  [base-url]
  (let [resp (http/get (str base-url "/" bucket-name "?list-type=2&max-keys=1000")
                       {:throw-exceptions false
                        :as               :string
                        :conn-timeout     5000
                        :socket-timeout   10000})]
    (when (= 200 (:status resp))
      (->> (re-seq #"<Key>([^<]+)</Key>" (:body resp))
           (map second)
           set))))

;; ---------------------------------------------------------------------------
;; Jepsen client
;; ---------------------------------------------------------------------------

(defrecord S3Client [node->port url]
  client/Client
  (open! [this test node]
    (let [port (get node->port node 9000)
          host (or (:s3-host test) (name node))]
      (assoc this :url (s3-base-url host port))))

  (setup! [this _test]
    (create-bucket! url))

  (teardown! [_this _test])

  (close! [this _test]
    (assoc this :url nil))

  (invoke! [_this _test op]
    (try
      (let [[k v] (:value op)]
        (case (:f op)
          :write
          (do (s3-put! url k v)
              (assoc op :type :ok))

          :read
          (let [val (s3-get url k)]
            (assoc op :type :ok :value (independent/tuple k val)))))
      (catch java.net.ConnectException _
        (assoc op :type :info :error :connection-refused))
      (catch java.net.SocketTimeoutException _
        (assoc op :type :info :error :socket-timeout))
      (catch java.net.SocketException e
        (assoc op :type :info :error (str "socket: " (.getMessage e))))
      (catch org.apache.http.NoHttpResponseException _
        (assoc op :type :info :error :no-http-response))
      (catch clojure.lang.ExceptionInfo e
        (let [data (ex-data e)]
          (if (and (:status data) (>= (int (:status data)) 500))
            (assoc op :type :info :error (str "HTTP " (:status data)))
            (assoc op :type :info :error (.getMessage e)))))
      (catch Exception e
        (assoc op :type :info :error (.getMessage e))))))

;; ---------------------------------------------------------------------------
;; Workload & Test builders
;; ---------------------------------------------------------------------------

(def default-nodes ["n1" "n2" "n3" "n4" "n5"])

(defn s3-register-workload
  "Builds a linearizable-register workload targeting the S3 endpoint.
   Each independent key is an S3 object; the checker verifies linearizability
   of the write/read history per key."
  [opts]
  (let [key-count       (or (:key-count opts) 10)
        max-writes      (or (:max-writes-per-key opts) 100)
        client          (->S3Client (or (:node->port opts)
                                        (zipmap default-nodes (repeat 9000)))
                                    nil)]
    {:client    client
     :generator (independent/concurrent-generator
                  (or (:concurrency opts) 5)
                  (range key-count)
                  (fn [_k]
                    (->> (gen/mix [(map (fn [v] {:f :write :value v}) (range))
                                  (repeat {:f :read})])
                         (gen/limit max-writes))))
     :checker   (independent/checker
                  (checker/compose
                    {:linear   (checker/linearizable
                                 {:model     (model/register)
                                  :algorithm :linear})
                     :timeline (timeline/html)}))}))

(defn elastickv-s3-test
  "Builds a Jepsen test map that drives elastickv's S3-compatible API
   with the linearizable register workload."
  ([] (elastickv-s3-test {}))
  ([opts]
   (let [nodes      (or (:nodes opts) default-nodes)
         s3-ports   (or (:s3-ports opts) (repeat (count nodes) (or (:s3-port opts) 9000)))
         node->port (or (:node->port opts) (cli/ports->node-map s3-ports nodes))
         local?     (:local opts)
         db         (if local?
                      jdb/noop
                      (ekdb/db {:grpc-port    (or (:grpc-port opts) 50051)
                                :redis-port   (or (:redis-port opts) 6379)
                                :s3-port      node->port
                                :raft-groups  (:raft-groups opts)
                                :shard-ranges (:shard-ranges opts)}))
         rate       (double (or (:rate opts) 5))
         time-limit (or (:time-limit opts) 30)
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
         workload   (s3-register-workload (assoc opts :node->port node->port))]
     (merge workload
            {:name            (or (:name opts) "elastickv-s3-register")
             :nodes           nodes
             :db              db
             :s3-host         (:s3-host opts)
             :os              (if local? os/noop debian/os)
             :net             (if local? net/noop net/iptables)
             :ssh             (merge {:username                  "vagrant"
                                      :private-key-path          "/home/vagrant/.ssh/id_rsa"
                                      :strict-host-key-checking  false}
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

(def s3-cli-opts
  "S3-specific CLI options, appended to common opts."
  [[nil "--s3-ports PORTS" "Comma separated S3 ports (per node)."
    :default nil
    :parse-fn (fn [s]
                (->> (str/split s #",")
                     (remove str/blank?)
                     (mapv #(Integer/parseInt %))))]
   [nil "--s3-port PORT" "S3 port (applied to all nodes)."
    :default 9000
    :parse-fn #(Integer/parseInt %)]
   [nil "--redis-port PORT" "Redis port."
    :default 6379
    :parse-fn #(Integer/parseInt %)]])

(defn- prepare-s3-opts
  "Transform parsed CLI options into the map expected by elastickv-s3-test."
  [options]
  (let [s3-ports (:s3-ports options)
        options  (cli/parse-common-opts options s3-ports)
        node->port (if s3-ports
                     (cli/ports->node-map s3-ports (:nodes options))
                     (zipmap (:nodes options) (repeat (:s3-port options))))]
    (assoc options
      :s3-host    (:host options)
      :node->port node->port
      :s3-port    (:s3-port options)
      :redis-port (:redis-port options))))

(defn -main
  [& args]
  (cli/run-workload! args
                     (into cli/common-cli-opts s3-cli-opts)
                     prepare-s3-opts
                     elastickv-s3-test))
