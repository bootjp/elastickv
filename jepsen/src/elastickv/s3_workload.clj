(ns elastickv.s3-workload
  "Jepsen workload for elastickv's S3-compatible API.
   Uses a linearizable register model: each S3 object is an independent
   register.  Writes PUT a value as the object body; reads GET the object
   body.  The checker verifies that every read is consistent with a
   linearizable history of writes."
  (:gen-class)
  (:require [clojure.string :as str]
            [clj-http.client :as http]
            [clj-http.conn-mgr :as conn]
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
            [jepsen.os.debian :as debian])
  (:import [java.io StringReader]
           [java.util.concurrent Callable Executors Future TimeUnit]
           [javax.xml XMLConstants]
           [javax.xml.parsers DocumentBuilderFactory]
           [org.w3c.dom Element]
           [org.xml.sax InputSource]
           [org.xml.sax.helpers DefaultHandler]))

;; ---------------------------------------------------------------------------
;; Constants
;; ---------------------------------------------------------------------------

(def ^:private bucket-name "jepsen-test")
(def ^:private default-s3-port 9000)

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

(defn- xml-root
  "Parse S3 XML without DTDs, external resources, or user-defined entities."
  [body]
  (let [factory (doto (DocumentBuilderFactory/newInstance)
                  (.setNamespaceAware true)
                  (.setFeature XMLConstants/FEATURE_SECURE_PROCESSING true)
                  (.setFeature "http://apache.org/xml/features/disallow-doctype-decl" true)
                  (.setFeature "http://xml.org/sax/features/external-general-entities" false)
                  (.setFeature "http://xml.org/sax/features/external-parameter-entities" false)
                  (.setFeature "http://apache.org/xml/features/nonvalidating/load-external-dtd" false)
                  (.setAttribute XMLConstants/ACCESS_EXTERNAL_DTD "")
                  (.setAttribute XMLConstants/ACCESS_EXTERNAL_SCHEMA "")
                  (.setXIncludeAware false)
                  (.setExpandEntityReferences false))
        builder (.newDocumentBuilder factory)]
    (.setErrorHandler builder (proxy [DefaultHandler] []
                               (error [e] (throw e))
                               (fatalError [e] (throw e))))
    (.getDocumentElement (.parse builder (InputSource. (StringReader. (or body "")))))))

(defn- xml-field
  [^Element root field]
  (let [nodes (.getElementsByTagNameNS root "*" field)]
    (when-not (= 1 (.getLength nodes))
      (throw (ex-info (str "Missing or duplicate S3 XML " field) {})))
    (let [^Element node (.item nodes 0)
          value (.getTextContent node)]
      (when-not (and (= root (.getParentNode node))
                     (zero? (.getLength (.getElementsByTagNameNS node "*" "*")))
                     (not (str/blank? value)))
        (throw (ex-info (str "Malformed S3 XML " field) {})))
      value)))

(defn- success!
  [response]
  (when-not (<= 200 (:status response) 299)
    (throw (ex-info (str "S3 multipart HTTP " (:status response)) response)))
  response)

(defn- result-root!
  [response expected]
  (success! response)
  (let [root (xml-root (:body response))]
    (when-not (and (= expected (.getLocalName root))
                   (zero? (.getLength (.getElementsByTagNameNS root "*" "Error"))))
      (throw (ex-info "S3 multipart error or unexpected XML result" response)))
    root))

(defn- part-etag!
  [response]
  (success! response)
  (let [etag (get-in response [:headers "etag"])]
    (when-not (and (string? etag) (re-matches #"\"[0-9a-fA-F]{32}\"" etag))
      (throw (ex-info "Missing or malformed UploadPart ETag" response)))
    etag))

(def ^:private multipart-race-timeout-ms 20000)

(defn- race-requests!
  "Join both bounded HTTP calls, closing their sockets before joining workers
   on every exit path. No shared future executor survives this write."
  [manager requests]
  (let [executor (Executors/newFixedThreadPool 2)
        tasks (atom [])
        deadline (+ (System/nanoTime) (* 1000000 multipart-race-timeout-ms))]
    (try
      (doseq [request requests]
        (swap! tasks conj
               (.submit executor ^Callable
                        (bound-fn []
                          (try (request)
                               (catch Exception e {:exception e}))))))
      (mapv (fn [^Future task]
              (.get task (max 1 (- deadline (System/nanoTime))) TimeUnit/NANOSECONDS))
            @tasks)
      (finally
        (doseq [^Future task @tasks] (.cancel task true))
        (conn/shutdown-manager manager)
        (.shutdownNow executor)
        (when-not (.awaitTermination executor 5 TimeUnit/SECONDS)
          (throw (ex-info "Multipart HTTP workers did not terminate" {})))))))

(defn- response!
  [response]
  (if-let [e (:exception response)] (throw e) response))

(defn- no-such-upload?
  [response]
  (and (= 404 (:status response))
       (let [root (xml-root (:body response))]
         (and (= "Error" (.getLocalName root))
              (= "NoSuchUpload" (xml-field root "Code"))))))

(defn- s3-multipart-put!
  [base-url k v]
  (let [manager (conn/make-reusable-conn-manager {:threads 2 :default-per-route 2})
        url (object-url base-url k)
        opts {:connection-manager manager :throw-exceptions false :as :string
              :conn-timeout 5000 :connection-request-timeout 5000 :socket-timeout 10000
              :retry-handler (fn [_ _ _] false) :redirect-strategy :none}]
    (try
      (let [root (result-root! (http/post url (assoc opts :query-params {"uploads" ""}))
                               "InitiateMultipartUploadResult")
            id (xml-field root "UploadId")
            _ (when (or (> (count id) 1024) (re-find #"[\s\p{Cntrl}]" id))
                (throw (ex-info "Malformed S3 UploadId" {})))
            part-opts (assoc opts :query-params {"uploadId" id "partNumber" 1}
                                 :body (str v) :content-type "text/plain")
            etag (part-etag! (http/put url part-opts))
            ;; ETag is the server's quoted MD5, not a locally substituted hash.
            body (str "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"
                      etag "</ETag></Part></CompleteMultipartUpload>")
            [replacement completion]
            (race-requests! manager
                            [#(http/put url part-opts)
                             #(http/post url (assoc opts :query-params {"uploadId" id}
                                                         :body body :content-type "application/xml"))])
            completed (result-root! (response! completion) "CompleteMultipartUploadResult")
            completed-etag (xml-field completed "ETag")]
        (when-not (re-matches #"\"[0-9a-fA-F]{32}-1\"" completed-etag)
          (throw (ex-info "Malformed completion ETag" {})))
        ;; Upload deletion is expected only after a confirmed completion.
        (let [replacement (response! replacement)]
          (when-not (no-such-upload? replacement)
            (when-not (= etag (part-etag! replacement))
              (throw (ex-info "Same-body replacement changed ETag" {}))))))
      (finally (conn/shutdown-manager manager)))))

;; ---------------------------------------------------------------------------
;; Jepsen client
;; ---------------------------------------------------------------------------

(defrecord S3Client [node->port url]
  client/Client
  (open! [this test node]
    (let [port (get node->port node default-s3-port)
          host (or (:s3-host test) (name node))]
      (assoc this :url (s3-base-url host port))))

  (setup! [this _test]
    (create-bucket! url))

  (teardown! [_this _test])

  (close! [this _test]
    (assoc this :url nil))

  (invoke! [_this test op]
    (try
      (let [[k v] (:value op)
            ;; Both modes run on the same CI cluster; keep their registers disjoint.
            object-key (if (:multipart test) (str "multipart-" k) k)]
        (case (:f op)
          :write
          (do (if (:multipart test)
                (s3-multipart-put! url object-key v)
                (s3-put! url k v))
              (assoc op :type :ok))

          :read
          (let [val (s3-get url object-key)]
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

(defn multipart-final-read-ops
  [key-count]
  (map (fn [k] {:f :read :value (independent/tuple k nil) :final true})
       (range key-count)))

(defn multipart-final-reads-checker
  "Global acceptance gate: every configured key must finish its final read;
   empty histories and runs with no confirmed writes cannot pass."
  [key-count]
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [expected (set (range key-count))
            writes (filter #(and (= :write (:f %)) (= :ok (:type %))) history)
            reads (filter #(and (= :read (:f %)) (= :ok (:type %))) history)
            finals (filter #(and (:final %) (= :read (:f %))) history)
            invoked (filter #(= :invoke (:type %)) finals)
            completed (remove #(= :invoke (:type %)) finals)
            successful (filter #(= :ok (:type %)) completed)
            key-of #(first (:value %))
            written (set (map key-of writes))
            missing (remove #(contains? (set (map key-of successful)) %) expected)
            bad (filter #(or (not= :ok (:type %))
                             (not (or (integer? (second (:value %)))
                                      (and (nil? (second (:value %)))
                                           (not (contains? written (key-of %)))))))
                        completed)]
        {:valid? (boolean (and (seq expected) (seq writes) (seq reads)
                               (= expected (set (map key-of invoked)))
                               (= key-count (count invoked) (count completed))
                               (empty? missing) (empty? bad)))
         :completed-writes (count writes)
         :completed-reads (count reads)
         :missing-final-keys (vec (sort missing))
         :bad-final-reads (mapv #(into {} %) bad)}))))

(defn s3-register-checker
  "Builds the per-key S3 register checker. Knossos's default competition
   algorithm races its linear and WGL analyzers and returns the first verdict."
  []
  (checker/compose
    ;; Histories with indeterminate S3 writes can make either search strategy
    ;; pathological. Racing both keeps the full verdict while avoiding a
    ;; timeout when only one analyzer encounters a large search space.
    {:linear   (checker/linearizable {:model (model/register)})
     :timeline (timeline/html)}))

(defn s3-register-workload
  "Builds a linearizable-register workload targeting the S3 endpoint.
   Each independent key is an S3 object; the checker verifies linearizability
   of the write/read history per key.

   threads-per-key controls how many concurrent threads hammer each key
   simultaneously. The test-level :concurrency divided by threads-per-key
   determines how many keys are active at once (e.g. concurrency=10,
   threads-per-key=2 → 5 keys active, each with 2 concurrent threads)."
  [opts]
  (let [key-count       (or (:key-count opts) 10)
        max-writes      (or (:max-writes-per-key opts) 100)
        threads-per-key (or (:threads-per-key opts) 2)
        client          (->S3Client (or (:node->port opts)
                                        (zipmap default-nodes (repeat default-s3-port)))
                                    nil)]
    {:client    client
     :generator (independent/concurrent-generator
                  threads-per-key
                  (range key-count)
                  (fn [_k]
                    (->> (gen/mix [(map (fn [v] {:f :write :value v}) (range))
                                  (gen/repeat {:f :read})])
                         (gen/limit max-writes))))
     :checker   (let [register (independent/checker (s3-register-checker))]
                  (if (:multipart opts)
                    (checker/compose {:register register
                                      :complete-final-reads (multipart-final-reads-checker key-count)})
                    register))}))

(defn elastickv-s3-test
  "Builds a Jepsen test map that drives elastickv's S3-compatible API
   with the linearizable register workload."
  ([] (elastickv-s3-test {}))
  ([opts]
   (let [nodes      (or (:nodes opts) default-nodes)
         s3-ports   (or (:s3-ports opts) (repeat (count nodes) (or (:s3-port opts) default-s3-port)))
         node->port (or (:node->port opts) (cli/ports->node-map s3-ports nodes))
         local?     (:local opts)
         db         (if local?
                      jdb/noop
                      (ekdb/db {:grpc-port    (or (:grpc-port opts) 50051)
                                :redis-port   (or (:redis-port opts) 6379)
                                :s3-port      node->port
                                :raft-groups  (:raft-groups opts)
                                :shard-ranges (:shard-ranges opts)
                                :encryption   (:encryption opts)}))
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
         workload   (s3-register-workload (assoc opts :node->port node->port))
         main-gen   (->> (:generator workload)
                         (gen/nemesis nemesis-gen)
                         (gen/stagger (/ rate))
                         (gen/time-limit time-limit))]
     (merge workload
            (when (:multipart opts) {:multipart true})
            {:name            (or (:name opts) (if (:multipart opts)
                                                "elastickv-s3-multipart-register"
                                                "elastickv-s3-register"))
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
             :concurrency     (or (:concurrency opts) 10)
             :generator       (if (:multipart opts)
                                (gen/phases main-gen
                                            (when nemesis-p
                                              (gen/phases
                                                (gen/nemesis (:final-generator nemesis-p))
                                                (gen/sleep 10)))
                                            (gen/clients (multipart-final-read-ops (or (:key-count opts) 10))))
                                main-gen)}))))

;; ---------------------------------------------------------------------------
;; CLI
;; ---------------------------------------------------------------------------

(def s3-cli-opts
  "S3-specific CLI options, appended to common opts."
  [[nil "--multipart" "Race same-body UploadPart with CompleteMultipartUpload."
    :default false]
   [nil "--s3-ports PORTS" "Comma separated S3 ports (per node)."
    :default nil
    :parse-fn (fn [s]
                (->> (str/split s #",")
                     (remove str/blank?)
                     (mapv #(Integer/parseInt %))))]
   [nil "--s3-port PORT" "S3 port (applied to all nodes)."
    :default default-s3-port
    :parse-fn #(Integer/parseInt %)]
   [nil "--redis-port PORT" "Redis port."
    :default 6379
    :parse-fn #(Integer/parseInt %)]
   [nil "--threads-per-key N" "Concurrent threads per S3 object key."
    :default 2
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
      :s3-host         (:host options)
      :node->port      node->port
      :s3-port         (:s3-port options)
      :redis-port      (:redis-port options)
      :threads-per-key (:threads-per-key options))))

(defn -main
  [& args]
  (cli/run-workload! args
                     (into cli/common-cli-opts s3-cli-opts)
                     prepare-s3-opts
                     elastickv-s3-test))
