(ns elastickv.db
  "Jepsen DB adapter that builds, deploys, and manages elastickv nodes."
  (:require [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [db :as db]
                    [util :as util]]
            [jepsen.control :as c]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))

(def ^:private bin-dir "/opt/elastickv/bin")
(def ^:private data-dir "/var/lib/elastickv")
(def ^:private log-file "/var/log/elastickv.log")
(def ^:private transport-metrics-file "/var/log/elastickv-transport-metrics.prom")
(def ^:private pid-file "/var/run/elastickv.pid")
(def ^:private server-bin (str bin-dir "/elastickv"))
(def ^:private raftadmin-bin (str bin-dir "/raftadmin"))
(def ^:private split-bin (str bin-dir "/elastickv-split"))
(def ^:private list-routes-bin (str bin-dir "/elastickv-list-routes"))

(def ^:private build-dir
  ;; local (control node) directory for built binaries
  (str (System/getProperty "user.dir") "/target/elastickv-jepsen"))

(defn- ensure-build-dir! []
  (doto (io/file build-dir)
    (.mkdirs)))

(defn- build-binaries!
  "Build elastickv server and the repo-owned raftadmin helper on the control node."
  []
  (ensure-build-dir!)
  (let [root (-> (io/file "..") .getCanonicalPath)
        go-arch (clojure.string/trim (:out (sh/sh "go" "env" "GOARCH")))
        env  (merge (into {} (System/getenv))
                    {"GOOS" "linux"
                     "GOARCH" go-arch
                     "CGO_ENABLED" "0"
                     "GOPATH" "/home/vagrant/go"
                     "GOCACHE" "/home/vagrant/.cache/go-build"})]
    (doseq [[out-cmd args] [["elastickv" ["go" "build" "-o" (str build-dir "/elastickv") "./cmd/server"]]
                            ["raftadmin" ["go" "build" "-o" (str build-dir "/raftadmin") "./cmd/raftadmin"]]
                            ["elastickv-split" ["go" "build" "-o" (str build-dir "/elastickv-split") "./cmd/elastickv-split"]]
                            ["elastickv-list-routes" ["go" "build" "-o" (str build-dir "/elastickv-list-routes") "./cmd/elastickv-list-routes"]]]]
      (let [{:keys [exit err]} (apply sh/sh (concat args [:env env :dir root]))]
        (when-not (zero? exit)
          (throw (ex-info (str "failed to build " out-cmd) {:err err})))))))

(defonce ^:private built? (delay (build-binaries!)))

(defn- install-deps!
  "Install minimal packages on a node."
  [node]
  (c/on node
    (debian/install [:curl :netcat-openbsd :rsync :iptables :chrony :libfaketime])))

(defn- upload-binaries!
  "Copy built binaries to the given node."
  [test node]
  @built?
  (c/on node
    (c/su
      (c/exec :mkdir :-p bin-dir)
      (doseq [bin ["elastickv" "raftadmin" "elastickv-split" "elastickv-list-routes"]]
        (c/upload (str build-dir "/" bin) (str bin-dir "/" bin))
        (c/exec :chmod "755" (str bin-dir "/" bin))))))

(defn- node-addr
  "Returns host:port for the node and port."
  [node port]
  (str (name node) ":" port))

(defn- port-for [port-spec node]
  (if (map? port-spec)
    (get port-spec node)
    port-spec))

(defn- group-ids [raft-groups]
  (->> (keys raft-groups)
       (sort)))

(defn- group-addr [node raft-groups group-id]
  (node-addr node (port-for (get raft-groups group-id) node)))

(defn- build-raft-groups-arg [node raft-groups]
  (->> (group-ids raft-groups)
       (map (fn [gid]
              (str gid "=" (group-addr node raft-groups gid))))
       (clojure.string/join ",")))

(defn- build-raft-service-map [nodes grpc-port service-port raft-groups]
  (let [groups (when (seq raft-groups) (group-ids raft-groups))]
    (->> nodes
         (mapcat (fn [n]
                   (let [service-addr (node-addr n (port-for service-port n))]
                     (if (seq groups)
                       (map (fn [gid]
                              (str (group-addr n raft-groups gid) "=" service-addr))
                            groups)
                       [(str (node-addr n (port-for grpc-port n)) "=" service-addr)]))))
         (clojure.string/join ","))))

(defn- build-raft-redis-map [nodes grpc-port redis-port raft-groups]
  (build-raft-service-map nodes grpc-port redis-port raft-groups))

(defn- build-raft-dynamo-map [nodes grpc-port dynamo-port raft-groups]
  (build-raft-service-map nodes grpc-port dynamo-port raft-groups))

(defn- start-node!
  [test node {:keys [bootstrap-node grpc-port redis-port dynamo-port s3-port sqs-port sqs-region data-dir raft-groups shard-ranges raft-engine server-env migration-enabled]}]
  (when (and (seq raft-groups)
             (> (count raft-groups) 1)
             (nil? shard-ranges))
    (throw (ex-info "shard-ranges is required when raft-groups has multiple entries" {})))
  (let [grpc (if (seq raft-groups)
               (group-addr node raft-groups (first (group-ids raft-groups)))
               (node-addr node (port-for grpc-port node)))
        redis (node-addr node (port-for redis-port node))
        dynamo (when dynamo-port
                 (node-addr node (port-for dynamo-port node)))
        s3 (when s3-port
             (node-addr node (port-for s3-port node)))
        sqs (when sqs-port
              (node-addr node (port-for sqs-port node)))
        raft-redis-map (build-raft-redis-map (:nodes test) grpc-port redis-port raft-groups)
        raft-dynamo-map (when dynamo
                          (build-raft-dynamo-map (:nodes test) grpc-port dynamo-port raft-groups))
        bootstrap? (= node bootstrap-node)
        args (cond-> ["--address" grpc
                      "--redisAddress" redis
                      "--raftId" (name node)
                      "--raftDataDir" data-dir
                      "--raftEngine" (or raft-engine "etcd")
                      "--raftRedisMap" raft-redis-map]
               dynamo (conj "--dynamoAddress" dynamo
                            "--raftDynamoMap" raft-dynamo-map)
               s3 (conj "--s3Address" s3)
               sqs (conj "--sqsAddress" sqs)
               (and sqs sqs-region) (conj "--sqsRegion" sqs-region)
               (seq raft-groups) (conj "--raftGroups" (build-raft-groups-arg node raft-groups))
               (seq shard-ranges) (conj "--shardRanges" shard-ranges)
               bootstrap? (conj "--raftBootstrap"))
        effective-server-env (cond-> (or server-env {})
                               migration-enabled
                               (merge {"ELASTICKV_ENABLE_MIGRATION_IMPORT_OPCODE" "true"
                                       "ELASTICKV_ENABLE_MIGRATION_PROMOTE_OPCODE" "true"
                                       "ELASTICKV_ENABLE_MIGRATION_CLEANUP_OPCODE" "true"}))
        daemon-opts (cond-> {:chdir bin-dir
                             :logfile log-file
                             :pidfile pid-file
                             :background? true}
                      (seq effective-server-env) (assoc :env effective-server-env))]
    (c/on node
      (c/su
        (c/exec :mkdir :-p data-dir)
        (apply cu/start-daemon! daemon-opts server-bin args)))))

(defn- stop-node!
  [node]
  (c/on node
    (c/su
      (cu/stop-daemon! pid-file)
      (c/exec :rm :-f pid-file))))

(defn- snapshot-transport-metrics!
  [node]
  (c/on node
    (c/su
      (c/exec :bash "-c"
              (str "tmp=$(mktemp /var/log/elastickv-transport-metrics.XXXXXX); "
                   "if curl --connect-timeout 2 --max-time 5 -fsS http://127.0.0.1:9090/metrics "
                   "| grep -E '^elastickv_raft_(send_stream|snapshot_stream|dispatch_errors|dispatch_dropped|step_queue_full)' > \"$tmp\"; "
                   "then { printf '# transport metrics snapshot node=" (name node) " captured_at=%s\\n' \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"; cat \"$tmp\"; } >> " transport-metrics-file "; "
                   "else printf '# metrics unavailable node=" (name node) " captured_at=%s\\n' \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\" >> " transport-metrics-file "; fi; "
                   "rm -f \"$tmp\"")))))

(defn- wait-for-grpc!
  "Wait until the given node listens on grpc port."
  [node grpc-port]
  (let [ports (if (sequential? grpc-port) grpc-port [grpc-port])]
    (doseq [p ports]
      (c/on node
        (c/exec :bash "-c"
                "for i in $(seq 1 60); do if nc -z -w 1 $1 $2; then exit 0; fi; sleep 1; done; echo \\\"Timed out waiting for $1:$2\\\"; exit 1"
                "--" (name node) (str p))))))

(defn- join-node!
  "Join peer into cluster via raftadmin, executed on bootstrap node."
  [bootstrap-node leader-addr peer-id peer-addr]
  (c/on bootstrap-node
    (c/su
      (try (c/exec :pkill :-f "raftadmin") (catch Exception _))
      (c/exec :env "RAFTADMIN_ALLOW_INSECURE=true"
              raftadmin-bin leader-addr "add_voter" peer-id peer-addr "0"))))

(defrecord ElastickvDB [opts]
  db/DB
  (setup! [_ test node]
    (install-deps! node)
    (upload-binaries! test node)
    (c/on node
      (c/su
        (c/exec :mkdir :-p data-dir)
        (c/exec :rm :-f log-file transport-metrics-file)))
    (start-node! test node (merge {:data-dir data-dir
                                   :grpc-port (or (:grpc-port opts) 50051)
                                   :redis-port (or (:redis-port opts) 6379)
                                   :bootstrap-node (first (:nodes test))}
                                  opts))
    (when (= node (first (:nodes test)))
      (let [raft-groups (:raft-groups opts)
            grpc-port (or (:grpc-port opts) 50051)
            group-ids (when (seq raft-groups) (group-ids raft-groups))]
        (doseq [peer (rest (:nodes test))]
          (util/await-fn
            (fn []
              (try
                (if (seq raft-groups)
                  (doseq [gid group-ids]
                    (wait-for-grpc! peer (port-for (get raft-groups gid) peer))
                    (join-node! node
                                (group-addr node raft-groups gid)
                                (name peer)
                                (group-addr peer raft-groups gid)))
                  (do
                    (wait-for-grpc! peer grpc-port)
                    (join-node! node
                                (node-addr node grpc-port)
                                (name peer)
                                (node-addr peer grpc-port))))
                true
                (catch Throwable t
                  (warn t "retrying join for" peer)
                  nil)))
            {:timeout 120000
             :log-message (str "joining " peer)}))))
    (info "node started" node))

  (teardown! [_ _test node]
    (try
      (snapshot-transport-metrics! node)
      (catch Throwable t
        (warn t "transport metrics snapshot failed")))
    (try
      (stop-node! node)
      (catch Throwable t
        (warn t "teardown stop failed")))
    (c/on node
      (c/su
        (c/exec :rm :-rf data-dir))))

  db/LogFiles
  (log-files [_ _test _node]
    {log-file "elastickv.log"
     transport-metrics-file "elastickv-transport-metrics.prom"})

  db/Kill
  (start! [this test node]
    (start-node! test node (merge {:data-dir data-dir
                                   :grpc-port (or (:grpc-port opts) 50051)
                                   :redis-port (or (:redis-port opts) 6379)
                                   :bootstrap-node (first (:nodes test))}
                                  opts))
    (if-let [raft-groups (:raft-groups opts)]
      (wait-for-grpc! node (map (fn [gid] (port-for (get raft-groups gid) node))
                                (group-ids raft-groups)))
      (wait-for-grpc! node (or (:grpc-port opts) 50051)))
    (info "node started" node)
    this)
  (kill! [this _test node]
    (try
      (snapshot-transport-metrics! node)
      (catch Throwable t
        (warn t "transport metrics snapshot before kill failed")))
    (stop-node! node)
    this)

  db/Pause
  (pause! [this _test node]
    (c/on node
      (c/su
        (c/exec :bash "-c"
                (str "if [ -f " pid-file " ]; then kill -STOP $(cat " pid-file "); fi"))))
    this)
  (resume! [this _test node]
    (c/on node
      (c/su
        (c/exec :bash "-c"
                (str "if [ -f " pid-file " ]; then kill -CONT $(cat " pid-file "); fi"))))
    this))

(defn db
  "Constructs an ElastickvDB with optional opts.
   opts: {:grpc-port 50051 :redis-port 6379
          :raft-groups {1 50051 2 50052}
          :shard-ranges \":m=1,m:=2\"
          :server-env {\"ELASTICKV_RAFT_SEND_STREAM\" \"true\"}}"
  ([] (->ElastickvDB {}))
  ([opts] (->ElastickvDB opts)))
