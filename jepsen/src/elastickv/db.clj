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
(def ^:private admin-bin (str bin-dir "/elastickv-admin"))
(def ^:private encryption-setup-bin (str bin-dir "/jepsen-encryption-setup"))
(def ^:private kek-file (str data-dir "/kek.bin"))
(def ^:private sidecar-file (str data-dir "/keys.json"))
(def ^:private kek-test-bytes (apply str (repeat 32 "k")))
(def ^:private storage-dek-id 1)
(def ^:private raft-dek-id 2)

(def ^:private build-dir
  ;; local (control node) directory for built binaries
  (str (System/getProperty "user.dir") "/target/elastickv-jepsen"))

(defn- ensure-build-dir! []
  (doto (io/file build-dir)
    (.mkdirs)))

(def ^:private binary-builds
  [["elastickv" ["go" "build" "-o" (str build-dir "/elastickv") "."]]
   ["raftadmin" ["go" "build" "-o" (str build-dir "/raftadmin") "./cmd/raftadmin"]]
   ["elastickv-admin" ["go" "build" "-o" (str build-dir "/elastickv-admin") "./cmd/elastickv-admin"]]
   ["jepsen-encryption-setup" ["go" "build" "-o" (str build-dir "/jepsen-encryption-setup") "./cmd/jepsen-encryption-setup"]]])

(defn- build-binaries!
  "Build the production server and Jepsen administration helpers."
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
    (doseq [[out-cmd args] binary-builds]
      (let [{:keys [exit err]} (apply sh/sh (concat args [:env env :dir root]))]
        (when-not (zero? exit)
          (throw (ex-info (str "failed to build " out-cmd) {:err err})))))))

(defonce ^:private built? (delay (build-binaries!)))

(defn- install-deps!
  "Install minimal packages on a node."
  [node]
  (c/on node
    (debian/install [:curl :netcat-openbsd :rsync :iptables :chrony :libfaketime :fuse3])))

(defn- upload-binaries!
  "Copy built binaries to the given node."
  [test node]
  @built?
  (c/on node
    (c/su
      (c/exec :mkdir :-p bin-dir)
      (doseq [bin ["elastickv" "raftadmin" "elastickv-admin" "jepsen-encryption-setup"]]
        (c/upload (str build-dir "/" bin) (str bin-dir "/" bin))
        (c/exec :chmod "755" (str bin-dir "/" bin))))))

(defn- provision-kek!
  "Writes the test-only file KEK before the encryption-capable server starts."
  [node]
  (c/on node
    (c/su
      (c/exec :mkdir :-p data-dir)
      (c/exec :bash :-c (str "printf '%s' '" kek-test-bytes "' > " kek-file))
      (c/exec :chmod "600" kek-file))))

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

(defn- frontend-maps
  [nodes grpc-port redis-port dynamo-port s3-port sqs-port raft-groups]
  {:raft-redis-map (build-raft-redis-map nodes grpc-port redis-port raft-groups)
   :raft-dynamo-map (when dynamo-port
                      (build-raft-dynamo-map nodes grpc-port dynamo-port raft-groups))
   :raft-s3-map (when s3-port
                  (build-raft-service-map nodes grpc-port s3-port raft-groups))
   :raft-sqs-map (when sqs-port
                   (build-raft-service-map nodes grpc-port sqs-port raft-groups))})

(defn server-args
  "Builds the production server argv for one managed Jepsen node."
  [{:keys [node grpc redis dynamo s3 sqs sqs-region data-dir raft-engine
           raft-redis-map raft-dynamo-map raft-s3-map raft-sqs-map raft-groups shard-ranges
           filesystem-mount encryption bootstrap?]}]
  (cond-> ["--address" grpc
           "--redisAddress" redis
           "--raftId" (name node)
           "--raftDataDir" data-dir
           "--raftEngine" (or raft-engine "etcd")
           "--raftRedisMap" raft-redis-map]
    dynamo (conj "--dynamoAddress" dynamo
                 "--raftDynamoMap" raft-dynamo-map)
    s3 (conj "--s3Address" s3 "--raftS3Map" raft-s3-map)
    sqs (conj "--sqsAddress" sqs "--raftSqsMap" raft-sqs-map)
    (and sqs sqs-region) (conj "--sqsRegion" sqs-region)
    filesystem-mount (conj "--filesystemMount" filesystem-mount)
    (seq raft-groups) (conj "--raftGroups" (build-raft-groups-arg node raft-groups))
    (seq shard-ranges) (conj "--shardRanges" shard-ranges)
    encryption (conj "--encryptionSidecarPath" sidecar-file
                     "--encryption-enabled"
                     "--kekFile" kek-file)
    bootstrap? (conj "--raftBootstrap")))

(defn- filesystem-mount-dir
  "Resolves the FUSE mount directory for node from the :filesystem-mount
   option, which is either a single path applied to every node or a map of
   node -> path. nil disables the filesystem frontend."
  [filesystem-mount node]
  (cond
    (nil? filesystem-mount) nil
    (map? filesystem-mount) (get filesystem-mount node)
    :else                   filesystem-mount))

(defn- sh-quote
  "Single-quotes s for use inside a bash -c script."
  [s]
  (str "'" (clojure.string/replace (str s) "'" "'\\''") "'"))

(defn- proc-mounts-escape
  "Encodes a path the way /proc/mounts prints mount points: backslash, space,
   tab and newline become octal escapes."
  [path]
  (-> (str path)
      (clojure.string/replace "\\" "\\134")
      (clojure.string/replace " " "\\040")
      (clojure.string/replace "\t" "\\011")
      (clojure.string/replace "\n" "\\012")))

(defn- unmount-filesystem-cmd
  "Shell snippet that lazily detaches a (possibly dangling) FUSE mount. A
   node killed with SIGKILL leaves its mount point in a 'transport endpoint
   is not connected' state, which would make the next --filesystemMount fail.
   The filesystem type comes from the mounts table (mountpoint(1) cannot stat
   a dangling mount) and only a FUSE mount is touched: the snippet runs as
   root, so a mis-typed --filesystem-mount naming an unrelated volume must
   warn rather than reach the generic umount fallback. The path is quoted
   because the snippet runs through bash -c."
  ([mount-dir]
   (unmount-filesystem-cmd mount-dir "/proc/mounts"))
  ([mount-dir mounts-file]
   (let [q   (sh-quote mount-dir)
         esc (sh-quote (proc-mounts-escape mount-dir))
         mf  (sh-quote mounts-file)]
     (str "fstype=$(while read -r _ mp fs _; do [ \"$mp\" = " esc " ] && printf '%s\\n' \"$fs\"; done < " mf " | tail -n 1); "
          "case \"$fstype\" in "
          "'') ;; "
          "fuse|fuse.*|fuseblk) fusermount3 -uz " q " 2>/dev/null || fusermount -uz " q " 2>/dev/null || umount -l " q " || true ;; "
          "*) echo \"refusing to unmount non-FUSE filesystem ($fstype) at \"" q " >&2 ;; "
          "esac"))))

(defn- wait-for-mount-cmd
  "Shell snippet that waits up to ~60s for mount-dir to become a mountpoint.
   The server mounts the filesystem after its public listeners are up, so a
   gRPC readiness check alone would let clients hit the bare directory."
  [mount-dir]
  (let [q (sh-quote mount-dir)]
    (str "for i in $(seq 1 60); do if mountpoint -q " q "; then exit 0; fi; sleep 1; done; "
         "echo \"Timed out waiting for FUSE mount at \"" q "; exit 1")))

(defn- prepare-filesystem-mount!
  "Ensures mount-dir exists and holds no stale mount before the server starts."
  [mount-dir]
  (c/exec :bash "-c" (unmount-filesystem-cmd mount-dir))
  (c/exec :mkdir :-p mount-dir))

(defn- wait-for-mount!
  "Blocks until node has mounted the filesystem at mount-dir (no-op when nil)."
  [node mount-dir]
  (when mount-dir
    (c/on node
      (c/exec :bash "-c" (wait-for-mount-cmd mount-dir)))))

(defn- cleanup-filesystem-mount!
  "Detaches the node's mount after the server stopped."
  [node mount-dir]
  (when mount-dir
    (c/on node
      (c/su
        (c/exec :bash "-c" (unmount-filesystem-cmd mount-dir))))))

(defn- start-node!
  [test node {:keys [bootstrap-node grpc-port redis-port dynamo-port s3-port sqs-port sqs-region data-dir raft-groups shard-ranges raft-engine server-env filesystem-mount encryption]}]
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
        {:keys [raft-redis-map raft-dynamo-map raft-s3-map raft-sqs-map]}
        (frontend-maps (:nodes test) grpc-port redis-port dynamo-port s3-port sqs-port raft-groups)
        mount-dir (filesystem-mount-dir filesystem-mount node)
        bootstrap? (= node bootstrap-node)
        args (server-args
               {:node node :grpc grpc :redis redis :dynamo dynamo :s3 s3 :sqs sqs
                :sqs-region sqs-region :data-dir data-dir :raft-engine raft-engine
                :raft-redis-map raft-redis-map :raft-dynamo-map raft-dynamo-map
                :raft-s3-map raft-s3-map :raft-sqs-map raft-sqs-map
                :raft-groups raft-groups :shard-ranges shard-ranges
                :filesystem-mount mount-dir :encryption encryption
                :bootstrap? bootstrap?})
        daemon-opts (cond-> {:chdir bin-dir
                             :logfile log-file
                             :pidfile pid-file
                             :background? true}
                      (seq server-env) (assoc :env server-env))]
    (c/on node
      (c/su
        (c/exec :mkdir :-p data-dir)
        (when mount-dir
          (prepare-filesystem-mount! mount-dir))
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

(defn encryption-endpoint
  "Returns the default Raft group's gRPC address for EncryptionAdmin."
  [node grpc-port raft-groups]
  (if (seq raft-groups)
    (group-addr node raft-groups (first (group-ids raft-groups)))
    (node-addr node grpc-port)))

(defn bootstrap-args
  "Builds argv for encryption bootstrap with a discovered writer per member."
  [endpoint peer-endpoints wrapped-storage wrapped-raft]
  (concat [admin-bin "encryption" "bootstrap"
           (str "--endpoint=" endpoint)
           (str "--storage-dek-id=" storage-dek-id)
           (str "--raft-dek-id=" raft-dek-id)
           (str "--wrapped-storage-dek=" wrapped-storage)
           (str "--wrapped-raft-dek=" wrapped-raft)]
          (map #(str "--discover-from=" %) peer-endpoints)))

(defn enable-storage-envelope-args
  [endpoint full-node-id local-epoch]
  [admin-bin "encryption" "enable-storage-envelope"
   (str "--endpoint=" endpoint)
   (str "--proposer-node-id=" full-node-id)
   (str "--proposer-local-epoch=" local-epoch)])

(defn enable-raft-envelope-args
  [endpoint full-node-id local-epoch]
  [admin-bin "encryption" "enable-raft-envelope"
   (str "--endpoint=" endpoint)
   (str "--proposer-node-id=" full-node-id)
   (str "--proposer-local-epoch=" local-epoch)])

(defn parse-encryption-status
  "Parses the proposer identity and both fail-closed envelope states."
  [out]
  (let [field (fn [k] (second (re-find (re-pattern (str "(?m)^\\s*" k ":\\s*(\\S+)\\s*$")) out)))
        num   (fn [k] (some-> (field k) bigint))]
    {:full-node-id            (num "full_node_id")
     :local-epoch             (num "local_epoch")
     :storage-envelope-active (= "true" (field "storage_envelope_active"))
     :raft-envelope-active    (pos? (or (num "raft_envelope_cutover_index") 0))}))

(defn- encryption-status
  [node endpoint]
  (parse-encryption-status
    (c/on node (c/su (c/exec admin-bin "encryption" "status" (str "--endpoint=" endpoint))))))

(defn- wrap-fresh-dek!
  [node]
  (clojure.string/trim
    (c/on node (c/su (c/exec encryption-setup-bin (str "--kek-file=" kek-file))))))

(defn- encryption-command!
  [node args]
  (c/on node (c/su (apply c/exec args))))

(defn- on-encryption-leader!
  "Retries an activation operation across members while elections settle."
  [endpoints operation]
  (util/await-fn
    (fn []
      (or (some (fn [endpoint]
                  (try
                    (operation endpoint)
                    true
                    (catch InterruptedException t
                      (throw t))
                    (catch Exception t
                      (warn t "retrying encryption activation" endpoint)
                      nil)))
                endpoints)
          (throw (ex-info "no member accepted the encryption operation"
                          {:endpoints endpoints}))))
    {:timeout 120000
     :log-message "waiting for an encryption leader"}))

(defn- cutover-encryption!
  [node endpoints args-fn]
  (on-encryption-leader!
    endpoints
    (fn [endpoint]
      (let [{:keys [full-node-id local-epoch]} (encryption-status node endpoint)]
        (when-not (and full-node-id (pos? full-node-id) (some? local-epoch))
          (throw (ex-info "encryption status did not report a proposer identity"
                          {:endpoint endpoint})))
        (encryption-command! node (args-fn endpoint full-node-id local-epoch))))))

(defn- activate-encryption!
  "Bootstraps both DEKs, activates both envelopes, and verifies every member."
  [test node grpc-port raft-groups]
  (let [peers     (mapv #(encryption-endpoint % grpc-port raft-groups) (:nodes test))
        wrapped-s (wrap-fresh-dek! node)
        wrapped-r (wrap-fresh-dek! node)]
    (on-encryption-leader!
      peers
      (fn [endpoint]
        (encryption-command! node (bootstrap-args endpoint peers wrapped-s wrapped-r))))
    (cutover-encryption! node peers enable-storage-envelope-args)
    (cutover-encryption! node peers enable-raft-envelope-args)
    (doseq [peer (:nodes test)]
      (let [peer-endpoint (encryption-endpoint peer grpc-port raft-groups)]
        (util/await-fn
          (fn []
            (let [state (encryption-status node peer-endpoint)]
              (when-not (and (:storage-envelope-active state)
                             (:raft-envelope-active state))
                (throw (ex-info "encryption envelopes are not active on member"
                                {:endpoint peer-endpoint :state state})))
              true))
          {:timeout 60000
           :log-message (str "waiting for both envelope cutovers on " peer)})))
    (info "encryption active on every node")))

(defn- node-start-opts
  [test opts]
  (merge {:data-dir data-dir
          :grpc-port (or (:grpc-port opts) 50051)
          :redis-port (or (:redis-port opts) 6379)
          :bootstrap-node (first (:nodes test))}
         opts))

(defn- wait-for-node-grpc!
  [node opts]
  (if-let [raft-groups (:raft-groups opts)]
    (wait-for-grpc! node (map (fn [gid] (port-for (get raft-groups gid) node))
                              (group-ids raft-groups)))
    (wait-for-grpc! node (or (:grpc-port opts) 50051))))

(defn- join-peer!
  "Adds peer to the cluster led by bootstrap-node, retrying until it answers."
  [bootstrap-node peer opts]
  (let [raft-groups (:raft-groups opts)
        grpc-port (or (:grpc-port opts) 50051)
        group-ids (when (seq raft-groups) (group-ids raft-groups))]
    (util/await-fn
      (fn []
        (if (seq raft-groups)
          (doseq [gid group-ids]
            (wait-for-grpc! peer (port-for (get raft-groups gid) peer))
            (join-node! bootstrap-node
                        (group-addr bootstrap-node raft-groups gid)
                        (name peer)
                        (group-addr peer raft-groups gid)))
          (do
            (wait-for-grpc! peer grpc-port)
            (join-node! bootstrap-node
                        (node-addr bootstrap-node grpc-port)
                        (name peer)
                        (node-addr peer grpc-port))))
        true)
      {:timeout 120000
       :log-message (str "joining " peer)})))

(defn- enable-peer-mount!
  "Restarts a freshly joined peer with its FUSE mount. The server initializes
   the filesystem root synchronously at startup and needs a Raft leader for
   that, which an unjoined peer does not have, so peers start without the
   mount and only get it once they are cluster members."
  [test peer opts]
  (when-let [mount-dir (filesystem-mount-dir (:filesystem-mount opts) peer)]
    (stop-node! peer)
    (start-node! test peer (node-start-opts test opts))
    (wait-for-node-grpc! peer opts)
    (wait-for-mount! peer mount-dir)
    (info "filesystem mounted on joined peer" peer)))

(defn- finish-bootstrap-setup!
  "Waits for the bootstrap listener, joins peers, then activates encryption."
  [test node opts]
  (wait-for-node-grpc! node opts)
  (wait-for-mount! node (filesystem-mount-dir (:filesystem-mount opts) node))
  (doseq [peer (rest (:nodes test))]
    (join-peer! node peer opts)
    (enable-peer-mount! test peer opts))
  (when (:encryption opts)
    (activate-encryption! test node (or (:grpc-port opts) 50051)
                          (:raft-groups opts))))

(defrecord ElastickvDB [opts]
  db/DB
  (setup! [_ test node]
    (install-deps! node)
    (upload-binaries! test node)
    (c/on node
      (c/su
        (c/exec :mkdir :-p data-dir)
        (c/exec :rm :-f log-file transport-metrics-file)))
    (when (:encryption opts)
      (provision-kek! node))
    (let [bootstrap? (= node (first (:nodes test)))
          start-opts (node-start-opts test opts)]
      ;; Only the bootstrap node mounts immediately; peers are restarted with
      ;; their mount by enable-peer-mount! once they have joined.
      (start-node! test node (if bootstrap?
                               start-opts
                               (assoc start-opts :filesystem-mount nil)))
      (when bootstrap?
        (finish-bootstrap-setup! test node opts)))
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
    (try
      (cleanup-filesystem-mount! node (filesystem-mount-dir (:filesystem-mount opts) node))
      (catch Throwable t
        (warn t "teardown unmount failed")))
    (c/on node
      (c/su
        (c/exec :rm :-rf data-dir))))

  db/LogFiles
  (log-files [_ _test _node]
    {log-file "elastickv.log"
     transport-metrics-file "elastickv-transport-metrics.prom"})

  db/Kill
  (start! [this test node]
    (start-node! test node (node-start-opts test opts))
    (wait-for-node-grpc! node opts)
    (wait-for-mount! node (filesystem-mount-dir (:filesystem-mount opts) node))
    (info "node started" node)
    this)
  (kill! [this _test node]
    (try
      (snapshot-transport-metrics! node)
      (catch Throwable t
        (warn t "transport metrics snapshot before kill failed")))
    (stop-node! node)
    (try
      (cleanup-filesystem-mount! node (filesystem-mount-dir (:filesystem-mount opts) node))
      (catch Throwable t
        (warn t "unmount after kill failed")))
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
          :encryption true
          :filesystem-mount \"/mnt/elastickv\" ; or {node -> dir}; nil disables FUSE
          :raft-groups {1 50051 2 50052}
          :shard-ranges \":m=1,m:=2\"
          :server-env {\"ELASTICKV_RAFT_SEND_STREAM\" \"true\"}}"
  ([] (->ElastickvDB {}))
  ([opts]
   (when (and (:encryption opts)
              (> (count (:raft-groups opts)) 1))
     (throw (ex-info "--encryption currently requires exactly one Raft group"
                     {:raft-group-count (count (:raft-groups opts))})))
   (->ElastickvDB opts)))
