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

;; §8.4 acceptance gate: the encrypted-cluster run reuses the EXISTING
;; Redis and DynamoDB workloads rather than adding a new one —
;; encryption is consistency-transparent (same input bytes, different
;; output bytes, apply still deterministic), so what has to be built is
;; the ability to stand the cluster up encrypted, not a new checker.
(def ^:private kek-file (str data-dir "/kek.bin"))
(def ^:private sidecar-file (str data-dir "/keys.json"))

;; kek-bytes is the §5.1 KEK: exactly 32 raw bytes, owner-only mode.
;; A fixed test value, not a generated one — every node in the cluster
;; must unwrap the same sidecar, and a per-node random KEK would make
;; the cluster refuse to start with ErrKEKMismatch, which is a far more
;; confusing failure than a hardcoded test key.
(def ^:private kek-test-bytes
  (apply str (repeat 32 "k")))
(def ^:private raftadmin-bin (str bin-dir "/raftadmin"))
(def ^:private admin-bin (str bin-dir "/elastickv-admin"))
(def ^:private encryption-setup-bin (str bin-dir "/jepsen-encryption-setup"))

;; §5.2 DEK ids for the bootstrap. Any non-zero pair that differs is valid;
;; bootstrap rejects zero and rejects the two being equal.
(def ^:private storage-dek-id 1)
(def ^:private raft-dek-id 2)

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
                            ["elastickv-admin" ["go" "build" "-o" (str build-dir "/elastickv-admin") "./cmd/elastickv-admin"]]
                            ["jepsen-encryption-setup" ["go" "build" "-o" (str build-dir "/jepsen-encryption-setup") "./cmd/jepsen-encryption-setup"]]]]
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
      (doseq [bin ["elastickv" "raftadmin" "elastickv-admin" "jepsen-encryption-setup"]]
        (c/upload (str build-dir "/" bin) (str bin-dir "/" bin))
        (c/exec :chmod "755" (str bin-dir "/" bin))))))

(defn- provision-kek!
  "Writes the §5.1 KEK file with owner-only permissions.

  Runs before start-node! because --encryption-enabled refuses to start
  without a readable KEK source, and the refusal happens during startup
  guards — well before anything the workload could observe."
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

(defn server-args
  "Builds the elastickv server argv for one node.

  Extracted from start-node! as a pure function so the flag set — and
  in particular whether encryption is actually switched on — is
  testable without SSH. A --encryption run that silently produced an
  UNENCRYPTED cluster would report PASS and be recorded as evidence for
  the §8.4 acceptance gate, which is worse than having no gate."
  [{:keys [node grpc redis dynamo s3 sqs sqs-region data-dir raft-engine
           raft-redis-map raft-dynamo-map raft-groups shard-ranges
           encryption bootstrap?]}]
  (cond-> ["--address" grpc
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
    ;; Sidecar path alone only enables read-only capability probing; the
    ;; mutating RPCs the bootstrap needs also require
    ;; --encryption-enabled AND a KEK source, so the three travel
    ;; together or not at all.
    encryption (conj "--encryptionSidecarPath" sidecar-file
                     "--encryption-enabled"
                     "--kekFile" kek-file)
    bootstrap? (conj "--raftBootstrap")))

(defn- start-node!
  [test node {:keys [bootstrap-node grpc-port redis-port dynamo-port s3-port sqs-port sqs-region data-dir raft-groups shard-ranges raft-engine server-env encryption]}]
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
        args (server-args
               {:node node :grpc grpc :redis redis :dynamo dynamo :s3 s3 :sqs sqs
                :sqs-region sqs-region :data-dir data-dir :raft-engine raft-engine
                :raft-redis-map raft-redis-map :raft-dynamo-map raft-dynamo-map
                :raft-groups raft-groups :shard-ranges shard-ranges
                :encryption encryption :bootstrap? bootstrap?})
        daemon-opts (cond-> {:chdir bin-dir
                             :logfile log-file
                             :pidfile pid-file
                             :background? true}
                      (seq server-env) (assoc :env server-env))]
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

(defn encryption-endpoint
  "Returns the gRPC address the EncryptionAdmin RPCs must be sent to.

  The bootstrap and cutover entries are proposed through the DEFAULT Raft
  group, so a multi-group deployment must be addressed on that group's port
  rather than on whichever port happens to be first in the map. group-ids is
  sorted, so the lowest group id is the default one."
  [node grpc-port raft-groups]
  (if (seq raft-groups)
    (group-addr node raft-groups (first (group-ids raft-groups)))
    (node-addr node grpc-port)))

(defn bootstrap-args
  "argv for `elastickv-admin encryption bootstrap`.

  The writer batch comes from --discover-from rather than hand-written
  --writer entries: §5.6 step 1a requires one registry entry per member, and
  polling GetCapability is the only way to learn each node's real full_node_id
  and local_epoch. A hand-written batch would go stale the moment a node
  restarted and bumped its epoch."
  [endpoint peer-endpoints wrapped-storage wrapped-raft]
  (concat [admin-bin "encryption" "bootstrap"
           (str "--endpoint=" endpoint)
           (str "--storage-dek-id=" storage-dek-id)
           (str "--raft-dek-id=" raft-dek-id)
           (str "--wrapped-storage-dek=" wrapped-storage)
           (str "--wrapped-raft-dek=" wrapped-raft)]
          (map #(str "--discover-from=" %) peer-endpoints)))

(defn enable-storage-envelope-args
  "argv for `elastickv-admin encryption enable-storage-envelope`.

  This is the step that actually makes writes ciphertext: --encryption-enabled
  only opens the mutator RPCs, and buildEncryptionWriteWiring keeps the store
  gate closed until this entry applies."
  [endpoint full-node-id local-epoch]
  [admin-bin "encryption" "enable-storage-envelope"
   (str "--endpoint=" endpoint)
   (str "--proposer-node-id=" full-node-id)
   (str "--proposer-local-epoch=" local-epoch)])

(defn parse-encryption-status
  "Parses `elastickv-admin encryption status` output into a map.

  Returns :full-node-id and :local-epoch (needed as the proposer identity for
  the cutover) and :storage-envelope-active (the only thing that proves the
  cluster is storing ciphertext)."
  [out]
  (let [field (fn [k] (second (re-find (re-pattern (str "(?m)^\\s*" k ":\\s*(\\S+)\\s*$")) out)))
        num   (fn [k] (some-> (field k) Long/parseLong))]
    {:full-node-id            (num "full_node_id")
     :local-epoch             (num "local_epoch")
     :storage-envelope-active (= "true" (field "storage_envelope_active"))}))

(defn- encryption-status
  [node endpoint]
  (parse-encryption-status
    (c/on node (c/su (c/exec admin-bin "encryption" "status" (str "--endpoint=" endpoint))))))

(defn- wrap-fresh-dek!
  "Returns a base64 KEK-wrapped DEK, generated on the node."
  [node]
  (clojure.string/trim
    (c/on node (c/su (c/exec encryption-setup-bin (str "--kek-file=" kek-file))))))

(defn- activate-encryption!
  "Bootstraps the DEKs and performs the §7.1 Phase-1 storage cutover, then
  VERIFIES it applied.

  The verification is the point. --encryption-enabled only enables the
  EncryptionAdmin mutator RPCs; buildEncryptionWriteWiring deliberately keeps
  the store's envelope gate closed until BOTH BootstrapEncryption and
  EnableStorageEnvelope have applied. Without these calls every workload ran
  against a cleartext cluster and could still report PASS -- which, as the §8.4
  gate's own rationale says, is worse than having no gate, because the run gets
  recorded as encryption evidence. So a cluster that does not report
  storage_envelope_active here must fail setup rather than proceed."
  [test node grpc-port raft-groups]
  (let [endpoint  (encryption-endpoint node grpc-port raft-groups)
        peers     (map #(encryption-endpoint % grpc-port raft-groups) (:nodes test))
        wrapped-s (wrap-fresh-dek! node)
        wrapped-r (wrap-fresh-dek! node)]
    (info "bootstrapping encryption" endpoint)
    (c/on node (c/su (apply c/exec (bootstrap-args endpoint peers wrapped-s wrapped-r))))
    (let [{:keys [full-node-id local-epoch]} (encryption-status node endpoint)]
      (when-not full-node-id
        (throw (ex-info "encryption status did not report a full_node_id"
                        {:endpoint endpoint})))
      (info "enabling storage envelope" endpoint full-node-id local-epoch)
      (c/on node (c/su (apply c/exec (enable-storage-envelope-args
                                       endpoint full-node-id (or local-epoch 0))))))
    ;; Every node must report the cutover, not just the proposer: a node that
    ;; has not applied it is still writing cleartext, and the workload would be
    ;; measuring a half-encrypted cluster.
    (doseq [peer (:nodes test)]
      (let [peer-endpoint (encryption-endpoint peer grpc-port raft-groups)]
        (util/await-fn
          (fn []
            (when (:storage-envelope-active (encryption-status node peer-endpoint))
              true))
          {:timeout 60000
           :log-message (str "waiting for storage envelope cutover on " peer)})))
    (info "encryption active on every node")))

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
             :log-message (str "joining " peer)}))
        ;; After membership, not before: the bootstrap's writer batch needs a
        ;; registry entry for every member (§5.6 step 1a), and the cutover's
        ;; capability gate requires every voter to report encryption-capable.
        ;; Running this against a single-node cluster would register one writer
        ;; and then refuse the cutover once the peers joined.
        (when (:encryption opts)
          (activate-encryption! test node grpc-port raft-groups))))
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
