(ns elastickv.encrypted-cluster-test
  "Pins the §8.4 encrypted acceptance gate: the EXISTING Redis and
  DynamoDB workloads must be runnable against a cluster with
  data-at-rest encryption enabled.

  The design is explicit that no new workload is required — encryption
  is consistency-transparent, so the gate is a cluster-setup switch. The
  risk that switch carries is that it silently does nothing: a run that
  reported PASS while the cluster was never actually encrypted would be
  worse than no gate at all, because it would be recorded as evidence."
  (:require [clojure.test :refer :all]
            [elastickv.cli :as cli]
            [elastickv.db :as ekdb]
            [elastickv.dynamodb-multi-table-workload :as multi]
            [elastickv.dynamodb-types-workload :as types]
            [elastickv.dynamodb-workload :as dynamo]
            [elastickv.redis-workload :as redis]
            [elastickv.redis-zset-safety-workload :as zset]
            [elastickv.s3-workload :as s3]
            [elastickv.sqs-htfifo-workload :as sqs]))

(deftest encryption-flag-is-available-to-every-workload
  ;; It lives in common-cli-opts rather than per-workload, so a future
  ;; workload gets the gate without opting in.
  (let [names (set (map second cli/common-cli-opts))]
    (is (contains? names "--encryption"))))

(deftest encryption-defaults-off
  ;; The unencrypted suites are the existing baseline; turning this on
  ;; by default would silently change what every current run measures.
  (let [spec (first (filter #(= "--encryption" (second %)) cli/common-cli-opts))]
    (is (false? (:default (apply hash-map (drop 3 spec)))))))

(deftest redis-workload-propagates-encryption-to-the-db
  (let [test-map (redis/elastickv-redis-test {:encryption true})]
    (is (true? (get-in test-map [:db :opts :encryption])))))

(deftest dynamodb-workload-propagates-encryption-to-the-db
  (let [test-map (dynamo/elastickv-dynamodb-test {:encryption true})]
    (is (true? (get-in test-map [:db :opts :encryption])))))

(deftest encryption-is-absent-from-the-db-when-not-requested
  ;; The flag must not leak a truthy value into an ordinary run.
  (let [test-map (redis/elastickv-redis-test {})]
    (is (not (true? (get-in test-map [:db :opts :encryption]))))))

(deftest db-accepts-the-encryption-option
  ;; ekdb/db carries opts verbatim; this pins that the key survives
  ;; construction rather than being dropped by a destructuring form.
  (let [db (ekdb/db {:grpc-port 50051 :encryption true})]
    (is (true? (get-in db [:opts :encryption])))))

;; ---------------------------------------------------------------------------
;; The load-bearing property: the switch must actually reach the server
;; ---------------------------------------------------------------------------

(defn- args-for [over]
  (ekdb/server-args (merge {:node "n1" :grpc "n1:50051" :redis "n1:6379"
                            :data-dir "/var/lib/elastickv"
                            :raft-redis-map "n1=n1:6379"}
                           over)))

(deftest encryption-emits-all-three-server-flags
  ;; A sidecar path alone only enables read-only capability probing.
  ;; The mutating RPCs the bootstrap needs require --encryption-enabled
  ;; AND a KEK source, so all three must travel together — two of the
  ;; three would produce a cluster that refuses to start, or worse, one
  ;; that starts unencrypted.
  (let [args (set (args-for {:encryption true}))]
    (is (contains? args "--encryption-enabled"))
    (is (contains? args "--encryptionSidecarPath"))
    (is (contains? args "--kekFile"))))

(deftest without-encryption-no-encryption-flag-is-emitted
  ;; The unencrypted suites must be byte-identical to before, or the
  ;; baseline every existing run measures has silently changed.
  (let [args (set (args-for {}))]
    (is (not (contains? args "--encryption-enabled")))
    (is (not (contains? args "--encryptionSidecarPath")))
    (is (not (contains? args "--kekFile")))))

(deftest encryption-does-not-disturb-the-other-flags
  (let [plain     (remove #{"--encryptionSidecarPath" "--encryption-enabled" "--kekFile"}
                          (args-for {:encryption true}))
        encrypted (args-for {})]
    ;; Removing the encryption flags and their values must leave the
    ;; same argv an unencrypted run would produce.
    (is (= (set encrypted)
           (set (remove #(or (= % "/var/lib/elastickv/keys.json")
                             (= % "/var/lib/elastickv/kek.bin"))
                        plain))))))

;; ---------------------------------------------------------------------------
;; The switch must ACTIVATE encryption, not just reach the server
;; ---------------------------------------------------------------------------
;;
;; The flag tests above pass whether or not the cluster ends up encrypted:
;; --encryption-enabled only opens the EncryptionAdmin mutator RPCs, and
;; buildEncryptionWriteWiring keeps the store's envelope gate closed until BOTH
;; BootstrapEncryption and EnableStorageEnvelope have applied. Without those
;; calls every workload ran against a cleartext cluster and still reported PASS
;; -- the exact failure this namespace's docstring names as worse than no gate.

(deftest bootstrap-discovers-the-writer-batch-from-every-member
  ;; §5.6 step 1a needs one registry entry per member, with each node's real
  ;; full_node_id and local_epoch. Hand-written --writer entries would go stale
  ;; as soon as a node restarted and bumped its epoch, so the batch must be
  ;; discovered from every node.
  (let [args (ekdb/bootstrap-args "n1:50051" ["n1:50051" "n2:50051" "n3:50051"] "WS" "WR")]
    (is (= 3 (count (filter #(re-find #"^--discover-from=" %) args))))
    (is (some #{"--discover-from=n2:50051"} args))
    (is (some #{"--discover-from=n3:50051"} args))))

(deftest bootstrap-passes-distinct-non-zero-dek-ids
  ;; Bootstrap rejects a zero id and rejects the two being equal.
  (let [args (ekdb/bootstrap-args "n1:50051" ["n1:50051"] "WS" "WR")
        id   (fn [flag] (some->> args
                                 (filter #(clojure.string/starts-with? % (str flag "=")))
                                 first
                                 (re-find #"\d+$")
                                 Long/parseLong))]
    (is (pos? (id "--storage-dek-id")))
    (is (pos? (id "--raft-dek-id")))
    (is (not= (id "--storage-dek-id") (id "--raft-dek-id")))))

(deftest bootstrap-carries-both-wrapped-deks
  (let [args (set (ekdb/bootstrap-args "n1:50051" ["n1:50051"] "WRAPPED-S" "WRAPPED-R"))]
    (is (contains? args "--wrapped-storage-dek=WRAPPED-S"))
    (is (contains? args "--wrapped-raft-dek=WRAPPED-R"))))

(deftest enable-storage-envelope-carries-the-proposer-identity
  ;; §6.1 treats proposer-node-id 0 as the not-capable sentinel, so the real
  ;; full_node_id read back from `encryption status` has to be threaded through.
  (let [args (set (ekdb/enable-storage-envelope-args "n1:50051" 12345 7))]
    (is (contains? args "--proposer-node-id=12345"))
    (is (contains? args "--proposer-local-epoch=7"))))

(deftest status-parsing-reads-the-fields-the-cutover-needs
  (let [out (str "capability:\n"
                 "  encryption_capable: true\n"
                 "  sidecar_present:    true\n"
                 "  full_node_id:       8675309\n"
                 "  local_epoch:        3\n"
                 "sidecar:\n"
                 "  storage_envelope_active:     true\n")
        got (ekdb/parse-encryption-status out)]
    (is (= 8675309 (:full-node-id got)))
    (is (= 3 (:local-epoch got)))
    (is (true? (:storage-envelope-active got)))))

(deftest status-parsing-treats-an-inactive-envelope-as-inactive
  ;; The polling loop waits on this value, so a false must never read as true:
  ;; that would let the workload start against a cleartext cluster, which is the
  ;; whole failure mode.
  (let [out (str "capability:\n"
                 "  full_node_id:       1\n"
                 "  local_epoch:        0\n"
                 "sidecar:\n"
                 "  storage_envelope_active:     false\n")
        got (ekdb/parse-encryption-status out)]
    (is (false? (:storage-envelope-active got)))))

(deftest status-parsing-does-not-invent-an-active-envelope
  ;; A node with no sidecar omits the line entirely. Absent must not be active.
  (let [got (ekdb/parse-encryption-status "capability:\n  encryption_capable: false\n")]
    (is (false? (:storage-envelope-active got)))
    (is (nil? (:full-node-id got)))))

(deftest encryption-admin-targets-the-default-raft-group
  ;; Bootstrap and the cutover are proposed through the default group, so a
  ;; multi-group deployment must be addressed on that group's port. group-ids is
  ;; sorted, so the lowest id is the default one.
  (is (= "n1:50051" (ekdb/encryption-endpoint "n1" 50051 nil)))
  (is (= "n1:50061" (ekdb/encryption-endpoint "n1" 50051 {1 50061, 2 50062})))
  (is (= "n1:50061" (ekdb/encryption-endpoint "n1" 50051 {2 50062, 1 50061}))))

;; ---------------------------------------------------------------------------
;; Every workload that accepts --encryption must honour it
;; ---------------------------------------------------------------------------

(deftest every-workload-accepting-encryption-propagates-it
  ;; --encryption lives in common-cli-opts so that, as the test above puts it,
  ;; "a future workload gets the gate without opting in". That promise was not
  ;; kept: five entrypoints accepted the flag from the common options and then
  ;; dropped it when constructing ekdb/db, so `--encryption` on those commands
  ;; silently launched a cleartext cluster -- the CLI advertising a guarantee it
  ;; did not provide.
  ;;
  ;; Driving every constructor from one list is the point: a new workload that
  ;; forgets to thread the option fails here instead of shipping a silent lie.
  (doseq [[label ctor] [["redis"                 redis/elastickv-redis-test]
                        ["redis-zset-safety"     zset/elastickv-zset-safety-test]
                        ["dynamodb"              dynamo/elastickv-dynamodb-test]
                        ["dynamodb-types"        types/elastickv-dynamodb-types-test]
                        ["dynamodb-multi-table"  multi/elastickv-dynamodb-multi-table-test]
                        ["s3"                    s3/elastickv-s3-test]
                        ["sqs-htfifo"            sqs/elastickv-sqs-htfifo-test]]]
    (testing label
      (is (true? (get-in (ctor {:encryption true}) [:db :opts :encryption]))
          (str label " accepts --encryption from common-cli-opts but drops it"))
      (is (not (true? (get-in (ctor {}) [:db :opts :encryption])))
          (str label " must not enable encryption when it was not requested")))))
