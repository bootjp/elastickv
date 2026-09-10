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
            [elastickv.dynamodb-workload :as dynamo]
            [elastickv.redis-workload :as redis]))

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
