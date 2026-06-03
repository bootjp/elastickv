(ns elastickv.dynamodb-multi-table-workload-test
  (:require [clojure.test :refer :all]
            [jepsen.client :as client]
            [elastickv.dynamodb-multi-table-workload :as workload]))

(deftest builds-test-spec
  (let [test-map (workload/elastickv-dynamodb-multi-table-test {})]
    (is (map? test-map))
    (is (= "elastickv-dynamodb-append-multi-table" (:name test-map)))
    (is (= ["n1" "n2" "n3" "n4" "n5"] (:nodes test-map)))))

(deftest custom-options-override-defaults
  (let [test-map (workload/elastickv-dynamodb-multi-table-test
                   {:time-limit 60
                    :concurrency 10
                    :dynamo-port 9000})]
    (is (= 10 (:concurrency test-map)))))

(deftest host-override-creates-client
  (let [test-map (workload/elastickv-dynamodb-multi-table-test
                   {:dynamo-host "127.0.0.1"
                    :node->port  {"n1" 8000 "n2" 8001}})
        c        (:client test-map)
        opened   (client/open! c test-map "n1")]
    (is (some? (:ddb opened)))))

;; ---------------------------------------------------------------------------
;; Key routing — the load-bearing M5a invariant
;; ---------------------------------------------------------------------------

;; key->table-idx, key->table-name and key->pk are private inside the
;; workload namespace, but the routing they encode IS the M5a contract
;; with the launch script's --shardRanges (tables 1-2 → group 1, 3-4
;; → group 2).  Access them via var here so a future change to the
;; routing surfaces in this test rather than as a silent G1c during a
;; Jepsen run.

(def ^:private key->table-idx  (var-get #'workload/key->table-idx))
(def ^:private key->table-name (var-get #'workload/key->table-name))
(def ^:private key->pk         (var-get #'workload/key->pk))

(deftest key-routing-distributes-across-all-tables
  ;; Elle's default key-count is 12.  With N=4 tables and the
  ;; (mod k N)+1 routing, keys [0..11] must land 3-per-table.
  ;; If this fails, the launch script's --shardRanges layout no
  ;; longer makes every Elle key reachable.
  (let [table-counts (->> (range 12)
                          (map key->table-idx)
                          frequencies)]
    (is (= {1 3, 2 3, 3 3, 4 3} table-counts)
        "12 keys must distribute evenly across 4 tables (3 per table)")))

(deftest key-routing-table-name-matches-prefix
  (is (= "jepsen_append_t1" (key->table-name 0)))
  (is (= "jepsen_append_t2" (key->table-name 1)))
  (is (= "jepsen_append_t3" (key->table-name 2)))
  (is (= "jepsen_append_t4" (key->table-name 3)))
  (is (= "jepsen_append_t1" (key->table-name 4))
      "wraparound: k=4 must hit the same table as k=0"))

(deftest key-routing-pks-disambiguate_colliding_keys
  ;; k=0 and k=4 land on the same table (t1) but must have distinct pks
  ;; so each Elle key maps to a unique storage location.
  (is (= "0" (key->pk 0)))
  (is (= "1" (key->pk 4)))
  (is (= "2" (key->pk 8)))
  (is (not= (key->pk 0) (key->pk 4))
      "colliding-table keys must have distinct pks"))

(deftest multi-op-txn-spans_multiple_groups
  ;; The M5a launch script places tables {1,2} in group 1 and {3,4}
  ;; in group 2.  A default 4-mop txn with keys [0,1,2,3] must
  ;; touch BOTH groups — that is the entire point of the multi-table
  ;; workload (otherwise dispatchMultiShardTxn never fires).
  (let [keys-in-group1 #{1 2}
        keys-in-group2 #{3 4}
        sample-txn-keys [0 1 2 3]
        tables  (map key->table-idx sample-txn-keys)
        hits-g1 (some keys-in-group1 tables)
        hits-g2 (some keys-in-group2 tables)]
    (is hits-g1 "sample 4-key txn must touch at least one table in group 1")
    (is hits-g2 "sample 4-key txn must touch at least one table in group 2")))
