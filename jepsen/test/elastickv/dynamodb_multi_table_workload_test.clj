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

(def ^:private key->table-idx       (var-get #'workload/key->table-idx))
(def ^:private key->table-name      (var-get #'workload/key->table-name))
(def ^:private key->pk              (var-get #'workload/key->pk))
(def ^:private distinct-group-ids   (var-get #'workload/distinct-group-ids))
(def ^:private default-grpc-host-port-for (var-get #'workload/default-grpc-host-port-for))

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

(deftest key-routing-pks-disambiguate-colliding-keys
  ;; k=0 and k=4 land on the same table (t1) but must have distinct pks
  ;; so each Elle key maps to a unique storage location.
  (is (= "0" (key->pk 0)))
  (is (= "1" (key->pk 4)))
  (is (= "2" (key->pk 8)))
  (is (not= (key->pk 0) (key->pk 4))
      "colliding-table keys must have distinct pks"))

(deftest rejects-key-count-not-divisible-by-num-tables
  ;; The workload's distribution guarantee depends on key-count % N
  ;; = 0.  A CLI invocation that passes --key-count 10 (or any value
  ;; that splits unevenly across 4 tables) would silently leave some
  ;; tables with fewer keys, breaking the multi-group-span invariant.
  ;; The construction-time assertion surfaces this loudly
  ;; (claude[bot] low on PR #916).
  (is (thrown? AssertionError
        (workload/dynamodb-append-multi-table-workload {:key-count 10}))
      "key-count not divisible by 4 must fail-fast at construction")
  (is (thrown? AssertionError
        (workload/dynamodb-append-multi-table-workload {:key-count 7}))
      "odd key-count must fail-fast"))

(deftest accepts-divisible-key-counts
  ;; Sanity-check that the assertion does NOT reject valid values.
  (is (map? (workload/dynamodb-append-multi-table-workload {:key-count 4})))
  (is (map? (workload/dynamodb-append-multi-table-workload {:key-count 8})))
  (is (map? (workload/dynamodb-append-multi-table-workload {:key-count 16}))))

(deftest multi-op-txn-spans-multiple-groups
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

;; ---------------------------------------------------------------------------
;; M5a setup-hook verification — distinct-group-ids regex
;; ---------------------------------------------------------------------------

(deftest distinct-group-ids-extracts-multi-group-routing
  ;; The M5a-required shape: >=2 distinct raft_group_id values present.
  (let [json (str "{\"catalog_version\":7,\"routes\":["
                  "{\"route_id\":100,\"raft_group_id\":1,\"start\":\"\",\"end\":\"\",\"state\":\"ROUTE_STATE_ACTIVE\"},"
                  "{\"route_id\":101,\"raft_group_id\":2,\"start\":\"\",\"end\":\"\",\"state\":\"ROUTE_STATE_ACTIVE\"}"
                  "]}")]
    (is (= #{1 2} (distinct-group-ids json))
        "two routes on distinct groups must yield #{1 2}")))

(deftest distinct-group-ids-collapses-single-group
  ;; Failure shape: launch script ran with --shardRanges but no
  ;; --raftGroups (or default single-group fallback).
  (let [json (str "{\"catalog_version\":1,\"routes\":["
                  "{\"route_id\":100,\"raft_group_id\":1,\"start\":\"\",\"end\":\"\",\"state\":\"ROUTE_STATE_ACTIVE\"},"
                  "{\"route_id\":101,\"raft_group_id\":1,\"start\":\"\",\"end\":\"\",\"state\":\"ROUTE_STATE_ACTIVE\"}"
                  "]}")]
    (is (= #{1} (distinct-group-ids json))
        "two routes on the same group must yield #{1} — verify-multi-group-routing! must reject this")))

(deftest distinct-group-ids-empty-on-missing-shape
  ;; A future ListRoutes schema change that renames raft_group_id
  ;; would make the regex return nothing — the empty set causes
  ;; verify-multi-group-routing! to throw, surfacing the schema
  ;; drift loudly rather than silently passing.
  (is (= #{} (distinct-group-ids "{\"catalog_version\":7,\"routes\":[]}"))
      "no routes must yield empty set")
  (is (= #{} (distinct-group-ids "{\"unrelated\":true}"))
      "missing routes field must yield empty set"))

(deftest default-grpc-host-port-resolves-from-first-node
  ;; Distributed Jepsen runs configure :nodes with real hostnames
  ;; (e.g. ["n1" "n2" "n3"]).  default-grpc-host-port-for must
  ;; derive the --address from the first node so the setup-hook
  ;; doesn't punt every distributed run to localhost (gemini
  ;; medium on PR #925).
  (is (= "n1:50051" (default-grpc-host-port-for {:nodes ["n1" "n2" "n3"]}))
      "first node hostname must form the default --address")
  (is (= "alpha.internal:50051"
         (default-grpc-host-port-for {:nodes ["alpha.internal" "beta.internal"]}))
      "FQDN-style nodes must round-trip cleanly")
  (is (= "n1:50051" (default-grpc-host-port-for {:nodes [:n1 :n2]}))
      "keyword node ids must be coerced via (name)"))

(deftest default-grpc-host-port-falls-back-on-empty-nodes
  ;; The pre-existing 127.0.0.1:50051 default is the right fallback
  ;; for test maps with no :nodes key (the workload-builder unit
  ;; tests under this file, for one).
  (is (= "127.0.0.1:50051" (default-grpc-host-port-for {})))
  (is (= "127.0.0.1:50051" (default-grpc-host-port-for {:nodes []}))))

(deftest distinct-group-ids-handles-whitespace
  ;; The CLI pretty-prints with two-space indent; the regex must
  ;; tolerate \s* between the key, colon, and value.
  (let [json "{\n  \"routes\": [\n    {\n      \"raft_group_id\":  3,\n      \"route_id\": 1\n    }\n  ]\n}"]
    (is (= #{3} (distinct-group-ids json))
        "whitespace and newlines between key/colon/value must be tolerated")))
