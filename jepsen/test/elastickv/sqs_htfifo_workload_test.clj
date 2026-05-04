(ns elastickv.sqs-htfifo-workload-test
  (:require [clojure.test :refer :all]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [elastickv.sqs-htfifo-workload :as workload]))

(deftest builds-test-spec
  (let [test-map (workload/elastickv-sqs-htfifo-test {})]
    (is (map? test-map))
    (is (= "elastickv-sqs-htfifo" (:name test-map)))
    (is (= ["n1" "n2" "n3" "n4" "n5"] (:nodes test-map)))))

(deftest custom-options-override-defaults
  (let [test-map (workload/elastickv-sqs-htfifo-test
                   {:time-limit      60
                    :concurrency     12
                    :sqs-port        12345
                    :partition-count 8
                    :group-count     12})]
    (is (= 12 (:concurrency test-map)))))

(deftest host-override-creates-client
  ;; Verify open! produces an HTFIFOClient with a live cognitect/aws-api
  ;; SQS client when a host/port override is supplied.
  (let [test-map (workload/elastickv-sqs-htfifo-test
                   {:sqs-host    "127.0.0.1"
                    :node->port  {"n1" 9324 "n2" 9325}})
        c        (:client test-map)
        opened   (client/open! c test-map "n1")]
    (is (some? (:sqs opened)))))

;; ---------------------------------------------------------------------------
;; Checker tests — pure-function pinning of the three contracts
;; ---------------------------------------------------------------------------

(defn- check-history
  "Run the workload's checker against a synthetic history and return the
   result map. Mirrors how Jepsen drives checker/check at the end of a run."
  [history]
  (checker/check (workload/ht-fifo-checker) {} history {}))

(defn- send-op [t group seq-num & {:keys [type] :or {type :ok}}]
  {:type type :f :send :time t :value [group seq-num]})

(defn- recv-op [t tuples & {:keys [type] :or {type :ok}}]
  {:type type :f :recv :time t :value tuples})

(deftest checker-clean-history-is-valid
  (let [hist [(send-op 100 "g0" 0)
              (send-op 200 "g0" 1)
              (send-op 300 "g1" 0)
              (recv-op 400 [["g0" 0] ["g0" 1]])
              (recv-op 500 [["g1" 0]])]
        r    (check-history hist)]
    (is (:valid? r))
    (is (= 3 (:committed-sends r)))
    (is (= 3 (:received r)))
    (is (empty? (:lost r)))
    (is (empty? (:duplicates r)))
    (is (empty? (:ordering-violations r)))))

(deftest checker-detects-loss
  ;; g0:1 sent OK but never received — must show up as :lost.
  (let [hist [(send-op 100 "g0" 0)
              (send-op 200 "g0" 1)
              (recv-op 400 [["g0" 0]])]
        r    (check-history hist)]
    (is (false? (:valid? r)))
    (is (= #{["g0" 1]} (:lost r)))))

(deftest checker-info-send-is-not-loss
  ;; A :send with :info status (network failure mid-send) is not counted
  ;; as :lost even if it never arrives — its commit status is undefined.
  (let [hist [(send-op 100 "g0" 0)
              (send-op 200 "g0" 1 :type :info)
              (recv-op 400 [["g0" 0]])]
        r    (check-history hist)]
    (is (:valid? r))
    (is (empty? (:lost r)))
    (is (= 1 (:in-flight-sends r)))))

(deftest checker-detects-duplicates
  ;; The same (group, seq) appearing twice in the receive history is a
  ;; FIFO contract violation (delete-side bug).
  (let [hist [(send-op 100 "g0" 0)
              (recv-op 200 [["g0" 0]])
              (recv-op 300 [["g0" 0]])]
        r    (check-history hist)]
    (is (false? (:valid? r)))
    (is (= #{["g0" 0]} (:duplicates r)))))

(deftest checker-detects-within-group-ordering-violation
  ;; g0 receives seq=1 BEFORE seq=0 — within-group ordering broken.
  (let [hist [(send-op 100 "g0" 0)
              (send-op 200 "g0" 1)
              (recv-op 300 [["g0" 1]])
              (recv-op 400 [["g0" 0]])]
        r    (check-history hist)]
    (is (false? (:valid? r)))
    (is (contains? (:ordering-violations r) "g0"))))

(deftest checker-cross-group-receives-do-not-violate-ordering
  ;; Different groups can interleave freely — only WITHIN-group ordering
  ;; is constrained. The receive sequence below is fine even though g0
  ;; and g1 messages alternate.
  (let [hist [(send-op 100 "g0" 0)
              (send-op 110 "g1" 0)
              (send-op 200 "g0" 1)
              (send-op 210 "g1" 1)
              (recv-op 300 [["g1" 0]])
              (recv-op 400 [["g0" 0]])
              (recv-op 500 [["g1" 1]])
              (recv-op 600 [["g0" 1]])]
        r    (check-history hist)]
    (is (:valid? r))
    (is (empty? (:ordering-violations r)))))

(deftest checker-failed-sends-are-not-counted
  ;; A :send op with :type :fail did not commit; it should not appear in
  ;; the committed-sends count and the receiver isn't expected to see it.
  (let [hist [(send-op 100 "g0" 0)
              (send-op 200 "g0" 1 :type :fail)
              (recv-op 400 [["g0" 0]])]
        r    (check-history hist)]
    (is (:valid? r))
    (is (= 1 (:committed-sends r)))
    (is (empty? (:lost r)))))

(deftest checker-empty-receives-do-not-pollute
  ;; A :recv that returned 0 messages is a no-op for the checker.
  (let [hist [(send-op 100 "g0" 0)
              (recv-op 200 [])
              (recv-op 300 [["g0" 0]])]
        r    (check-history hist)]
    (is (:valid? r))
    (is (= 1 (:received r)))))
