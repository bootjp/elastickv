(ns elastickv.sqs-htfifo-workload-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [elastickv.sqs-common :as sqs]
            [elastickv.sqs-htfifo-workload :as workload])
  (:import (java.util.concurrent CountDownLatch TimeUnit)))

(defn- await-channel! [ch]
  (let [[v port] (async/alts!! [ch (async/timeout 5000)])]
    (when (or (not= port ch) (nil? v))
      (throw (ex-info "Observation regression barrier timed out" {})))
    v))

(defn- barrier-history [reversed?]
  (let [test-map (workload/elastickv-sqs-htfifo-test {})
        base (assoc (:client test-map) :groups ["g1"]
                    :seq-counters (atom {"g1" 4 "other" 0}))
        slow-client (assoc base :sqs :slow)
        fast-client (assoc base :sqs :fast)
        deleted-four (async/chan 1)
        fast-completed (async/chan 1)
        slow-seq (if reversed? 5 4)
        fast-seq (if reversed? 4 5)
        slow-receipt (str "secret-" slow-seq)
        history (atom [])
        invoke! (fn [c p f]
                  (let [op {:f f :process p :type :invoke :time (System/nanoTime)}]
                    (swap! history conj op)
                    (let [result (assoc (client/invoke! c test-map op) :time (System/nanoTime))]
                      (swap! history conj result)
                      result)))]
    (with-redefs [sqs/invoke!
                  (fn [who action req]
                    (case action
                      :SendMessage {}
                      :ReceiveMessage
                      (if (= who :slow)
                        {:Messages [{:Body (str "g1:" slow-seq) :MessageId (str "message-" slow-seq) :ReceiptHandle slow-receipt}
                                    {:Body "other:0" :MessageId "message-other" :ReceiptHandle "secret-other"}]}
                        (do (await-channel! deleted-four)
                            {:Messages [{:Body (str "g1:" fast-seq) :MessageId (str "message-" fast-seq) :ReceiptHandle (str "secret-" fast-seq)}]}))
                      :DeleteMessage
                      (cond
                        (= slow-receipt (:ReceiptHandle req)) (do (async/>!! deleted-four true) {})
                        (= "secret-other" (:ReceiptHandle req)) (do (await-channel! fast-completed) {})
                        :else {})))]
      (invoke! slow-client 0 :send)
      (invoke! slow-client 0 :send)
      (invoke! (assoc slow-client :groups ["other"]) 0 :send)
      (let [finished (CountDownLatch. 2)
            slow (future (try (invoke! slow-client 3 :recv)
                              (finally (.countDown finished))))
            fast (future (try
                           (let [r (invoke! fast-client 4 :recv)]
                             (async/>!! fast-completed true)
                             r)
                           (finally (.countDown finished))))]
        (try
          (is (map? (deref fast 10000 nil)))
          (is (map? (deref slow 10000 nil)))
          (is (= [[["g1" fast-seq]] [["g1" slow-seq] ["other" 0]]]
                 (mapv :value (filter #(and (= :recv (:f %)) (= :ok (:type %))) @history))))
          {:test-map test-map :history @history}
          (finally
            (async/close! deleted-four)
            (async/close! fast-completed)
            (when-not (.await finished 10 TimeUnit/SECONDS)
              (future-cancel slow)
              (future-cancel fast)
              ;; Cancellation marks a Future done before its body exits.
              (.await finished))))))))

(deftest client-observes-delivery-before-unrelated-delete-completion
  (let [{:keys [test-map history]} (barrier-history false)
        result (checker/check (workload/ht-fifo-checker) test-map history {})]
    (is (true? (:valid? result)) (pr-str result))
    (is (= 3 (:raw-deliveries result)))
    (is (not (re-find #"secret-|message-4|message-5|message-other|ReceiptHandle|QueueUrl"
                      (pr-str history))))))

(deftest client-rejects-real-reversed-raw-delivery-despite-legal-completion-order
  (let [{:keys [test-map history]} (barrier-history true)
        result (checker/check (workload/ht-fifo-checker) test-map history {})]
    (is (false? (:valid? result)))
    (is (empty? (:observation-errors result)))
    (is (= {"g1" [{:prev 5 :curr 4}]} (:ordering-violations result)))))

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
    (try
      (is (some? (:sqs opened)))
      (finally (client/close! opened test-map)))))

;; ---------------------------------------------------------------------------
;; Checker tests — pure-function pinning of the three contracts
;; ---------------------------------------------------------------------------

(defn- check-history
  "Run the workload's checker against a synthetic history and return the
   result map. Mirrors how Jepsen drives checker/check at the end of a run."
  [history]
  (let [events (mapcat (fn [op]
                        (if (::point-send op)
                          [{:type :invoke :f :send :process (:process op) :time (:time op)}
                           (dissoc op ::point-send)]
                          [op])) history)]
    (checker/check (workload/ht-fifo-checker)
                   {:sqs-observation-clock-id "synthetic-clock"} events {})))

(defn- test-hash [s]
  (let [digest (.digest (java.security.MessageDigest/getInstance "SHA-256")
                        (.getBytes (str s) java.nio.charset.StandardCharsets/UTF_8))]
    (apply str (map #(format "%02x" (bit-and 255 %)) digest))))

(defn- send-op [t group seq-num & {:keys [type process] :or {type :ok process 0}}]
  ;; check-history expands test-only point sends into ordinary framework
  ;; invoke/completion pairs. The production checker has no fixture bypass.
  {::point-send true :type type :f :send :time t :process process :value [group seq-num]
   :sqs-observation (cond-> {:version 1 :clock-id "synthetic-clock"
                            :send-start t}
                      (= :ok type) (assoc :send-end t))})

(defn- recv-op [t tuples & {:keys [type process] :or {type :ok process 0}}]
  {:type type :f :recv :time (+ t 1 (* 2 (count tuples))) :process process
   :value (if (= :ok type) tuples [])
   :sqs-observation
   {:version 1 :clock-id "synthetic-clock" :receive-start (dec t) :received-at t
    :deliveries
    (mapv (fn [i tuple]
            (let [receipt (test-hash [t process i])]
              {:delivery-index i :tuple tuple :message-hash (test-hash tuple)
               :receipt-hash receipt
               :delete {:delivery-index i :receipt-hash receipt
                        :start (+ t 1 (* 2 i)) :end (+ t 2 (* 2 i)) :type type}}))
          (range) tuples)}})

(deftest checker-clean-history-is-valid
  (let [hist [(send-op 100 "g0" 0)
              (send-op 200 "g0" 1)
              (send-op 300 "g1" 0)
              (recv-op 400 [["g0" 0] ["g0" 1]])
              (recv-op 500 [["g1" 0]])]
        r    (check-history hist)]
    (is (true? (:valid? r)))
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
    (is (true? (:valid? r)))
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
    (is (true? (:valid? r)))
    (is (empty? (:ordering-violations r)))))

(deftest checker-failed-sends-are-not-counted
  ;; A :send op with :type :fail did not commit; it should not appear in
  ;; the committed-sends count and the receiver isn't expected to see it.
  (let [hist [(send-op 100 "g0" 0)
              (send-op 200 "g0" 1 :type :fail)
              (recv-op 400 [["g0" 0]])]
        r    (check-history hist)]
    (is (true? (:valid? r)))
    (is (= 1 (:committed-sends r)))
    (is (empty? (:lost r)))))

(deftest checker-empty-receives-do-not-pollute
  ;; A :recv that returned 0 messages is a no-op for the checker.
  (let [hist [(send-op 100 "g0" 0)
              (recv-op 200 [])
              (recv-op 300 [["g0" 0]])]
        r    (check-history hist)]
    (is (true? (:valid? r)))
    (is (= 1 (:received r)))))

(deftest checker-info-recv-is-ignored
  ;; A :recv with :info status (delete failed mid-batch) is dropped by
  ;; collect-receives. The message will be redelivered, the next :ok
  ;; recv records it, and the checker sees no duplicate.
  (let [hist [(send-op 100 "g0" 0)
              (recv-op 200 [["g0" 0]] :type :info)
              (recv-op 300 [["g0" 0]])]
        r    (check-history hist)]
    (is (true? (:valid? r)))
    (is (= 1 (:received r)))
    (is (empty? (:duplicates r)))))

(deftest checker-same-time-batch-preserves-within-group-order
  ;; A single :recv batch may carry multiple messages from the same
  ;; group with the SAME completion time. The checker must respect
  ;; the within-batch order (FIFO of the server's response) rather
  ;; than reordering them by sort-by stability alone.
  (let [hist [(send-op 100 "g0" 0)
              (send-op 110 "g0" 1)
              (send-op 120 "g0" 2)
              (recv-op 200 [["g0" 0] ["g0" 1] ["g0" 2]])]
        r    (check-history hist)]
    (is (true? (:valid? r)) (str "got " (pr-str r)))
    (is (empty? (:ordering-violations r)))))

(deftest checker-rejects-vacuous-history
  (is (false? (:valid? (check-history []))))
  (is (false? (:valid? (check-history [(send-op 100 "g0" 0 :type :info)])))))

(defn- interval-send [start returned group n & {:keys [type process] :or {type :ok process 0}}]
  [{:f :send :type :invoke :process process :time start}
   (-> (send-op returned group n :type type :process process)
       (dissoc ::point-send)
       (assoc-in [:sqs-observation :send-start] start))])

(deftest checker-allows-only-orders-consistent-with-send-real-time
  (doseq [[label sends tuples valid?]
          [["overlapping sends can arrive in reverse numerical order"
            (concat (interval-send 1 10 "g0" 0 :process 0)
                    (interval-send 2 3 "g0" 1 :process 1))
            [["g0" 1] ["g0" 0]] true]
           ["numeric sequence is not the send order"
            (concat (interval-send 1 2 "g0" 7)
                    (interval-send 3 4 "g0" 3))
            [["g0" 7] ["g0" 3]] true]
           ["nonoverlapping sends cannot be inverted"
            (concat (interval-send 1 2 "g0" 0)
                    (interval-send 3 4 "g0" 1))
            [["g0" 1] ["g0" 0]] false]
           ["equal endpoints do not prove precedence"
            (concat (interval-send 1 2 "g0" 0)
                    (interval-send 2 3 "g0" 1))
            [["g0" 1] ["g0" 0]] true]
           ["info completion is not a successful end"
            (concat (interval-send 1 2 "g0" 0 :type :info)
                    (interval-send 3 4 "g0" 1))
            [["g0" 1] ["g0" 0]] true]
           ["an info send still began after an earlier successful send"
            (concat (interval-send 1 2 "g0" 0)
                    (interval-send 3 4 "g0" 1 :type :info))
            [["g0" 1] ["g0" 0]] false]]]
    (testing label
      (let [r (check-history (concat (sort-by :time sends) [(recv-op 100 tuples)]))]
        (is (= valid? (:valid? r)) (pr-str r))
        (is (empty? (:observation-errors r)) (pr-str r))))))

(deftest checker-detects-nonadjacent-send-precedence-inversion
  ;; B overlaps C and C overlaps A, but B started after A's successful end.
  (let [sends (concat (interval-send 1 2 "g0" 0 :process 0)
                      (interval-send 3 10 "g0" 1 :process 1)
                      (interval-send 1 11 "g0" 2 :process 2))
        r (check-history (concat (sort-by :time sends)
                                 [(recv-op 100 [["g0" 1] ["g0" 2] ["g0" 0]])]))]
    (is (false? (:valid? r)))
    (is (= {"g0" [{:prev 1 :curr 0}]} (:ordering-violations r)))))

(deftest checker-keeps-every-raw-redelivery-in-ordering-evidence
  (let [sends [(send-op 1 "g0" 0) (send-op 2 "g0" 1)]
        r (check-history (concat sends [(recv-op 100 [["g0" 0]] :type :info)
                                         (recv-op 200 [["g0" 1]])
                                         (recv-op 300 [["g0" 0]])]))]
    (is (false? (:valid? r)))
    (is (= 3 (:raw-deliveries r)))
    (is (empty? (:duplicates r)))
    (is (= {"g0" [{:prev 1 :curr 0}]} (:ordering-violations r)))))

(deftest checker-rejects-wrong-order-within-one-raw-batch
  (let [r (check-history [(send-op 1 "g0" 0) (send-op 2 "g0" 1)
                          (recv-op 100 [["g0" 1] ["g0" 0]])])]
    (is (false? (:valid? r)))
    (is (empty? (:observation-errors r)))
    (is (= {"g0" [{:prev 1 :curr 0}]} (:ordering-violations r)))))

(deftest checker-rejects-malformed-observation-traces
  (let [send (send-op 1 "g0" 0)
        recv (recv-op 100 [["g0" 0]])
        path [:sqs-observation :deliveries 0]
        malformed
        {"missing observation" (dissoc recv :sqs-observation)
         "unknown outcome" (assoc recv :type :unrecognized)
         "foreign run nonce" (assoc-in recv [:sqs-observation :clock-id] "old-clock")
         "unsupported version" (assoc-in recv [:sqs-observation :version] 0)
         "missing raw response" (update recv :sqs-observation dissoc :received-at)
         "delivery before send" (-> recv
                                     (assoc-in [:sqs-observation :receive-start] 0)
                                     (assoc-in [:sqs-observation :received-at] 0))
         "wrong raw batch shape" (assoc-in recv [:sqs-observation :deliveries] {})
         "wrong delivery index" (assoc-in recv (conj path :delivery-index) 1)
         "malformed body" (assoc-in recv (conj path :tuple) nil)
         "missing message identity" (update-in recv path dissoc :message-hash)
         "missing receipt" (update-in recv path dissoc :receipt-hash)
         "missing delete" (update-in recv path dissoc :delete)
         "delete for another receipt" (assoc-in recv (conj path :delete :receipt-hash) (test-hash "old-receipt"))
         "delete for another delivery" (assoc-in recv (conj path :delete :delivery-index) 1)
         "delete before delivery" (assoc-in recv (conj path :delete :start) 90)
         "delete end before start" (assoc-in recv (conj path :delete :end) 90)
         "malformed delete time" (assoc-in recv (conj path :delete :end) "not-time")
         "fabricated acknowledgment" (assoc recv :value [["g0" 9]])
         "omitted acknowledgment" (assoc recv :value [])
         "non-vector acknowledgment" (assoc recv :value 42)
         "list acknowledgment" (assoc recv :value (list ["g0" 0]))
         "failed delete counted as acknowledged" (assoc-in recv (conj path :delete :type) :info)
         "unmatched delivery" (-> recv
                                  (assoc-in (conj path :tuple) ["g0" 9])
                                  (assoc :value [["g0" 9]]))}]
    (doseq [[label bad] malformed]
      (testing label
        (let [r (check-history [send bad])]
          (is (false? (:valid? r)) (pr-str r))
          (is (seq (:observation-errors r)) (pr-str r)))))
    (testing "receipt replay is not a new delivery"
      (let [again (-> (recv-op 200 [["g0" 0]])
                      (assoc-in (conj path :receipt-hash) (get-in recv (conj path :receipt-hash)))
                      (assoc-in (conj path :delete :receipt-hash) (get-in recv (conj path :receipt-hash))))
            r (check-history [send recv again])]
        (is (false? (:valid? r)))
        (is (some #{:receipt-identity-reused} (:observation-errors r)))))
    (testing "completion-only runtime sends cannot masquerade as point fixtures"
      (let [r (checker/check (workload/ht-fifo-checker)
                             {:sqs-observation-clock-id "synthetic-clock"} [send recv] {})]
        (is (false? (:valid? r)))
        (is (some #{:missing-send-invocation} (:observation-errors r)))))
    (testing "info send cannot carry a successful end"
      (let [r (check-history [(assoc send :type :info) recv])]
        (is (false? (:valid? r)))
        (is (some #(= :ambiguous-send-has-end (:reason %)) (:observation-errors r)))))))

(deftest checker-does-not-retrofit-legacy-histories-with-a-pass
  (let [history (mapv #(dissoc % :sqs-observation)
                      [(send-op 1 "g1" 4) (send-op 2 "g1" 5)
                       (recv-op 100 [["g1" 5]]) (recv-op 200 [["g1" 4]])])
        r (check-history history)]
    (is (not (true? (:valid? r))))
    (is (= 2 (:committed-sends r) (:received r)))
    (is (empty? (:lost r)))
    (is (seq (:observation-errors r)))))

(deftest client-records-malformed-raw-deliveries-instead-of-dropping-them
  (doseq [bad [{:Body "malformed" :MessageId "bad-id" :ReceiptHandle "bad-secret"}
               {:Body "g0:1" :MessageId "missing-receipt"}]]
    (let [test-map (workload/elastickv-sqs-htfifo-test {})
          c (assoc (:client test-map) :groups ["g0"] :sqs :mock)
          history (atom [])
          invoke! (fn [f]
                    (let [op {:f f :type :invoke :process 0 :time (System/nanoTime)}]
                      (swap! history conj op)
                      (let [r (assoc (client/invoke! c test-map op) :time (System/nanoTime))]
                        (swap! history conj r)
                        r)))]
      (with-redefs [sqs/invoke! (fn [_ action _]
                                (case action
                                  :SendMessage {}
                                  :ReceiveMessage {:Messages [{:Body "g0:0" :MessageId "good-id" :ReceiptHandle "good-secret"} bad]}
                                  :DeleteMessage {}))]
        (invoke! :send)
        (let [op (invoke! :recv)
              r (checker/check (workload/ht-fifo-checker) test-map @history {})]
          (is (= 2 (count (get-in op [:sqs-observation :deliveries]))))
          (is (false? (:valid? r)))
          (is (seq (:observation-errors r)))
          (is (not (re-find #"bad-secret|good-secret|good-id|bad-id|ReceiptHandle|QueueUrl"
                            (pr-str (:sqs-observation op))))))))))

(deftest client-validates-raw-response-shapes-after-other-acknowledgments
  (doseq [[response valid?] [[{} true] [{:Messages nil} true] [{:Messages []} true]
                            [{:Messages {}} false] [{:Messages 42} false]
                            [{:Messages "invalid"} false] [{:Messages #{}} false]
                            [nil false] [[] false] [42 false]]]
    (let [test-map (workload/elastickv-sqs-htfifo-test {})
          c (assoc (:client test-map) :groups ["g0"] :sqs :mock)
          history (atom [])
          receives (atom 0)
          invoke! (fn [f]
                    (let [op {:f f :type :invoke :process 0 :time (System/nanoTime)}]
                      (swap! history conj op)
                      (let [r (assoc (client/invoke! c test-map op) :time (System/nanoTime))]
                        (swap! history conj r)
                        r)))]
      (with-redefs [sqs/invoke!
                    (fn [_ action _]
                      (case action
                        :SendMessage {}
                        :DeleteMessage {}
                        :ReceiveMessage
                        (if (= 1 (swap! receives inc))
                          {:Messages [{:Body "g0:0" :MessageId "good-id" :ReceiptHandle "good-secret"}]}
                          response)))]
        (invoke! :send)
        (invoke! :recv)
        (is (true? (:valid? (checker/check (workload/ht-fifo-checker) test-map @history {}))))
        (let [op (invoke! :recv)
              r (checker/check (workload/ht-fifo-checker) test-map @history {})]
          (is (integer? (get-in op [:sqs-observation :received-at]))
              "SDK return must be recorded before parsing can fail")
          (is (= valid? (:valid? r)) (pr-str response))
          (is (= 1 (:committed-sends r) (:received r)))
          (is (empty? (:lost r)))
          (is ((if valid? empty? seq) (:observation-errors r)))
          (is (not (re-find #"good-secret|good-id|ReceiptHandle" (pr-str op)))))))))

(deftest client-preserves-ambiguous-send-classification-without-an-end
  (doseq [[data expected-error]
          [[{:category :cognitect.anomalies/unavailable} :network-error]
           [{:type "InternalFailure"} "InternalFailure"]
           [{:type "Throttling"} "Throttling"]
           [{:category :cognitect.anomalies/unsupported} :cognitect.anomalies/unsupported]]]
    (let [test-map (workload/elastickv-sqs-htfifo-test {})
          c (assoc (:client test-map) :groups ["g0"] :sqs :mock)]
      (with-redefs [sqs/invoke! (fn [& _] (throw (ex-info "transport failed" data)))]
        (let [r (client/invoke! c test-map {:type :invoke :f :send :process 0})]
          (is (= :info (:type r)))
          (is (= expected-error (:error r)))
          (is (= ["g0" 0] (:value r)))
          (is (integer? (get-in r [:sqs-observation :send-start])))
          (is (not (contains? (:sqs-observation r) :send-end))))))))

(deftest client-does-not-record-unexpected-exception-payloads
  (doseq [action [:SendMessage :ReceiveMessage :DeleteMessage]
          failure [(RuntimeException. "secret-marker receipt=plaintext body=payload")
                   (ex-info "secret-marker receipt=plaintext body=payload"
                            {:resp {:ReceiptHandle "secret-marker"}})]]
    (let [test-map (workload/elastickv-sqs-htfifo-test {})
          c (assoc (:client test-map) :groups ["g0"] :sqs :mock)]
      (with-redefs [sqs/invoke! (fn [_ actual _]
                                (when (= action actual) (throw failure))
                                (case actual
                                  :ReceiveMessage {:Messages [{:Body "g0:0" :MessageId "message-id"
                                                             :ReceiptHandle "secret-marker"}]}
                                  {}))]
        (let [r (client/invoke! c test-map {:type :invoke :process 0
                                           :f (if (= action :SendMessage) :send :recv)})]
          ;; ExceptionInfo delete failures retain the existing per-delete
          ;; handling; unexpected RuntimeExceptions remain outer :info.
          (is (= (if (and (= action :DeleteMessage)
                          (instance? clojure.lang.ExceptionInfo failure)) :ok :info)
                 (:type r)))
          (is (not (re-find #"secret-marker|plaintext|payload|ReceiptHandle|message-id" (pr-str r))))
          (when (= :info (:type r))
            (is (= :unexpected-client-error (:error r)))))))))
