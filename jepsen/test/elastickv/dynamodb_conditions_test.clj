(ns elastickv.dynamodb-conditions-test
  (:require [clojure.test :refer :all]
            [cognitect.aws.client.api :as aws]
            [elastickv.dynamodb-conditions :as conditions]
            [elastickv.elle-checker :as elle]
            [elastickv.dynamodb-workload :as single]
            [elastickv.dynamodb-multi-table-workload :as multi]
            [jepsen.client :as client]
            [jepsen.checker :as checker]
            [jepsen.history :as history]))

(defn txn [p type & mops] {:process p :f :txn :type type :value (vec mops)})

(deftest condition-rejections-through-both-clients
  (doseq [build [single/dynamodb-append-workload multi/dynamodb-append-multi-table-workload]
          [error details specific?]
          [["TransactionCanceledException" {:message "conditional check failed"} false]
           ["ConditionalCheckFailedException" {} false]
           ["TransactionCanceledException" {:CancellationReasons [{:Code "ConditionalCheckFailed"}
                                                                 {:Code "None"}]} true]]]
    (let [{:keys [client checker]} (build {})
          ;; Read-only key 0 and updated key 1 both carry snapshot conditions.
          call (txn 0 :invoke [:r 0 nil] [:append 1 2])
          response (fn [_ {:keys [op request retriable?]}]
                     (case op
                       :TransactGetItems {:Responses [{:Item {:val {:L [{:N "1"}]}}}
                                                       {:Item {:val {:L [{:N "1"}]}}}]}
                       :TransactWriteItems
                       (do (is (= ["#v = :cv" "#v = :cv"]
                                  (mapv (comp :ConditionExpression val first) (:TransactItems request))))
                           (is (false? (retriable? {})))
                           (merge {:cognitect.anomalies/category :cognitect.anomalies/incorrect
                                   :__type error} details))))
          reply (with-redefs [aws/invoke response] (client/invoke! client {} call))
          valid? (fn [& events]
                   (true? (:valid? (conditions/condition-verdict
                                    (concat [(txn 7 :invoke [:append 0 1] [:append 1 1])
                                             (txn 7 :ok [:append 0 1] [:append 1 1])] events)))))
          writer (fn [k type v] (txn 1 type [:append k v]))]
      (is (= :fail (:type reply)))
      (is (= {0 [1] 1 [1]} (:condition-snapshot reply)))
      (is (= (when specific? [0]) (:failed-keys reply)))
      (is (false? (valid? call reply)))
      (is (false? (valid? (writer 0 :invoke 3) (writer 0 :ok 3) call reply)))
      (is (false? (valid? call reply (writer 0 :invoke 3) (writer 0 :ok 3))))
      (is (false? (valid? call (writer 2 :invoke 3) reply)))
      (is (false? (valid? call (writer 0 :invoke 3) reply (writer 0 :fail 3))))
      (is (valid? call (writer 0 :invoke 3) reply (writer 0 :ok 3)))
      (is (valid? (writer 0 :invoke 3) (writer 0 :info 3) call reply))
      (is (= (not specific?) (valid? call (writer 1 :invoke 3) reply)))
      ;; When another writer explains the rejection, Elle must still reject
      ;; visibility of the canceled append itself.
      (let [history (history/history [(txn 2 :invoke [:append 1 1]) (txn 2 :ok [:append 1 1])
                                      call (writer 0 :invoke 3) reply (writer 0 :ok 3)
                                      (txn 2 :invoke [:r 1 nil]) (txn 2 :ok [:r 1 [1 2]])])
            r (checker/check checker {:name "canceled-append" :start-time (java.time.OffsetDateTime/now)}
                             history {})]
        (is (false? (get-in r [:conditions :valid?])))
        (is (false? (:valid? r)))
        (is (some #{:G1a} (:anomaly-types r)))))))

(deftest ambiguous-write-outcome
  (doseq [build [single/dynamodb-append-workload multi/dynamodb-append-multi-table-workload]]
    (let [c (:client (build {}))]
      (with-redefs [aws/invoke (fn [_ {:keys [op]}]
                                (if (= :TransactGetItems op)
                                  {:Responses [{}]}
                                  {:cognitect.anomalies/category :cognitect.anomalies/unavailable}))]
        (let [result (client/invoke! c {} (txn 0 :invoke [:r 0 nil] [:append 0 2]))]
          (is (= :info (:type result)))
          (is (nil? (:condition-snapshot result))))))))

(deftest all-observations-need-one-serialization
  (let [init [(txn 0 :invoke [:append 0 1]) (txn 0 :ok [:append 0 1])]
        start (txn 1 :invoke [:append 0 7])
        ack (assoc start :type :ok)
        read! (fn [v] [(txn 2 :invoke [:r 0 nil]) (txn 2 :ok [:r 0 v])])
        rejection (fn [v] (let [call (txn 3 :invoke [:r 0 nil] [:append 0 v])]
                            [call (assoc call :type :fail :failed-keys [0] :condition-snapshot {0 [1]})]))]
    (doseq [[expected tail] [[false (concat [start] (rejection 8) (read! [1]) [ack])]
                             [false (concat [start] (rejection 8) (rejection 9) [ack])]
                             [true (concat [start] (rejection 8) [ack] (read! [1 7]))]
                             [false (concat [start (assoc ack :type :info)] (rejection 8) (read! [1]))]
                             [true (concat [start (assoc ack :type :info)] (rejection 8) (read! [1 7]))]]]
      (let [h (history/history (concat init tail))]
        (is (= expected (:valid? (conditions/condition-verdict h))))
        (is (= expected (:valid? (elle/check-history :list-append h "target/condition-replay"))))))))

(deftest failed-snapshot-never-dispatches-an-append
  (doseq [build [single/dynamodb-append-workload multi/dynamodb-append-multi-table-workload]]
    (let [calls (atom [])
          c (:client (build {}))
          op (txn 1 :invoke [:r 0 nil] [:append 0 7])
          reply (with-redefs [aws/invoke (fn [_ req] (swap! calls conj (:op req))
                                           {:cognitect.anomalies/category :cognitect.anomalies/unavailable})]
                  (client/invoke! c {} op))]
      (is (= [:TransactGetItems] @calls))
      (is (= [:fail :transact-get] ((juxt :type :phase) reply))))))

(deftest pre-read-constrains-uncertain-and-denied-writes
  (doseq [build [single/dynamodb-append-workload multi/dynamodb-append-multi-table-workload]
          error ["AccessDeniedException" "aws#AccessDeniedException" "InternalServerError"]
          [prior expected] [[nil false] [[1] true]]]
    (let [{:keys [client checker]} (build {})
          call (txn 1 :invoke [:r 0 nil] [:append 0 7])
          reply (with-redefs [aws/invoke (fn [_ {:keys [op]}]
                                          (if (= op :TransactGetItems)
                                            {:Responses [(if prior {:Item {:val {:L [{:N "1"}]}}} {})]}
                                            {:cognitect.anomalies/category :cognitect.anomalies/fault :__type error}))]
                  (client/invoke! client {} call))
          h (history/history [(txn 0 :invoke [:append 0 1]) (txn 0 :ok [:append 0 1]) call reply
                              (txn 2 :invoke [:r 0 nil]) (txn 2 :ok [:r 0 [1]])])
          r (checker/check checker {:name "pre-read" :start-time (java.time.OffsetDateTime/now)} h {})]
      (is (= (if (= error "InternalServerError") :info :fail) (:type reply)))
      (is (= expected (:valid? r))))))

(deftest truncated-pre-read-is-a-protocol-error
  (doseq [build [single/dynamodb-append-workload multi/dynamodb-append-multi-table-workload]]
    (let [c (:client (build {}))
          call (txn 0 :invoke [:r 0 nil] [:append 1 2])
          reply (with-redefs [aws/invoke (fn [_ _] {:Responses [{}]})] (client/invoke! c {} call))]
      (is (= [:fail :transact-get] ((juxt :type :phase) reply)))
      (is (false? (:valid? (conditions/check-result {:valid? true} [call reply])))))))

(deftest predicate-rejection-must-name-a-guarded-key
  (let [h [(txn 0 :invoke [:append 2 9]) (txn 0 :ok [:append 2 9])
           (txn 1 :invoke [:append 0 3])
           (assoc (txn 1 :fail [:append 0 3]) :condition-snapshot {0 nil} :failed-keys [2])]]
    (is (false? (:valid? (conditions/condition-verdict h))))
    (is (= :unknown (:valid? (binding [elastickv.history-windows/*candidate-budget* 0]
                               (conditions/condition-verdict h)))))))
