(ns elastickv.dynamodb-conditions-test
  (:require [clojure.test :refer :all]
            [cognitect.aws.client.api :as aws]
            [elastickv.dynamodb-conditions :as conditions]
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
          valid? (fn [& events] (empty? (conditions/unexplained-rejections events)))
          writer (fn [k type v] (txn 1 type [:append k v]))]
      (is (= :fail (:type reply)))
      (is (= {0 [1] 1 [1]} (:condition-snapshot reply)))
      (is (= (when specific? [0]) (:failed-keys reply)))
      (is (false? (valid? call reply)))
      (is (false? (valid? (writer 0 :invoke 3) (writer 0 :ok 3) call reply)))
      (is (false? (valid? call reply (writer 0 :invoke 3) (writer 0 :ok 3))))
      (is (false? (valid? call (writer 2 :invoke 3) reply)))
      (is (false? (valid? call (writer 0 :invoke 3) reply (writer 0 :fail 3))))
      (is (false? (valid? (writer 0 :invoke 1) call reply (writer 0 :ok 1))))
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
        (is (empty? (:unexplained-rejections r)))
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
