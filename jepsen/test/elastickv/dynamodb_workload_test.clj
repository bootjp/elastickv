(ns elastickv.dynamodb-workload-test
  (:require [clojure.test :refer :all]
            [jepsen.client :as client]
            [elastickv.dynamodb-workload :as workload]))

(deftest builds-test-spec
  (let [test-map (workload/elastickv-dynamodb-test {})]
    (is (map? test-map))
    (is (= "elastickv-dynamodb-append" (:name test-map)))
    (is (= ["n1" "n2" "n3" "n4" "n5"] (:nodes test-map)))))

(deftest custom-options-override-defaults
  (let [test-map (workload/elastickv-dynamodb-test
                   {:time-limit 60
                    :concurrency 10
                    :dynamo-port 9000})]
    (is (= 10 (:concurrency test-map)))))

(deftest host-override-creates-client
  ;; Verify that open! produces a DynamoDBClient with a live cognitect/aws-api
  ;; client object (not nil) when a host/port override is supplied.
  (let [test-map (workload/elastickv-dynamodb-test
                   {:dynamo-host "127.0.0.1"
                    :node->port  {"n1" 8000 "n2" 8001}})
        c        (:client test-map)
        opened   (client/open! c test-map "n1")]
    (is (some? (:ddb opened)))))

(deftest transaction-canceled-is-indeterminate
  (let [c  (workload/->DynamoDBClient {} ::ddb)
        op {:type :invoke
            :process 0
            :f :txn
            :value [[:append 1 7]
                    [:append 2 8]]}]
    (with-redefs [workload/dynamo-transact-get!   (fn [_ ks]
                                                    (zipmap ks (repeat nil)))
                  workload/dynamo-transact-write! (fn [& _]
                                                    (throw
                                                      (ex-info "DynamoDB TransactionCanceledException"
                                                               {:type "TransactionCanceledException"})))]
      (let [result (client/invoke! c {} op)]
        (is (= :info (:type result)))
        (is (= "TransactionCanceledException" (:error result)))
        (is (= (:value op) (:value result)))))))
