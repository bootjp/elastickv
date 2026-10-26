(ns elastickv.redis-workload-test
  (:require [clojure.test :refer :all]
            [elastickv.redis-workload :as workload]
            [jepsen.redis.client :as rc]))

(deftest complete-mops-parses-reads
  (let [op     {:type :invoke
                :f     :txn
                :value [[:r 1 nil] [:append 1 3]]}
        result (#'workload/complete-mops-or-error
                nil
                op
                [[:r 1 ["1" "2"]] [:append 1 3]])]
    (is (= :ok (:type result)))
    (is (= [[:r 1 [1 2]] [:append 1 3]] (:value result)))))

(deftest exec-array-error-becomes-info
  (let [op     {:type :invoke
                :f     :txn
                :value [[:r 1 nil] [:append 1 3]]}
        err    (ex-info "NOTLEADER leader not found" {:prefix :notleader})
        result (#'workload/complete-exec-results
                nil
                op
                [["1"] err])]
    (is (= :info (:type result)))
    (is (= [:exec-array-error :notleader] (:error result)))
    (is (= (:value op) (:value result)))))

(deftest notleader-reply-after-dispatch-remains-indeterminate
  (let [op     {:type :invoke :f :txn :value [[:append 6 9]]}
        raw    (rc/with-exceptions op #{}
                 (throw (ex-info "NOTLEADER leadership lost" {:prefix :notleader})))
        result (#'workload/preserve-unknown-leader-outcome raw)]
    (is (= :fail (:type raw)))
    (is (= :info (:type result)))
    (is (= :notleader (:error result)))
    (is (= (:value op) (:value result)))))
