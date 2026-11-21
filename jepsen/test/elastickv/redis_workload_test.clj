(ns elastickv.redis-workload-test
  (:require [clojure.test :refer :all]
            [elastickv.redis-workload :as workload]
            [elastickv.elle-checker :as elle]
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

(deftest exec-replies-preserve-every-requested-operation
  (let [op {:type :invoke :f :txn :value [[:r 0 nil] [:append 1 7]]}
        reply #(#'workload/complete-exec-results nil op %)]
    (doseq [values [[["1"]] [["1"] 1 2] nil]]
      (let [bad (reply values)]
        (is (= (:value op) (:value bad)))
        (is (false? (:valid? (elle/check-exec-replies {:valid? true} [bad]))))))
    (let [mixed (reply [["99"] (ex-info "NOTLEADER" {})])]
      (is (= [{:index 0 :operation [:r 0 nil] :reply ["99"]}] (:exec-observations mixed)))
      (is (= :unknown (:valid? (elle/check-exec-replies {:valid? true} [mixed])))))))

(deftest invalid-and-mixed-replies-survive-offline-replay
  (let [call {:type :invoke :f :txn :process 0 :value [[:append 0 7] [:r 0 nil]]}
        call2 (assoc-in call [:value 0 2] 8)
        call3 (assoc-in call [:value 0 2] 10)
        reply #(#'workload/complete-exec-results nil %1 %2)
        bad (reply call [nil ["7"]])
        partial (reply call2 [1 (ex-info "NOTLEADER" {})])
        short (reply call3 [1])
        ddb-call (assoc call :value [[:append 1 9]])
        ddb-info (assoc ddb-call :type :info :write-snapshot {1 nil})
        h [call bad call2 partial call3 short ddb-call ddb-info]
        dir "target/test-exec-evidence"
        r (elle/analyze-history! :list-append h dir)
        reread (elle/read-history (str dir "/elle-history.edn"))]
    (is (= [0] (:invalid-exec-slots bad)))
    (is (= [1] (mapv :reply (:exec-observations short))))
    (is (= 2 (count (:invalid-exec-replies r))))
    (is (= 3 (count (:partial-exec-replies r))))
    (is (= {1 nil} (:write-snapshot (last reread))))
    (is (= (mapv :exec-observations h) (mapv :exec-observations reread)))
    (is (false? (:valid? (elle/check-history :list-append reread dir))))))

(deftest one-ambiguous-slot-cannot-hide-a-definitive-wrongtype
  (let [call {:type :invoke :f :txn :process 0 :value [[:append 1 7] [:append 2 8]]}
        errors (mapv #(ex-info % {}) ["NOTLEADER" "WRONGTYPE"])
        reply (#'workload/complete-exec-results nil call errors)
        read-op {:type :ok :f :txn :process 1 :value [[:r 2 [8]]]}]
    (is (= [:notleader :wrongtype] (mapv :error (:exec-errors reply))))
    (is (= :unknown (:valid? (elle/check-exec-replies {:valid? true} [call reply]))))
    (is (false? (:valid? (elle/check-exec-replies {:valid? true} [call reply read-op]))))))

(deftest rpush-lengths-must-match-one-serial-history
  (let [call {:type :invoke :f :txn :process 0 :value [[:append 0 7] [:append 0 8]]}
        check (fn [lengths]
                (let [reply (#'workload/complete-exec-results nil call lengths)]
                  (:valid? (elle/check-exec-replies {:valid? true} [call reply]))))]
    (is (false? (check [1 1])))
    (is (true? (check [1 2])))))
