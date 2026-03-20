(ns elastickv.redis-workload-test
  (:require [clojure.test :refer :all]
            [elastickv.redis-workload :as workload]))

(deftest fail-on-invalid-passes-through-valid-results
  (let [result {:valid? true}]
    (is (= result (workload/fail-on-invalid! result)))))

(deftest fail-on-invalid-throws-for-invalid-results
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"Jepsen analysis invalid"
       (workload/fail-on-invalid! {:valid? false}))))
