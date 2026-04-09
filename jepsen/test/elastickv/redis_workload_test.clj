(ns elastickv.redis-workload-test
  (:require [clojure.test :refer :all]
            [elastickv.cli :as cli]))

(deftest fail-on-invalid-passes-through-valid-results
  (let [result {:results {:valid? true}}]
    (is (= result (cli/fail-on-invalid! result)))))

(deftest fail-on-invalid-throws-for-invalid-results
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"Jepsen analysis invalid"
       (cli/fail-on-invalid! {:results {:valid? false}}))))

(deftest fail-on-invalid-throws-for-unknown-results
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"Jepsen analysis invalid"
       (cli/fail-on-invalid! {:results {:valid? :unknown}}))))
