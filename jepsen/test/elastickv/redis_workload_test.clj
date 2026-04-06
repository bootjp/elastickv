(ns elastickv.redis-workload-test
  (:require [clojure.test :refer :all]
            [elastickv.cli :as cli]))

(deftest fail-on-invalid-passes-through-valid-results
  (let [result {:valid? true}]
    (is (= result (cli/fail-on-invalid! result)))))

(deftest fail-on-invalid-throws-for-invalid-results
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"Jepsen analysis invalid"
       (cli/fail-on-invalid! {:valid? false}))))
