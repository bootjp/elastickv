(ns elastickv.dynamodb-workload-test
  (:require [clojure.test :refer :all]
            [elastickv.dynamodb-workload :as workload]))

(deftest builds-test-spec
  (let [test-map (workload/elastickv-dynamodb-test {})]
    (is (map? test-map))
    (is (= "elastickv-dynamodb-append" (:name test-map)))
    (is (= ["n1" "n2" "n3" "n4" "n5"] (:nodes test-map)))))

(deftest fail-on-invalid-passes-through-valid-results
  (let [result {:valid? true}]
    (is (= result (workload/fail-on-invalid! result)))))

(deftest fail-on-invalid-throws-for-invalid-results
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"Jepsen analysis invalid"
       (workload/fail-on-invalid! {:valid? false}))))

(deftest custom-options-override-defaults
  (let [test-map (workload/elastickv-dynamodb-test
                   {:time-limit 60
                    :concurrency 10
                    :dynamo-port 9000})]
    (is (= 10 (:concurrency test-map)))))
