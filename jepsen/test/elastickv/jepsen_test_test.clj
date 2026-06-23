(ns elastickv.jepsen-test-test
  (:require [clojure.test :refer :all]
            [elastickv.jepsen-test :as jt]))

(deftest builds-test-spec
  (is (map? (jt/elastickv-test))))

(deftest selected-workloads-accept-option-map
  (doseq [test-fn [jt/elastickv-test
                   jt/elastickv-dynamodb-test
                   jt/elastickv-s3-test
                   jt/elastickv-zset-safety-test]]
    (is (map? (test-fn {})))))
