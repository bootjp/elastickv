(ns elastickv.s3-workload-test
  (:require [clojure.test :refer :all]
            [jepsen.client :as client]
            [elastickv.cli :as cli]
            [elastickv.s3-workload :as workload]))

(deftest builds-test-spec
  (let [test-map (workload/elastickv-s3-test {})]
    (is (map? test-map))
    (is (= "elastickv-s3-register" (:name test-map)))
    (is (= ["n1" "n2" "n3" "n4" "n5"] (:nodes test-map)))))

(deftest custom-options-override-defaults
  (let [test-map (workload/elastickv-s3-test
                   {:time-limit  60
                    :concurrency 10
                    :s3-port     9999})]
    (is (= 10 (:concurrency test-map)))))

(deftest host-override-uses-provided-host
  (let [test-map (workload/elastickv-s3-test
                   {:s3-host "127.0.0.1"
                    :node->port {"n1" 9000 "n2" 9001}})
        c        (:client test-map)
        opened   (client/open! c test-map "n1")]
    (is (re-find #"http://127\.0\.0\.1:9000" (:url opened)))))
