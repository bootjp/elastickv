(ns elastickv.dynamodb-types-workload-test
  (:require [clojure.test :refer :all]
            [jepsen.client :as client]
            [elastickv.dynamodb-types-workload :as workload]))

(deftest builds-test-spec-for-each-type
  (doseq [t workload/value-type-keys]
    (let [test-map (workload/elastickv-dynamodb-types-test {:value-type t})]
      (is (map? test-map) (str "test-map for " (name t) " is a map"))
      (is (= (str "elastickv-dynamodb-type-" (name t)) (:name test-map))
          (str "test name for " (name t)))
      (is (some? (:client test-map)) (str "client for " (name t)))
      (is (some? (:checker test-map)) (str "checker for " (name t)))
      (is (some? (:generator test-map)) (str "generator for " (name t))))))

(deftest unknown-value-type-throws
  (is (thrown? clojure.lang.ExceptionInfo
        (workload/elastickv-dynamodb-types-test {:value-type :nope}))))

(deftest custom-options-override-defaults
  (let [test-map (workload/elastickv-dynamodb-types-test
                   {:value-type  :number
                    :time-limit  60
                    :concurrency 20
                    :dynamo-port 9000})]
    (is (= 20 (:concurrency test-map)))))

(deftest host-override-creates-client
  (let [test-map (workload/elastickv-dynamodb-types-test
                   {:value-type  :string
                    :dynamo-host "127.0.0.1"
                    :node->port  {"n1" 8000 "n2" 8001}})
        c        (:client test-map)
        opened   (client/open! c test-map "n1")]
    (is (some? (:ddb opened)))))

;; Each type's encode -> decode round-trip must be lossless and produce the
;; canonical form the register checker compares against.
(deftest encode-decode-round-trips
  (doseq [t workload/value-type-keys]
    (let [{:keys [encode decode gen]} (get @#'workload/type-specs t)
          v (gen 7)]
      (is (= v (decode (encode v)))
          (str "round-trip for " (name t) " value " (pr-str v))))))

(deftest distinct-table-per-type
  (let [tables (map #(:table (get @#'workload/type-specs %))
                    workload/value-type-keys)]
    (is (= (count tables) (count (set tables)))
        "every type uses a distinct table name")))
