(ns elastickv.jepsen-test
  (:gen-class)
  (:require [elastickv.redis-workload :as redis-workload]
            [elastickv.dynamodb-workload :as dynamodb-workload]
            [elastickv.dynamodb-types-workload :as dynamodb-types-workload]
            [elastickv.s3-workload :as s3-workload]
            [elastickv.sqs-htfifo-workload :as sqs-htfifo-workload]
            [jepsen.cli :as cli]))

(defn elastickv-test []
  (redis-workload/elastickv-redis-test {}))

(defn elastickv-dynamodb-test []
  (dynamodb-workload/elastickv-dynamodb-test {}))

(defn elastickv-dynamodb-types-test
  ([] (elastickv-dynamodb-types-test {}))
  ([opts] (dynamodb-types-workload/elastickv-dynamodb-types-test opts)))

(defn elastickv-s3-test []
  (s3-workload/elastickv-s3-test {}))

(defn elastickv-sqs-htfifo-test
  "HT-FIFO Jepsen test (PR 7b). Run via the workload's own -main:
   `lein run -m elastickv.sqs-htfifo-workload [opts]`. Same pattern
   as elastickv-dynamodb-test / elastickv-s3-test — each workload
   exposes its own -main so this -main only dispatches Redis."
  ([] (elastickv-sqs-htfifo-test {}))
  ([opts] (sqs-htfifo-workload/elastickv-sqs-htfifo-test opts)))

(defn -main
  [& args]
  (cli/run! (cli/single-test-cmd {:test-fn elastickv-test}) args))
