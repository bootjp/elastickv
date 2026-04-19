(ns elastickv.jepsen-test
  (:gen-class)
  (:require [elastickv.redis-workload :as redis-workload]
            [elastickv.redis-zset-safety-workload :as zset-safety-workload]
            [elastickv.dynamodb-workload :as dynamodb-workload]
            [elastickv.s3-workload :as s3-workload]
            [jepsen.cli :as cli]))

(defn elastickv-test []
  (redis-workload/elastickv-redis-test {}))

(defn elastickv-dynamodb-test []
  (dynamodb-workload/elastickv-dynamodb-test {}))

(defn elastickv-s3-test []
  (s3-workload/elastickv-s3-test {}))

(defn elastickv-zset-safety-test []
  (zset-safety-workload/elastickv-zset-safety-test {}))

(defn -main
  [& args]
  (cli/run! (cli/single-test-cmd {:test-fn elastickv-test}) args))
