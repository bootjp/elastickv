(defproject elastickv-jepsen "0.1.0-SNAPSHOT"
  :description "Jepsen tests for Elastickv"
  :repositories [["clojars" {:url "https://repo.clojars.org"}]]
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen "0.3.5"]
                 [redis.clients/jedis "5.1.0"]]
  :main elastickv.jepsen-test)
