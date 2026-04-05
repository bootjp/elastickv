(defproject elastickv-jepsen "0.1.0-SNAPSHOT"
  :description "Jepsen tests for Elastickv"
  :repositories [["clojars" {:url "https://repo.clojars.org"}]]
  :source-paths ["src" "redis/src"]
  :jvm-opts ["-Xmx4g" "-Djava.awt.headless=true"]
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen "0.3.11"]
                 [com.taoensso/carmine "3.5.0"]
                 [slingshot "0.12.2"]
                 [redis.clients/jedis "5.1.0" :exclusions [org.slf4j/slf4j-api]]
                 [clj-http "3.13.1"]
                 [cheshire "5.13.0"]
                 [org.slf4j/slf4j-nop "2.0.9"]]
  :main elastickv.jepsen-test)
