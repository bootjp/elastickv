(ns elastickv.grpc-workload
  "Public RawKV and TransactionalKV point operations over actual gRPC."
  (:gen-class)
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [elastickv.cli :as cli]
            [elastickv.elle-checker :as elle]
            [elastickv.redis-workload :as redis]
            [elastickv.s3-workload :as s3]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.independent :as independent])
  (:import [java.lang ProcessBuilder$Redirect]
           [java.util.concurrent TimeUnit]))

(defn exchange! [writer reader request]
  (.write writer (str (json/write-str request) "\n"))
  (.flush writer)
  (let [line (.readLine reader)]
    (when-not line (throw (ex-info "gRPC helper closed without a result" {})))
    (let [result (json/read-str line :key-fn keyword)]
      (when-not (#{"ok" "fail" "info"} (:type result))
        (throw (ex-info "Invalid gRPC helper result" {})))
      result)))

(defrecord GRPCClient [node->port api binary process reader writer]
  client/Client
  (open! [this test node]
    (let [address (str (or (:grpc-host test) (name node)) ":" (get node->port node 50051))
          args (cond-> [binary "--address" address "--api" (name api)]
                 (:grpc-tls-ca-file test) (conj "--tls-ca-file" (:grpc-tls-ca-file test)))
          builder (ProcessBuilder. ^java.util.List args)
          _ (.redirectError builder ProcessBuilder$Redirect/INHERIT)
          process (.start builder)]
      (assoc this :process process :reader (io/reader (.getInputStream process))
                  :writer (io/writer (.getOutputStream process)))))
  (setup! [this _test] this)
  (invoke! [_this _test op]
    (let [[k v] (:value op)]
      (try
        (let [result (exchange! writer reader
                                {:op (name (:f op))
                                 :key (str "jepsen-grpc/" (name api) "/" k)
                                 :value v})]
          (cond-> (assoc op :type (keyword (:type result)))
            (= :read (:f op)) (assoc :value (independent/tuple k (:value result)))
            (:error result) (assoc :error (:error result))))
        (catch Exception e
          (assoc op :type :info :error (.getMessage e))))))
  (teardown! [_this _test] nil)
  (close! [this _test]
    (when process
      (.destroy process)
      (when-not (.waitFor process 3 TimeUnit/SECONDS) (.destroyForcibly process)))
    (assoc this :process nil :reader nil :writer nil)))

(defn grpc-register-workload [opts]
  (let [api (or (:grpc-api opts) :raw)
        max-writes (or (:max-writes-per-key opts) 32)]
    (when-not (#{:raw :transactional} api)
      (throw (ex-info "Unsupported public gRPC API" {:api api})))
    {:client (->GRPCClient (:node->port opts) api
                           (or (:grpc-client-bin opts) "/tmp/jepsen-grpc-client") nil nil nil)
     :generator (independent/concurrent-generator
                  2 (range (or (:key-count opts) 8))
                  (fn [_k] (->> (gen/mix [(map (fn [v] {:f :write :value v}) (range))
                                         (gen/repeat {:f :read})])
                                (gen/limit max-writes))))
     :checker (checker/compose
                {:register (independent/checker (s3/s3-register-checker))
                 :elle (elle/register-checker :grpc)})}))

(defn elastickv-grpc-test [opts]
  (when-not (:local opts)
    (throw (ex-info "Use hosted-fault-workload for managed gRPC fault tests" {})))
  (let [ports (or (:node->port opts) (zipmap (:nodes opts) (repeat (or (:grpc-port opts) 50051))))
        concurrency (or (:concurrency opts) 6)
        workload (grpc-register-workload (assoc opts :node->port ports))]
    (when-not (and (pos? concurrency) (even? concurrency))
      (throw (ex-info "gRPC concurrency must be positive and divisible by two" {})))
    (merge (redis/elastickv-redis-test opts) workload
           {:name (str "elastickv-grpc-" (name (or (:grpc-api opts) :raw)))
            :grpc-host (:grpc-host opts)
            :concurrency concurrency :wrap-generator nil :final-generator nil
            :generator (->> (:generator workload)
                            (gen/stagger (/ (double (or (:rate opts) 5))))
                            (gen/time-limit (or (:time-limit opts) 30)))})))

(def grpc-cli-opts
  [[nil "--grpc-ports PORTS" "Comma-separated public gRPC ports."
    :parse-fn #(mapv (fn [port] (Integer/parseInt port)) (str/split % #","))]
   [nil "--concurrency N" "Workers, divisible by two." :default 6 :parse-fn #(Integer/parseInt %)]
   [nil "--grpc-api API" "raw or transactional" :default :raw :parse-fn keyword]
   [nil "--grpc-client-bin PATH" "JSON-line gRPC helper binary." :default "/tmp/jepsen-grpc-client"]])

(defn prepare-opts [options]
  (let [opts (cli/parse-common-opts options (:grpc-ports options))]
    (assoc opts :grpc-host (:host options)
                :node->port (zipmap (:nodes opts) (or (:grpc-ports opts) (repeat (:grpc-port opts)))))))

(defn -main [& args]
  (cli/run-workload! args
                     (into (vec (remove #(= "--concurrency N" (second %)) cli/common-cli-opts)) grpc-cli-opts)
                     prepare-opts elastickv-grpc-test))
