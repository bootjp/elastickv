(ns elastickv.grpc-workload-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [clojure.tools.cli :as tools]
            [elastickv.cli :as cli]
            [elastickv.grpc-workload :as grpc]
            [jepsen.client :as client]
            [jepsen.independent :as independent])
  (:import [java.io StringReader StringWriter]))

(deftest json-exchange-preserves-zero-and-null
  (let [writer (StringWriter.)]
    (is (= {:type "ok" :value 0}
           (grpc/exchange! writer (io/reader (StringReader. "{\"type\":\"ok\",\"value\":0}\n"))
                           {:op "write" :key "k" :value 0})))
    (is (.contains (str writer) "\"value\":0"))
    (is (nil? (:value (grpc/exchange! writer (io/reader (StringReader. "{\"type\":\"ok\",\"value\":null}\n"))
                                             {:op "read" :key "k"}))))
    (is (thrown? Exception (grpc/exchange! writer (io/reader (StringReader. "")) {})))
    (is (thrown? Exception (grpc/exchange! writer (io/reader (StringReader. "{}\n")) {})))))

(deftest operation-outcomes-remain-client-observations
  (doseq [api [:raw :transactional]]
    (let [c (:client (grpc/grpc-register-workload {:grpc-api api}))
          write {:f :write :type :invoke :value (independent/tuple 2 0)}
          request (atom nil)]
      (with-redefs [grpc/exchange! (fn [_ _ op] (reset! request op) {:type "info" :error "reply lost"})]
        (let [result (client/invoke! c {} write)]
          (is (= :info (:type result)))
          (is (= (:value write) (:value result)))
          (is (= (str "jepsen-grpc/" (name api) "/2") (:key @request)))))
      (with-redefs [grpc/exchange! (fn [& _] {:type "ok" :value nil})]
        (is (= (independent/tuple 2 nil)
               (:value (client/invoke! c {} {:f :read :value (independent/tuple 2 nil)})))))
      (with-redefs [grpc/exchange! (fn [& _] (throw (ex-info "process exited" {})))]
        (is (= :info (:type (client/invoke! c {} write))))))))

(deftest gRPC-builder-requires-real-local-endpoints-or-hosted-nemesis
  (is (thrown? Exception (grpc/elastickv-grpc-test {})))
  (is (thrown? Exception (grpc/elastickv-grpc-test {:local true :concurrency 5})))
  (is (thrown? Exception (grpc/grpc-register-workload {:grpc-api :unsupported})))
  (let [options (tools/parse-opts ["--local" "--host" "127.0.0.1" "--grpc-ports" "50051,50052,50053"]
                                (into cli/common-cli-opts (remove #(= "--concurrency N" (second %)) grpc/grpc-cli-opts)))
        opts (grpc/prepare-opts (:options options))]
    (is (= {"n1" 50051 "n2" 50052 "n3" 50053} (:node->port opts)))
    (is (= "127.0.0.1" (:grpc-host opts)))))

(deftest local-gRPC-builder-honors-the-configured-port
  (let [test (grpc/elastickv-grpc-test {:local true :nodes ["n1"] :grpc-port 51515})]
    (is (= {"n1" 51515} (get-in test [:client :node->port])))))
