(ns elastickv.fault-transport-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer :all]
            [cognitect.aws.client.api :as aws]
            [cognitect.aws.http :as http]
            [elastickv.fault-transport :as transport]
            [elastickv.sqs-common :as sqs]))

(deftest bounded-transport-owns-request-deadline-and-lifecycle
  (let [seen (atom [])
        stopped (atom 0)
        created (atom 0)
        delegate (reify http/HttpClient
                   (-submit [_ request channel] (swap! seen conj request) channel)
                   (-stop [_] (swap! stopped inc)))
        client (transport/bounded-http-client 37 #(do (swap! created inc) delegate))]
    (is (zero? @created))
    (is (= :channel (http/submit client {:uri "/" :timeout-msec 999} :channel)))
    (is (= 1 @created))
    (is (= {:uri "/" :timeout-msec 37 :cognitect.http-client/timeout-msec 37}
           (first @seen)))
    (http/stop client)
    (is (= 1 @stopped))))

(deftest sqs-query-errors-preserve-service-code-and-message
  (with-redefs [aws/invoke (fn [& _] {:cognitect.anomalies/category :cognitect.anomalies/incorrect
                                     :Error {:Code "ReceiptHandleIsInvalid" :Message "wrong queue"}})]
    (try
      (sqs/invoke! :client :DeleteMessage {})
      (is false "must throw")
      (catch clojure.lang.ExceptionInfo e
        (is (= "ReceiptHandleIsInvalid" (:type (ex-data e))))
        (is (= "SQS ReceiptHandleIsInvalid: wrong queue" (.getMessage e)))))))

(deftest pinned-http-transport-enforces-total-request-deadline
  ;; A peer accepts the connection but never answers. An idle/connect timeout
  ;; alone cannot bound this request; exercise the pinned real HTTP client.
  (with-open [listener (java.net.ServerSocket. 0 1 (java.net.InetAddress/getByName "127.0.0.1"))]
    (let [release (promise)
          peer (future
                 (with-open [socket (.accept listener)]
                   (deref release 5000 nil)))
          client (transport/bounded-http-client 100 aws/default-http-client)]
      (try
        (let [response (http/submit client {:scheme :http :server-name "127.0.0.1"
                                            :server-port (.getLocalPort listener)
                                            :uri "/" :request-method :get :headers {}})
              [value source] (async/alts!! [response (async/timeout 2000)])]
          (is (= response source) "total deadline must finish before the peer responds")
          (is (contains? value :cognitect.anomalies/category)))
        (finally
          (deliver release true)
          (http/stop client)
          (future-cancel peer))))))
