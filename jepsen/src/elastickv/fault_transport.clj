(ns elastickv.fault-transport
  "Bound AWS requests during container faults without replaying mutations."
  (:require [cognitect.aws.client.api :as aws]
            [cognitect.aws.http :as http]))

(def request-timeout-ms 10000)

(defn bounded-http-client
  "A client-owned, lazily initialized transport. SDK stop closes its delegate.
   Both keys are required: the AWS protocol documents the unqualified key,
   while the pinned cognitect HTTP implementation consumes the qualified key."
  ([] (bounded-http-client request-timeout-ms aws/default-http-client))
  ([timeout-ms create-client]
   (let [delegate (delay (create-client))]
     (reify http/HttpClient
       (-submit [_ request channel]
         (http/submit @delegate
                      (assoc request :timeout-msec timeout-ms
                                     :cognitect.http-client/timeout-msec timeout-ms)
                      channel))
       (-stop [_]
         (when (realized? delegate)
           (http/stop @delegate)))))))
