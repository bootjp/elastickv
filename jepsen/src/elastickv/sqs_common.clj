(ns elastickv.sqs-common
  "Shared SQS transport and Jepsen cluster lifecycle."
  (:require [cognitect.aws.client.api :as aws]
            [cognitect.aws.credentials :as creds]
            [elastickv.cli :as cli]
            [elastickv.db :as ekdb]
            [jepsen [generator :as gen] [net :as net]]
            [jepsen.control :as control]
            [jepsen.db :as jdb]
            [jepsen.nemesis :as nemesis]
            [jepsen.nemesis.combined :as combined]
            [jepsen.os :as os]
            [jepsen.os.debian :as debian]))

(defn make-client
  "Returns a cognitect/aws-api SQS client pointed at http://host:port.
   Dummy credentials match the elastickv server's open-endpoint mode (no
   --sqsCredentialsFile passed → adapter accepts any signed request)."
  [host port region]
  (aws/client
    {:api                  :sqs
     :region               (or region "us-east-1")
     :credentials-provider (creds/basic-credentials-provider
                             {:access-key-id     "dummy"
                              :secret-access-key "dummy"})
     :endpoint-override    {:protocol :http
                            :hostname  host
                            :port      port}}))

(defn- anomaly? [resp]
  (contains? resp :cognitect.anomalies/category))

(defn invoke!
  "Invoke op against sqs-client, returning the parsed response.
   Throws ex-info on any error (SQS API error or transport failure).
   ex-data: :type (SQS __type), :category (anomaly), :resp (raw)."
  [sqs op request]
  (let [resp (aws/invoke sqs {:op op :request request})]
    (if (anomaly? resp)
      (let [err-type (:__type resp)
            category (:cognitect.anomalies/category resp)
            msg      (or (:message resp)
                         (:Message resp)
                         (:cognitect.anomalies/message resp)
                         "")]
        (throw (ex-info (str "SQS " (or err-type category) ": " msg)
                        {:type     err-type
                         :category category
                         :resp     resp})))
      resp)))

(defn build-test
  "Build an SQS test, heal faults, then drain beyond the visibility timeout."
  [opts workload-fn]
  (let [nodes      (or (:nodes opts) ["n1" "n2" "n3" "n4" "n5"])
         sqs-ports  (or (:sqs-ports opts)
                        (repeat (count nodes) (or (:sqs-port opts) 9324)))
         node->port (or (:node->port opts) (cli/ports->node-map sqs-ports nodes))
         sqs-region (or (:sqs-region opts) "us-east-1")
         local?     (:local opts)
         db         (if local?
                      jdb/noop
                      (ekdb/db {:grpc-port    (or (:grpc-port opts) 50051)
                                :redis-port   (or (:redis-port opts) 6379)
                                :sqs-port     node->port
                                :sqs-region   sqs-region
                                :raft-groups  (:raft-groups opts)
                                :shard-ranges (:shard-ranges opts)
                                :encryption   (:encryption opts)}))
         rate       (double (or (:rate opts) 5))
         time-limit (or (:time-limit opts) 30)
         ;; Drain after healing and beyond the 30s visibility timeout.
         drain-time (or (:drain-time opts) 40)
         faults     (if local?
                      []
                      (cli/normalize-faults (or (:faults opts) [:partition :kill])))
         nemesis-p  (when-not local?
                      (combined/nemesis-package {:db       db
                                                 :faults   faults
                                                 :interval (or (:fault-interval opts) 40)}))
         nemesis-gen (if nemesis-p
                       (:generator nemesis-p)
                       (gen/once {:type :info :f :noop}))
         workload   (workload-fn (assoc opts :node->port node->port))
         main-gen   (->> (:generator workload)
                         (gen/nemesis nemesis-gen)
                         (gen/stagger (/ rate))
                         (gen/time-limit time-limit))
         drain-gen  (->> (gen/repeat {:f :recv})
                         (gen/stagger (/ rate))
                         (gen/time-limit drain-time)
                         (gen/clients))]
     (merge workload
            {:name            (or (:name opts) "elastickv-sqs-htfifo")
             :nodes           nodes
             :db              db
             :sqs-host        (:sqs-host opts)
             :os              (if local? os/noop debian/os)
             :net             (if local? net/noop net/iptables)
             :ssh             (merge {:username                  "vagrant"
                                      :private-key-path          "/home/vagrant/.ssh/id_rsa"
                                      :strict-host-key-checking  false}
                                     (when local? {:dummy true})
                                     (:ssh opts))
             :remote          control/ssh
             :nemesis         (if nemesis-p (:nemesis nemesis-p) nemesis/noop)
             :final-generator nil
             :concurrency     (or (:concurrency opts) 8)
             :generator       (gen/phases
                                main-gen
                                (when-let [final-gen (:final-generator nemesis-p)]
                                  (gen/nemesis final-gen))
                                drain-gen)})))
