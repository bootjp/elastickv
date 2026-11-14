(ns elastickv.sqs-workload
  "Standard queue delivery and acknowledgement under concurrent SQS traffic.
   Duplicate delivery and reordering are allowed. After recovery and a bounded
   drain, every acknowledged send must have been received and deleted, and
   received bodies must belong to successful or indeterminate sends."
  (:gen-class)
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [cognitect.aws.client.api :as aws]
            [elastickv.cli :as cli]
            [elastickv.sqs-common :as sqs]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.generator :as gen]))

(deftype PendingDeletes [state]
  Object
  (toString [_] "pending delete receipts (redacted)"))

(defn- delete-receipt! [conn queue-url pending body receipt]
  (try
    (sqs/invoke! conn :DeleteMessage {:QueueUrl queue-url :ReceiptHandle receipt})
    {:deleted body :receipt receipt}
    (catch Exception e
      (swap! (.-state ^PendingDeletes pending) assoc receipt body)
      {:receipt receipt
       :error (.getMessage e)
       :details {:body body
                 :type (:type (ex-data e))
                 :category (:category (ex-data e))
                 :response (select-keys (:resp (ex-data e))
                                        [:Error :ErrorResponse :__type
                                         :Message :message
                                         :cognitect.anomalies/message])}})))

(defn- receive-and-delete! [conn queue-url pending]
  ;; All copies of this queue's client share this lock and debt. No receive may
  ;; rotate a receipt while its DeleteMessage outcome is unknown. In particular,
  ;; a successful retry cannot be the stale-handle no-op permitted by SQS.
  (locking pending
    (let [debt (take 10 @(.-state ^PendingDeletes pending))
          ;; A retry is its own observable Jepsen operation, including during
          ;; faults. Record its evidence before attempting another receive.
          messages (if (seq debt)
                     []
                     (:Messages (sqs/invoke! conn :ReceiveMessage
                                  {:QueueUrl queue-url
                                   :MaxNumberOfMessages 10
                                   :VisibilityTimeout 30
                                   :WaitTimeSeconds 1})))
          receipts (if (seq debt)
                     debt
                     (map (juxt :ReceiptHandle :Body) messages))
          acknowledgements (mapv (fn [[receipt body]]
                                   (delete-receipt! conn queue-url pending body receipt))
                                 receipts)
          confirmed (filter #(contains? % :deleted) acknowledgements)]
      (swap! (.-state ^PendingDeletes pending)
             #(apply dissoc % (map :receipt confirmed)))
      {:value (mapv :Body messages)
       :receive-performed? (empty? debt)
       :deleted (mapv :deleted confirmed)
       :delete-retry-count (count debt)
       :delete-errors (mapv :error (filter :error acknowledgements))
       :delete-error-details (mapv :details (filter :error acknowledgements))})))

(defrecord StandardClient [node->port region queue-name counter conn queue-url pending-deletes]
  client/Client
  (open! [this test node]
    (assoc this :conn (sqs/make-client (or (:sqs-host test) (name node))
                                     (get node->port node 9324) region
                                     (:disable-sdk-retries test))))
  (setup! [this _test]
    ;; CreateQueue is idempotent for identical attributes; conflicting
    ;; attributes must fail setup rather than silently reuse another queue.
    (let [response (sqs/invoke! conn :CreateQueue {:QueueName queue-name})]
      (if-let [url (:QueueUrl response)]
        (do (reset! queue-url url) this)
        (throw (ex-info "CreateQueue did not return QueueUrl" {:response response})))))
  (invoke! [_this _test op]
    ;; Allocate before I/O and retain the identity on ambiguous outcomes.
    (let [op (if (= :send (:f op))
               (assoc op :value (str "jepsen-standard-" (swap! counter inc)))
               op)]
      (try
        (case (:f op)
          :send (do (sqs/invoke! conn :SendMessage
                                {:QueueUrl @queue-url :MessageBody (:value op)})
                    (assoc op :type :ok))
          :recv (merge op {:type :ok} (receive-and-delete! conn @queue-url pending-deletes)))
        (catch Exception e
          ;; A transport error or server error may happen after commit.
          ;; Conservatively keep these sends in the possible-send set.
          (assoc op :type :info :error (.getMessage e))))))
  (teardown! [_this _test] nil)
  (close! [this _test]
    (when conn (aws/stop conn))
    (assoc this :conn nil :queue-url nil)))

(defn standard-checker []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [sends (filter #(= :send (:f %)) history)
            committed (set (map :value (filter #(= :ok (:type %)) sends)))
            possible (set (map :value (filter #(#{:ok :info} (:type %)) sends)))
            receives (filter #(and (= :recv (:f %)) (= :ok (:type %))) history)
            deliveries (mapcat :value receives)
            received (set deliveries)
            deleted (set (mapcat :deleted receives))
            lost (set/difference committed received)
            unacked (set/difference committed deleted)
            unexpected (set/difference received possible)]
        {:valid? (and (pos? (count committed))
                      (empty? lost) (empty? unacked) (empty? unexpected))
         :committed-sends (count committed)
         :in-flight-sends (count (set/difference possible committed))
         :received (count received)
         :deleted (count deleted)
         :duplicate-deliveries (- (count deliveries) (count received))
         :delete-errors (count (mapcat :delete-errors receives))
         :lost lost
         :unacked unacked
         :unexpected unexpected}))))

(defn sqs-workload [opts]
  {:client (->StandardClient (:node->port opts) (:sqs-region opts)
                             (or (:queue-name opts)
                                 (str "jepsen-standard-" (java.util.UUID/randomUUID)))
                             (atom 0) nil (atom nil) (PendingDeletes. (atom {})))
   :generator (gen/mix [(gen/repeat {:f :send}) (gen/repeat {:f :recv})])
   :checker (standard-checker)})

(defn elastickv-sqs-test
  ([] (elastickv-sqs-test {}))
  ([opts]
   (sqs/build-test (assoc opts :name (or (:name opts) "elastickv-sqs-standard"))
                   sqs-workload)))

(def sqs-cli-opts
  [[nil "--sqs-ports PORTS" "Comma-separated SQS ports (one per node)."
    :parse-fn #(mapv (fn [p] (Integer/parseInt p)) (str/split % #","))]
   [nil "--sqs-port PORT" "SQS port on managed nodes."
    :default 9324 :parse-fn #(Integer/parseInt %)]
   [nil "--sqs-region REGION" "AWS signing region." :default "us-east-1"]
   [nil "--drain-time SECONDS" "Drain duration after recovery (default 40s)."
    :parse-fn #(Integer/parseInt %)]])

(defn prepare-opts [options]
  (let [ports (:sqs-ports options)]
    (assoc (cli/parse-common-opts options ports) :sqs-host (:host options))))

(defn -main [& args]
  (cli/run-workload! args (into cli/common-cli-opts sqs-cli-opts)
                     prepare-opts elastickv-sqs-test))
