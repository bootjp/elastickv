(ns elastickv.dynamodb-seal-workload
  "Local DynamoDB append acceptance with recorded, verified Raft elections."
  (:gen-class)
  (:require [clojure.java.shell :as shell]
            [clojure.string :as str]
            [cognitect.aws.client.api :as aws]
            [elastickv.cli :as cli]
            [elastickv.dynamodb-workload :as dynamo]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.db :as db]
            [jepsen.generator :as gen]
            [jepsen.nemesis :as nemesis]))

(defn- admin! [binary address command]
  (let [result (shell/sh binary address command)]
    (when-not (zero? (:exit result))
      (throw (ex-info "raftadmin failed" (assoc result :address address :command command))))
    (:out result)))

(defn- current-leader [binary addresses]
  (or (some (fn [address]
              (when (= "state: LEADER" (str/trim (admin! binary address "state")))
                address))
            addresses)
      (throw (ex-info "no Raft leader" {:addresses addresses}))))

(defn leader-transfer-nemesis [binary addresses]
  (reify
    nemesis/Reflection
    (fs [_] #{:leader-transfer})
    nemesis/Nemesis
    (setup! [this _] this)
    (invoke! [_ _ op]
      (try
        (let [before (current-leader binary addresses)
              _ (admin! binary before "leadership_transfer")
              deadline (+ (System/nanoTime) 10000000000)
              after (loop []
                      (let [leader (try (current-leader binary addresses)
                                        (catch Exception _ nil))]
                        (cond
                          (and leader (not= before leader)) leader
                          (< (System/nanoTime) deadline) (do (Thread/sleep 50) (recur))
                          :else (throw (ex-info "leadership did not change" {:before before})))))]
          (assoc op :value {:from before :to after}))
        (catch Exception e
          (assoc op :error (.getMessage e) :value :transfer-failed))))
    (teardown! [_ _] nil)))

(defn leader-change-checker []
  (reify checker/Checker
    (check [_ _ history _]
      (let [ops (filter #(and (= :nemesis (:process %)) (= :leader-transfer (:f %))) history)
            failures (filter :error ops)
            changes (filter #(let [{:keys [from to]} (:value %)]
                               (and from to (not= from to))) ops)]
        {:valid? (boolean (and (seq changes) (empty? failures)))
         :verified-leader-changes (count changes)
         :failures (vec failures)}))))

(defn fresh-append-db []
  (reify db/DB
    (setup! [_ test node]
      ;; DB setup completes on every node before any client setup starts.
      ;; Delete once; the existing clients recreate an empty new generation.
      (when (= node (first (:nodes test)))
        (let [opened (client/open! (:client test) test node)]
          (try
            (let [response (aws/invoke (:ddb opened)
                                      {:op :DeleteTable :request {:TableName "jepsen_append"}})]
              (when (and (:cognitect.anomalies/category response)
                         (not= "ResourceNotFoundException" (:__type response)))
                (throw (ex-info "cannot reset append table" {:response response}))))
            (finally (client/close! opened test))))))
    (teardown! [_ _ _] nil)))

(defn seal-test [opts]
  (when-not (and (:local opts) (seq (:raftadmin-bin opts)) (seq (:raft-addresses opts)))
    (throw (ex-info "seal acceptance requires local mode, raftadmin-bin and raft-addresses" {})))
  (let [base (dynamo/elastickv-dynamodb-test opts)
        workload (dynamo/dynamodb-append-workload opts)
        interval (or (:leader-change-interval opts) 2)]
    (assoc base
      :name "elastickv-dynamodb-snapshot-seal"
      :db (fresh-append-db)
      :nemesis (leader-transfer-nemesis (:raftadmin-bin opts) (:raft-addresses opts))
      :checker (checker/compose {:append (:checker workload) :leader-changes (leader-change-checker)})
      :generator (->> (:generator workload)
                      (gen/nemesis (cycle [(gen/sleep interval) {:type :info :f :leader-transfer}]))
                      (gen/stagger (/ (double (or (:rate opts) 5))))
                      (gen/time-limit (or (:time-limit opts) 30))))))

(def seal-cli-opts
  [[nil "--raftadmin-bin PATH" "Repository raftadmin binary (local control only)."]
   [nil "--raft-addresses ADDRESSES" "Comma-separated local Raft/gRPC endpoints."
    :parse-fn #(str/split % #",")]
   [nil "--leader-change-interval SECONDS" "Seconds between leadership transfers."
    :default 2 :parse-fn #(Double/parseDouble %)
    :validate [pos? "must be positive"]]])

(defn -main [& args]
  (cli/run-workload!
    args (into (into cli/common-cli-opts dynamo/dynamo-cli-opts) seal-cli-opts)
    (fn [options]
      (let [options (cli/parse-common-opts options (:dynamo-ports options))]
        (assoc options
          :dynamo-host (:host options)
          :node->port (cli/ports->node-map (:dynamo-ports options) (:nodes options)))))
    seal-test))
