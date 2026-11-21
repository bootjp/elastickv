(ns elastickv.dynamodb-conditions
  (:require [clojure.string :as str]
            [elastickv.history-windows :as windows]
            [jepsen.checker :as checker]))

(defn- error-code [s]
  (when s (last (str/split s #"#"))))

(defn transact-write!
  "Record what a single TransactWriteItems attempt actually compared.
   CancellationReasons, when present, follow TransactItems order."
  [invoke! ddb items ordered-keys snapshot]
  (try
    (invoke! ddb :TransactWriteItems {:TransactItems items})
    (catch clojure.lang.ExceptionInfo e
      (let [{:keys [type resp]} (ex-data e)
            code (error-code type)
            conditional? (= "ConditionalCheckFailedException" code)
            canceled? (= "TransactionCanceledException" code)
            reasons (:CancellationReasons resp)
            failed-keys (when (= (count ordered-keys) (count reasons))
                          (keep (fn [[k reason]]
                                  (when (= "ConditionalCheckFailed" (:Code reason)) k))
                                (map vector ordered-keys reasons)))
            ;; ElasticKV emits the predicate failure as the cancellation message.
            condition? (or conditional? (seq failed-keys)
                           (and canceled? (= "conditional check failed"
                                             (or (:message resp) (:Message resp)))))]
        (if (or conditional? canceled?)
          (throw (ex-info (.getMessage e)
                          (assoc (ex-data e) :write-rejection
                                 (cond-> {:type :fail :error code}
                                   (not condition?) (assoc :write-snapshot snapshot)
                                   condition? (assoc :condition-snapshot snapshot)
                                   (seq failed-keys) (assoc :failed-keys (vec failed-keys))))
                          e))
          (throw (ex-info (.getMessage e) (assoc (ex-data e) :write-snapshot snapshot) e)))))
    (catch Exception e
      (throw (ex-info (.getMessage e) {:write-snapshot snapshot} e)))))

(defn read-snapshot! [get! ddb keys]
  (try
    (get! ddb keys)
    (catch Exception e
      ;; The client has not reached TransactWriteItems.
      (throw (ex-info (.getMessage e) (merge {:read-error true} (select-keys (ex-data e) [:invalid-snapshot-reply])) e)))))

(defn failure-op [op e]
  (let [{:keys [type category read-error barrier-error invalid-snapshot-reply write-rejection write-snapshot resp]} (ex-data e)
        type (error-code type)
        result (cond
                 barrier-error (assoc op :type :fail :phase :before-transact-write :error :barrier-timeout)
                 read-error (assoc op :type :fail :phase :transact-get :error (.getMessage e))
                 write-rejection (merge op {:phase :transact-write} write-rejection)
                 (= type "ValidationException")
                 (assoc op :type :fail :error (str type ": " (or (:message resp) (:Message resp) "")))
                 (#{"ConditionalCheckFailedException" "TransactionCanceledException" "AccessDeniedException"
                    "ResourceNotFoundException" "UnrecognizedClientException"} type)
                 (assoc op :type :fail :error type)
                 (and (nil? type) (#{:cognitect.anomalies/fault :cognitect.anomalies/unavailable} category))
                 (assoc op :type :info :error :network-error)
                 :else (assoc op :type :info :error (or type (.getMessage e))))]
    (cond-> result invalid-snapshot-reply (assoc :invalid-snapshot-reply invalid-snapshot-reply)
                   write-snapshot (assoc :write-snapshot write-snapshot)
                   (and write-snapshot (not barrier-error)) (assoc :phase :transact-write))))

(defn- same-snapshot? [db snapshot]
  (every? (fn [[k v]] (= (get db k) v)) snapshot))

(defn- apply-mops [db mops check-reads? lengths]
  (reduce-kv (fn [db i [f k v]]
               (case f
                 :append (let [values (conj (get db k []) v)
                               length (get lengths i)]
                           (if (or (nil? length) (= length (count values)))
                             (assoc db k values) (reduced nil)))
                 :r (if (or (not check-reads?) (= (seq v) (seq (get db k)))) db (reduced nil))))
             db (vec mops)))

(defn- actions [{:keys [start end invoke completion]}]
  (let [outcome (:type completion)
        mops (:value (or invoke completion))
        snapshot (:condition-snapshot completion)
        writes? (some #(= :append (first %)) mops)]
    (cond
      (and (= :fail outcome) snapshot)
      {:start start :end end :stage :snapshot :snapshot snapshot :failed (:failed-keys completion)}

      (= :ok outcome)
      {:start start :end end :stage :txn :mops (:value completion) :checked true :lengths (:exec-lengths completion)
       :snapshot (:write-snapshot completion)}

      (and (= :fail outcome) (:write-snapshot completion))
      {:start start :end end :stage :read :snapshot (:write-snapshot completion)}

      (and (= :info outcome) (:write-snapshot completion))
      {:start start :end end :stage :uncertain-snapshot
       :snapshot (:write-snapshot completion) :mops mops}

      (and writes? (not= :fail outcome))
      {:start start :end Long/MAX_VALUE :stage :txn :mops mops
       :snapshot (:write-snapshot completion)})))

(defn- advance [db remaining [id action]]
  (let [{:keys [stage snapshot failed mops checked lengths]} action]
    (case stage
      :read (when (same-snapshot? db snapshot) [db (dissoc remaining id)])
      :snapshot (when (same-snapshot? db snapshot)
                  [db (assoc remaining id (assoc action :stage :reject))])
      :uncertain-snapshot
      (when (same-snapshot? db snapshot)
        [db (-> remaining (dissoc id)
                (assoc [Long/MAX_VALUE (:start action)]
                       (assoc action :stage :txn :end Long/MAX_VALUE)))])
      :reject (let [changed? #(not= (get db %) (get snapshot %))]
                (when (if (seq failed) (every? #(and (contains? snapshot %) (changed? %)) failed) (some changed? (keys snapshot)))
                  [db (dissoc remaining id)]))
      :txn (when (same-snapshot? db snapshot)
             (when-let [next-db (apply-mops db mops checked lengths)]
               [next-db (dissoc remaining id)])))))

(defn append-witness [history]
  (let [ops (keep actions (windows/operations (filter #(= :txn (:f %)) history)))]
    (windows/serial-witness ops advance)))

(defn condition-verdict
  "Find one serial execution for all reads, atomic appends and predicate
   failures. A rejected attempt observes its snapshot and later compares it,
   both within its call. An unknown write may execute once after its reply."
  [history]
  (when (some #(or (:condition-snapshot %) (:write-snapshot %)) history)
    (let [valid? (append-witness history)]
      (case valid?
        true {:valid? true}
        false {:valid? false :reason :no-consistent-condition-history}
        {:valid? :unknown :reason :condition-search-limit}))))

(defn check-result [result history]
  (let [conditions (condition-verdict history)
        invalid (keep :invalid-snapshot-reply history)]
    (cond-> result
      conditions (assoc :conditions conditions
                        :valid? (checker/merge-valid [(:valid? result) (:valid? conditions)]))
      (seq invalid) (assoc :valid? false :invalid-snapshot-replies (vec invalid)))))

(defn snapshot-responses [resp keys]
  (let [items (:Responses resp)]
    (when-not (and (sequential? items) (= (count keys) (count items)))
      (throw (ex-info "TransactGetItems returned a malformed response array"
                      {:invalid-snapshot-reply {:expected (count keys)
                                                :actual (when (sequential? items) (count items))}})))
    items))

(defn with-condition-checker [workload]
  (let [append-checker (:checker workload)]
    (assoc workload :checker
           (reify checker/Checker
             (check [_ test history opts]
               (check-result (checker/check append-checker test history opts) history))))))
