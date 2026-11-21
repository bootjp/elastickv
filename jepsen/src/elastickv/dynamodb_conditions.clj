(ns elastickv.dynamodb-conditions
  (:require [clojure.string :as str]
            [elastickv.history-windows :as windows]
            [jepsen.checker :as checker]))

(defn transact-write!
  "Record what a single TransactWriteItems attempt actually compared.
   CancellationReasons, when present, follow TransactItems order."
  [invoke! ddb items ordered-keys snapshot]
  (try
    (invoke! ddb :TransactWriteItems {:TransactItems items})
    (catch clojure.lang.ExceptionInfo e
      (let [{:keys [type resp]} (ex-data e)
            code (last (str/split (or type "") #"#"))
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
                                   condition? (assoc :condition-snapshot snapshot)
                                   (seq failed-keys) (assoc :failed-keys (vec failed-keys))))
                          e))
          (throw e))))))

(defn- possible-appends [history]
  (reduce (fn [by-key {:keys [invoke completion start end]}]
            (if (= :fail (:type completion))
              by-key
              (reduce (fn [m [f k v]]
                        (if (= :append f)
                          (update m k (fnil conj []) [start (when (= :ok (:type completion)) end) v])
                          m))
                      by-key (:value (or invoke completion)))))
          {} history))

(defn- unexplained-rejection [writers {:keys [start end completion]}]
  (when-let [snapshot (:condition-snapshot completion)]
    (let [possible? (fn [k]
                      (let [observed (set (get snapshot k))]
                        (some (fn [[begin finish v]]
                                ;; An ambiguous append can take effect after its response.
                                (and (< begin end) (or (nil? finish) (> finish start))
                                     (not (contains? observed v))))
                              (get writers k))))
          failed (:failed-keys completion)
          unexplained (if (seq failed)
                        (remove possible? failed)
                        (when-not (some possible? (keys snapshot)) (keys snapshot)))]
      (when (seq unexplained)
        {:process (:process completion) :start start :end end
         :keys (vec unexplained) :snapshot snapshot}))))

(defn unexplained-rejections [history]
  (let [txns (windows/operations (filter #(= :txn (:f %)) history))
        writers (possible-appends txns)]
    (vec (keep #(unexplained-rejection writers %) txns))))

(defn with-condition-checker [workload]
  (let [append-checker (:checker workload)]
    (assoc workload :checker
           (reify checker/Checker
             (check [_ test history opts]
               (let [result (checker/check append-checker test history opts)
                     errors (unexplained-rejections history)]
                 (cond-> result
                   (seq errors) (assoc :valid? false :unexplained-rejections errors))))))))
