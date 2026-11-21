(ns elastickv.elle-checker
  "Elle checks and offline replay of client-observed transaction histories."
  (:gen-class)
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [elle.list-append :as append]
            [elle.rw-register :as register]
            [elastickv.history-windows :as windows]
            [elastickv.dynamodb-conditions :as conditions]
            [elastickv.dynamodb-write-skew :as skew]
            [jepsen.checker :as checker]
            [jepsen.history :as history]
            [jepsen.store :as store])
  (:import [java.io PushbackReader]))

(def models [:strict-serializable])

(def register-kinds #{:s3 :grpc :filesystem :independent-register})

(defn register-operation
  "One observed API operation is one transaction. Only the filesystem's
   pre-created -1 sentinel maps to Elle's initial nil version."
  [kind op]
  (if (= :nemesis (:process op))
    op
    (let [value (:value op)
          f (case (:f op) :read :r :write :w
                  (throw (ex-info "Unsupported register operation" {:f (:f op)})))]
      (when-not (and (sequential? value) (= 2 (count value)))
        (throw (ex-info "Expected an independent key/value pair" {})))
      (let [[k v] value]
        (when (and (= kind :filesystem) (= :w f) (= -1 v))
          (throw (ex-info "Filesystem writes must not reuse the initial sentinel" {})))
        (assoc op :f :txn :value [[f k (if (and (= kind :filesystem) (= -1 v)) nil v)]])))))

(defn s3-operation [op] (register-operation :s3 op))

(defn transaction-history [kind events]
  (case kind
    (:s3 :grpc :filesystem :independent-register)
    (history/history (mapv #(register-operation kind %) events))
    (:list-append :write-skew :rw-register) (history/history events)
    (throw (ex-info "Unsupported Elle history kind" {:kind kind}))))

(defn- unexplained-register-reads [h]
  ;; Writes are unique per key. A transaction may also read its own earlier write.
  (let [txns (windows/operations (filter #(= :txn (:f %)) h))
        writers (into {} (for [{:keys [start invoke completion]} txns
                               :when (not= :fail (:type completion))
                               [f k v] (:value (or invoke completion))
                               :when (= :w f)]
                           [[k v] start]))]
    (for [{:keys [start end completion]} txns
          :when (= :ok (:type completion))
          [i [f k v]] (map-indexed vector (:value completion))
          :when (and (= :r f) (some? v))
          :let [writer (get writers [k v])]
          :when (or (nil? writer) (> writer end)
                    (and (= writer start)
                         (not-any? #{[:w k v]} (take i (:value completion)))))]
      {:index (:index completion) :key k :value v})))

(defn- rejected-append-reads [history]
  ;; The list-append generator uses unique tokens per key. WRONGTYPE
  ;; definitively rejects that RPUSH even if an adjacent reply is ambiguous.
  (let [rejected (into #{} (for [op history, {:keys [operation error]} (:exec-errors op)
                                 :let [[f k token] operation]
                                 :when (and (= :append f) (= :wrongtype error))] [k token]))]
    (when (seq rejected)
      (for [op history :when (= :ok (:type op)), [f k values] (:value op) :when (= :r f)
            token values :when (contains? rejected [k token])]
        {:index (:index op) :key k :token token}))))

(defn check-exec-replies [result history]
  (let [reads (rejected-append-reads history)
        errors (filter #(or (:exec-arity %) (:invalid-exec-slots %)) history)
        partial (filter #(contains? % :exec-observations) history)
        lengths? (and (empty? partial) (some :exec-lengths history))
        lengths (when lengths? (conditions/append-witness history))]
    (cond-> result
      lengths? (assoc :exec-lengths {:valid? lengths}
                      :valid? (checker/merge-valid [(:valid? result) lengths]))
      (seq partial) (assoc :partial-exec-replies (mapv #(select-keys % [:index :process :exec-observations :exec-errors]) partial)
                           :valid? (checker/merge-valid [(:valid? result) :unknown]))
      (seq reads) (assoc :valid? false :rejected-append-reads (vec reads))
      (seq errors) (assoc :valid? false :invalid-exec-replies
                          (mapv #(select-keys % [:index :process :exec-arity :invalid-exec-slots]) errors)))))

(defn check-history [kind events directory]
  (let [h (transaction-history kind events)
        opts {:consistency-models models :directory (str directory)}]
    (when (or (= :rw-register kind) (register-kinds kind))
      ;; Elle infers versions from values. Reusing a written value for one key
      ;; violates its assumptions, even when the underlying API allows it.
      (let [writes (for [op h
                         :when (and (= :invoke (:type op)) (not= :nemesis (:process op)))
                         [f k v] (:value op)
                         :when (= :w f)]
                     [k v])]
        (when (or (some #(nil? (second %)) writes)
                  (not= (count writes) (count (set writes))))
          (throw (ex-info "Elle register writes must use unique non-nil values per key" {})))))
    (case kind
      (:list-append :write-skew)
      (let [consistency (-> (append/check opts h) (conditions/check-result h) (check-exec-replies h))]
        (if (or (= :write-skew kind) (some (comp (set (map :round skew/rounds)) :round) h))
          (let [coverage (skew/coverage-result h)]
            {:consistency consistency :coverage coverage
             :valid? (checker/merge-valid [(:valid? consistency) (:valid? coverage)])})
          consistency))
      (:rw-register :s3 :grpc :filesystem :independent-register)
      (let [result (register/check opts h)
            errors (vec (unexplained-register-reads h))]
        (cond-> result
          (seq errors) (assoc :valid? false :unexplained-reads errors))))))

(defn read-history [file]
  ;; Jepsen history.edn contains one EDN operation per form, not a vector.
  ;; EDN reading does not evaluate code from an artifact.
  (with-open [reader (PushbackReader. (io/reader file))]
    (loop [events []]
      (let [op (edn/read {:eof ::eof :default tagged-literal} reader)]
        (if (= ::eof op)
          events
          (do
            (when-not (map? op)
              (throw (ex-info "Expected one operation map per EDN form" {})))
            (recur (conj events op))))))))

(defn analyze-history!
  "Retain the normalized input and Elle verdict alongside anomaly diagnostics."
  [kind events directory]
  (let [h (transaction-history kind events)
        dir (io/file directory)]
    (.mkdirs dir)
    (with-open [writer (io/writer (io/file dir "elle-history.edn"))]
      (doseq [op h]
        (.write writer (str (pr-str (into {} op)) "\n"))))
    ;; API histories have already been normalized; do not transform twice.
    (let [result (check-history (if (register-kinds kind) :rw-register kind) h dir)]
      (spit (io/file dir "elle-results.edn") (pr-str result))
      result)))

(defn analyze-file! [kind input directory]
  (analyze-history! kind (read-history input) directory))

(defn register-checker [kind]
  (reify checker/Checker
    (check [_ test h opts]
      (analyze-history! kind h
                        (.getCanonicalPath
                          (store/path! test (:subdirectory opts) "elle"))))))

(defn s3-checker [] (register-checker :s3))

(defn -main [& args]
  (try
    (when-not (= 3 (count args))
      (throw (ex-info "Usage: KIND HISTORY.edn OUTPUT-DIRECTORY; KIND=list-append|write-skew|rw-register|s3|grpc|filesystem|independent-register" {})))
    (let [[kind input directory] args
          result (analyze-file! (keyword kind) input directory)]
      (prn result)
      (shutdown-agents)
      (when-not (true? (:valid? result))
        (System/exit 1)))
    (catch Exception e
      (binding [*out* *err*] (println (.getMessage e)))
      (shutdown-agents)
      (System/exit 2))))
