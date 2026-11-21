(ns elastickv.version-cas
  "Client-observable invariants for a non-expiring register initialized once per run."
  (:require [jepsen.client :as client]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]))

(defn- version? [v] (and (integer? v) (pos? v)))

(defn- observe [state path value]
  (update-in state path (fnil max 0) value))

(defn- violation [s kind call completion]
  (update s :errors conj {:kind kind :invoke call :completion completion}))

(defn- check-result [s call op bound]
  (let [{:keys [key f expected]} call
        {:keys [version value missing]} op
        cas? (= :cas f)
        row? (and (version? version) (integer? value))
        known (get-in s [:values [key version]])]
    (cond
      (= :seed f) (observe s [:floor key] 1)
      (and (= :version-read f) missing)
      (if (pos? bound) (violation s :lost-key call op) s)
      (not row?) (violation s :invalid-versioned-result call op)
      :else
      (cond-> (-> s (observe [:floor key] version) (assoc-in [:values [key version]] value))
        (< version bound) (violation :version-regressed call op)
        (and (some? known) (not= known value)) (violation :version-value-changed call op)
        (and cas? (or (not (version? expected)) (<= version expected)
                      (< expected bound) (not= (:value call) value)))
        (violation :invalid-replacement call op)
        (and cas? (contains? (:consumed s) [key expected]))
        (violation :token-consumed-twice call op)
        cas? (update :consumed conj [key expected])
        cas? (update :successes inc)
        cas? (observe [:ack key] version)
        (and cas? (version? expected) (= expected (:after-ack call))) (update :chained-successes inc)
        (and (= f :version-read) (:after-ack call)) (update :reads-after-ack inc)))))

(defn check-history [history]
  (let [step (fn [s {:keys [type f key process expected] :as op}]
               (if-not (#{:seed :version-read :cas} f)
                 s
                 (if (= type :invoke)
                   (assoc-in s [:pending process]
                             [(assoc op :after-ack (get-in s [:ack key]))
                              (get-in s [:floor key] 0)
                              (contains? (:consumed s) [key expected])])
                   (let [[call bound stale?] (get-in s [:pending process])
                         s (update s :pending dissoc process)]
                     (cond
                       (nil? call) (violation s :missing-invocation nil op)
                       (or (not= f (:f call)) (not= key (:key call)))
                       (violation s :wrong-completion call op)
                       (= :ok type) (check-result s call op bound)
                       (and stale? (= f :cas) (= :fail type) (= "version-mismatch" (:error op)))
                       (update s :rejected-reuses inc)
                       (= :info type) (update s :indeterminate inc)
                       :else s)))))
        s (reduce step {:successes 0 :chained-successes 0 :reads-after-ack 0 :rejected-reuses 0
                        :indeterminate 0 :errors [] :consumed #{}}
                  history)]
    (assoc (select-keys s [:successes :chained-successes :reads-after-ack :rejected-reuses :indeterminate :errors])
           :valid? (cond (seq (:errors s)) false
                         (every? pos? ((juxt :chained-successes :reads-after-ack :rejected-reuses) s)) true
                         :else :unknown))))

(defn- record-version! [tokens k token]
  (when (version? token)
    (swap! tokens update-in [k :current] (fnil max 0) token)))

(defrecord VersionClient [transport exchange! tokens]
  client/Client
  (open! [this test node] (assoc this :transport (client/open! transport test node)))
  (setup! [this _] this)
  (invoke! [_ _ {:keys [f key expected value] :as op}]
    (if (and (= :cas f) (not (version? expected)))
      (assoc op :type :fail :error :no-token)
      (try
        (let [args (case f
                     :seed {:op "write" :value value}
                     :version-read {:op "version-read"}
                     :cas {:op "cas" :expected expected :value value})
              r (exchange! (:writer transport) (:reader transport) (assoc args :key key))
              completion (merge op (select-keys r [:version :value :missing :error])
                                {:type (keyword (:type r))})]
          (when (= :ok (:type completion))
            (record-version! tokens key (:version r))
            (when (= :cas f) (swap! tokens assoc-in [key :consumed] expected)))
          completion)
        (catch Exception e (assoc op :type :info :error (.getMessage e))))))
  (teardown! [_ _])
  (close! [this test] (assoc this :transport (client/close! transport test))))

(defn workload [transport exchange! opts]
  (let [k (str "jepsen-version-cas/" (java.util.UUID/randomUUID))
        tokens (atom {})
        payload (atom 0)
        read-op {:f :version-read :key k}
        update-op (fn [source]
                    (fn [] {:f :cas :key k :expected (get-in @tokens [k source])
                            :value (swap! payload inc)}))
        attempts (gen/mix [(gen/repeat read-op) (update-op :current) (update-op :consumed)])]
    {:client (->VersionClient transport exchange! tokens)
     :generator (gen/phases
                  (gen/once {:f :seed :key k :value 0})
                  (gen/once read-op)
                  (->> attempts (gen/stagger (/ (double (or (:rate opts) 5))))
                       (gen/time-limit (or (:time-limit opts) 30)))
                  (gen/each-thread (gen/once read-op)))
     :checker (reify checker/Checker (check [_ _ history _] (check-history history)))}))
