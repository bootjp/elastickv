(ns elastickv.history-windows)

(defn operations
  "Pair invocations with outcomes using positions in the original history.
   A pending operation has no completion. Completion-only fixtures use the
   completion position as the earliest observed start."
  [history]
  (let [{:keys [pending done]}
        (reduce (fn [state [index op]]
                  (let [id [(:process op) (:f op)]
                        pending (get-in state [:pending id])]
                    (case (:type op)
                      :invoke (cond-> (assoc-in state [:pending id] {:start index :invoke op})
                                pending (update :done conj pending))
                      (:ok :info :fail)
                      (-> state
                          (update :pending dissoc id)
                          (update :done conj (assoc (or pending {:start index})
                                                    :end index :completion op)))
                      state)))
                {:pending {} :done []}
                (map-indexed vector history))]
    (into done (vals pending))))

(def ^:dynamic *candidate-budget* 50000)

(defn serial-witness
  "Search for a single execution of all mandatory actions. The earliest
   outstanding completion bounds the next choice. Optional actions have an
   open deadline and may be omitted once every mandatory action is satisfied.
   advance returns the next [state remaining-actions], or nil on a mismatch."
  [actions advance]
  (let [initial (into (sorted-map) (map (fn [op] [[(:end op) (:start op)] op]) actions))
        budget (volatile! *candidate-budget*)]
    (try
      (loop [todo (list [{} initial])]
        (if-let [[state remaining] (first todo)]
          (let [deadline (ffirst (first remaining))]
            (if (or (nil? deadline) (= Long/MAX_VALUE deadline))
              true
              (let [branches (keep (fn [[_ op :as candidate]]
                                     (when (neg? (vswap! budget dec))
                                       (throw (ex-info "Candidate budget exhausted" {:reason ::budget})))
                                     (when (<= (:start op) deadline)
                                       (advance state remaining candidate)))
                                   remaining)]
                (recur (concat branches (rest todo))))))
          false))
      (catch clojure.lang.ExceptionInfo e
        (if (= ::budget (:reason (ex-data e))) :unknown (throw e))))))
