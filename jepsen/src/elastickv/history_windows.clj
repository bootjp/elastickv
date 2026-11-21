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
