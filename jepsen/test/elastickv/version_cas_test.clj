(ns elastickv.version-cas-test
  (:require [clojure.test :refer :all]
            [elastickv.version-cas :as cas]))

(defn call [process f fields]
  (merge {:type :invoke :key "k" :process process :f f} fields))
(defn finish [call type fields] (merge call {:type type} fields))

(def edit (call 0 :cas {:expected 10 :value 1}))
(def applied (finish edit :ok {:version 20}))
(def read-key (call 1 :version-read {}))
(def latest (finish read-key :ok {:version 20 :value 1}))
(def old (finish read-key :ok {:version 10 :value 0}))
(def reuse (call 2 :cas {:expected 10 :value 2}))
(def rejected (finish reuse :fail {:error "version-mismatch"}))
(def chain (call 0 :cas {:expected 20 :value 3}))
(def chained (finish chain :ok {:version 30}))
(def healthy [edit applied read-key latest reuse rejected chain chained])

(deftest clean-history-needs-positive-and-negative-observations
  (is (true? (:valid? (cas/check-history healthy))))
  (doseq [history [[] [edit (finish edit :info {}) read-key old] [edit applied read-key latest]
                   [edit applied read-key latest reuse rejected]]]
    (is (= :unknown (:valid? (cas/check-history history))))))

(deftest previously-missed-histories
  (is (= :version-regressed (-> [edit applied read-key old] cas/check-history :errors first :kind)))
  (is (contains? (set (map :kind (:errors (cas/check-history
                            [edit reuse applied (finish reuse :ok {:version 30})]))))
                 :token-consumed-twice))
  (is (false? (:valid? (cas/check-history [edit applied reuse (finish reuse :ok {:version 30})]))))
  (is (= :lost-key (-> [edit applied read-key (finish read-key :ok {:missing true})]
                        cas/check-history :errors first :kind))))

(deftest real-time-overlap-and-ambiguous-outcomes
  ;; The old read began before the CAS completed; it may linearize first.
  (is (true? (:valid? (cas/check-history
                        [read-key edit applied old read-key latest reuse rejected chain chained]))))
  (is (true? (:valid? (cas/check-history
                        (into healthy [reuse (finish reuse :info {:error "reply lost"})])))))
  (let [other (assoc edit :key "another")]
    (is (true? (:valid? (cas/check-history (into healthy [other (finish other :ok {:version 20})]))))))
  (is (false? (:valid? (cas/check-history
                         [edit applied read-key (assoc latest :value 42)]))))
  (is (false? (:valid? (cas/check-history [edit (finish edit :ok {:version 10})])))))

(deftest reuse-coverage-requires-an-acknowledged-consumption-before-invocation
  (let [ambiguous (cas/check-history [edit applied chain chained read-key
                                      (finish read-key :ok {:version 30 :value 3})
                                      reuse (finish reuse :info {:error "reply lost"})])
        overlap (cas/check-history [edit reuse applied rejected read-key latest chain chained])]
    (is (= :unknown (:valid? ambiguous)))
    (is (= 1 (:indeterminate ambiguous)))
    (is (zero? (:rejected-reuses ambiguous)))
    (is (= :unknown (:valid? overlap)))
    (is (zero? (:rejected-reuses overlap)))))
