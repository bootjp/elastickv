(ns elastickv.dynamodb-write-skew-test
  (:require [clojure.test :refer [deftest is]]
            [elastickv.dynamodb-write-skew :as skew]
            [elastickv.elle-checker :as elle]))

(deftest distinguish-skew-from-insufficient-coverage
  (let [round (first skew/rounds)
        [x y] (:keys round)
        [a b] (:tokens round)
        snapshot {x nil y nil}
        op (fn [step type value] {:round (:round round) :step step :type type :value value :route-groups {x 1 y 1}})
        left (assoc (op :race :ok [[:r y nil] [:append x a]]) :process 0 :observed-snapshot snapshot)
        right (assoc (op :race :fail [[:r x nil] [:append y b]]) :observed-snapshot snapshot
                     :process 1 :phase :transact-write :condition-snapshot snapshot)
        controls [(op :observe :ok [[:r x [a]] [:r y nil]])
                  (assoc (op :serial :ok [[:r y nil] [:append x (+ a 10)]]) :observed-snapshot {x [a] y nil})
                  (assoc (op :serial :ok [[:r x [a (+ a 10)]] [:append y (+ b 10)]])
                         :observed-snapshot {x [a (+ a 10)] y nil})
                  (op :final :ok [[:r x [a (+ a 10)]] [:r y [(+ b 10)]]])]
        verdict (fn [l r] (:valid? (skew/round-evidence round (into [(assoc l :type :invoke) (assoc r :type :invoke) l r] controls))))]
    (is (true? (verdict left right)))
    (let [h (into [(assoc left :type :invoke) (assoc right :type :invoke) left right]
                  (assoc-in controls [1 :observed-snapshot] snapshot))]
      (is (false? (:valid? (skew/round-evidence round h)))))
    (is (false? (verdict left (assoc right :type :ok))))
    (is (= :unknown (verdict left (assoc right :type :ok :observed-snapshot {x [a] y nil}))))
    (is (= :unknown (verdict left (dissoc right :condition-snapshot))))
    (is (= :unknown (verdict left (assoc right :type :info))))
    (is (= :unknown (verdict left (assoc right :observed-snapshot {}))))
    (is (= :unknown (:valid? (skew/round-evidence round []))))))

(deftest malformed-race-cannot-satisfy-the-coverage-gate
  (let [round (first skew/rounds)
        [x y] (:keys round)
        snap {x nil y nil}
        same-direction (for [[t token] [[:ok 101] [:fail 102]]]
                         {:round (:round round) :step :race :type t :phase :transact-write
                          :condition-snapshot snap :observed-snapshot snap
                          :value [[:r y nil] [:append x token]]})]
    (is (= :unknown (:valid? (skew/round-evidence round same-direction))))))


(deftest replay-requires-the-requested-coverage
  (let [h [{:process 0 :f :txn :type :invoke :value [[:append 0 7]]}
           {:process 0 :f :txn :type :ok :value [[:append 0 7]]}
           {:process 1 :f :txn :type :invoke :value [[:r 0 nil]]}
           {:process 1 :f :txn :type :ok :value [[:r 0 [7]]]}]
        result (elle/check-history :write-skew h "target/test-missing-round")]
    (is (true? (get-in result [:consistency :valid?])))
    (is (= :unknown (:valid? result)))))
