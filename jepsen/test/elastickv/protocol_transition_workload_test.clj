(ns elastickv.protocol-transition-workload-test
  (:require [clojure.test :refer [deftest is]]
            [elastickv.protocol-transition-workload :as protocol]
            [jepsen.checker :as checker]))

(defn- completed-pairs [node commands]
  (mapcat (fn [[f value]]
            [{:process node :node node :type :invoke :f f}
             {:process node :node node :type :ok :f f :value value}])
          commands))

(deftest recovery-requires-a-complete-observation-from-each-node
  (let [nodes ["n1" "n2" "n3"]
        history (vec (mapcat #(completed-pairs % [[:ttl :checked] [:visibility :checked]
                                                   [:multipart :checked] [:final-read 18]]) nodes))
        valid? (fn [ops] (:valid? (checker/check (protocol/transition-checker :recover nodes) {}
                                   (map-indexed #(assoc %2 :index %1) ops) {})))]
    (is (valid? history))
    (is (false? (valid? (pop history))))
    (is (false? (valid? (assoc-in history [7 :value] 17))))
    (is (false? (valid? (assoc-in history [3 :type] :fail))))
    (is (false? (valid? (assoc-in history [2 :f] :ttl))))))

(deftest seed-detects-a-stale-intervening-lua-read
  (let [nodes ["n1" "n2" "n3"]
        pre (mapcat #(completed-pairs % [[:ttl :checked] [:visibility :checked] [:multipart :checked]]) nodes)
        lua (completed-pairs "n1" (mapcat (fn [v] [[:increment v] [:read v]]) (range 1 19)))
        final (mapcat #(completed-pairs % [[:final-read 18]]) nodes)
        valid? (fn [ops] (:valid? (checker/check (protocol/transition-checker :seed nodes) {}
                        (map-indexed #(assoc %2 :index %1) (concat pre ops final)) {})))]
    (is (valid? lua))
    (is (false? (valid? (assoc-in (vec lua) [3 :value] 0))))
    (is (false? (valid? (drop-last 2 lua))))))

(deftest fixture-names-fit-all-node-buckets
  (let [workload #(protocol/workload {:fixture % :phase :seed :redis-ports [1 2 3]
                                      :s3-ports [4 5 6] :sqs-ports [7 8 9]})]
    (is (map? (workload (apply str (repeat 60 "a")))))
    (doseq [id ["A" "" (apply str (repeat 61 "a"))]]
      (is (thrown? clojure.lang.ExceptionInfo (workload id))))))
