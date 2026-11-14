(ns elastickv.hosted-fault-workload-test
  (:require [clojure.test :refer :all]
            [elastickv.cli :as cli]
            [elastickv.hosted-fault-workload :as hosted]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.nemesis :as nemesis]))

(def nodes ["n1" "n2" "n3"])

(defn good-history [fault]
  (let [[start stop] (hosted/fault-ops fault)]
    (vec (concat
           [{:type :ok :f :txn :process 0 :value [[:append 0 1]]}]
           (for [node nodes f [start stop]]
             {:type :info :process :nemesis :f f :target node})
           [{:type :info :process :nemesis :f stop :recovery true}]
           (for [node nodes k (range 2)]
             {:type :ok :process 0 :f :txn :final true :node node
              :value [[:r k (when (zero? k) [1])]]})))))

(defn check-history [fault history]
  (checker/check (hosted/evidence-checker nodes fault 2) {} history {}))

(deftest fault-and-recovery-evidence-is-required
  (doseq [fault [:partition :kill :pause]]
    (let [history (good-history fault)]
      (is (true? (:valid? (check-history fault history))))
      (testing "no missing, failed, or unexecuted fault/heal is accepted"
        (doseq [i (range 1 8)]
          (is (false? (:valid? (check-history fault
                                 (assoc-in history [i :error] :nemesis-error)))))))
      (testing "every node and key must be read successfully after recovery"
        (is (false? (:valid? (check-history fault (pop history)))))
        (is (false? (:valid? (check-history fault
                               (assoc-in history [8 :type] :fail))))))
      (testing "reads before recovery cannot satisfy the gate"
        (is (false? (:valid? (check-history fault
                               (concat (subvec history 8) (subvec history 0 8)))))))
      (testing "an empty write history must fail"
        (is (false? (:valid? (check-history fault (subvec history 1)))))))))

(deftest partition-keeps-a-quorum-and-covers-every-node
  (let [ops (filter map? (hosted/fault-cycle nodes :partition 1))
        starts (filter #(= :start-partition (:f %)) ops)]
    (is (= nodes (mapv :target starts)))
    (doseq [{:keys [target value]} starts]
      (is (= (set (remove #{target} nodes)) (set (value target)))))
    (is (= 3 (count (filter #(= :stop-partition (:f %)) ops))))))

(def opts
  {:nodes nodes :faults [:kill] :fault-interval 1 :time-limit 10
   :rate 5 :concurrency 6 :key-count 2 :recovery-time 1})

(deftest only-the-requested-fault-nemesis-is-initialized
  (doseq [fault [:partition :kill :pause]]
    (let [test (hosted/hosted-fault-test (assoc opts :faults [fault]))]
      (is (= (if (= :partition fault)
               #{:start-partition :stop-partition}
               #{:kill :start :pause :resume})
             (set (nemesis/fs (:nemesis test))))))))

(deftest final-reads-use-every-node-endpoint-and-every-key
  (let [test {:concurrency 6 :nodes nodes}
        ctx (gen/context test)
        touched (atom #{0 1})
        generator (hosted/final-read-phase nodes touched)
        _ (swap! touched conj 12)
        ops (loop [g generator result []]
              (let [[op next-gen] (gen/op g test ctx)]
                (if (or (nil? op) (= :pending op))
                  result
                  (recur next-gen (conj result op)))))
        by-node (group-by #(mod (:process %) 3) ops)]
    (is (= 3 (count ops)))
    (is (every? :final ops))
    (is (= #{0 1 2} (set (keys by-node))))
    (doseq [[_ reads] by-node]
      (is (= [0 1 12] (mapv second (:value (first reads))))))))

(deftest rotated-keys-also-require-recovery-reads
  (let [history (into [{:type :invoke :f :txn :process 0 :value [[:append 12 1]]}]
                      (good-history :kill))]
    (is (false? (:valid? (check-history :kill history))))
    (is (= #{["n1" 12] ["n2" 12] ["n3" 12]}
           (set (:missing-final-reads (check-history :kill history)))))))

(deftest managed-test-constructs-and-rejects-disabled-or-incomplete-faults
  (is (:generator (hosted/hosted-fault-test opts)))
  (is (nil? (:final-generator (hosted/hosted-fault-test opts))))
  (doseq [bad [{:local true} {:host "localhost"} {:nodes ["n1" "n2"]}
               {:nodes ["n1" "n1" "n3"]} {:concurrency 5} {:rate 0}
               {:faults [:clock]} {:faults [:kill :pause]} {:time-limit 7}]]
    (is (thrown? clojure.lang.ExceptionInfo
                 (hosted/hosted-fault-test (merge opts bad)))))
  (is (thrown? clojure.lang.ExceptionInfo
               (hosted/hosted-fault-test
                 (merge opts (cli/parse-common-opts
                               {:nodes "n1,n2,n3" :host "localhost" :faults "kill"}
                               [6379 6380 6381]))))))
