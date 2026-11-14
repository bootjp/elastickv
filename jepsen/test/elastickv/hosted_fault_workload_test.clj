(ns elastickv.hosted-fault-workload-test
  (:require [clojure.test :refer :all]
            [elastickv.cli :as cli]
            [elastickv.hosted-fault-workload :as hosted]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.independent :as independent]
            [jepsen.nemesis :as nemesis]))

(def nodes ["n1" "n2" "n3"])

(deftest resource-setup-retries-transient-startup-errors-only
  (doseq [error [{:status 503} {:type "ServiceUnavailable"}
                {:category :cognitect.anomalies/busy} {:type "OperationOutcomeUnknown"}]]
    (let [calls (atom 0)
          delegate (reify client/Client
                     (setup! [_ _]
                       (if (= 1 (swap! calls inc))
                         (throw (ex-info "leader not ready" error)) :ready)))]
      (is (= :ready (hosted/setup-client! delegate {})))
      (is (= 2 @calls))))
  (let [calls (atom 0)
        delegate (reify client/Client
                   (setup! [_ _] (swap! calls inc)
                     (throw (ex-info "invalid request" {:status 400}))))]
    (is (thrown? clojure.lang.ExceptionInfo (hosted/setup-client! delegate {})))
    (is (= 1 @calls))))

(defn good-history [fault]
  (let [[start stop] (hosted/fault-ops fault)]
    (vec (concat
           [{:type :ok :f :txn :process 0 :value [[:append 0 1]]}]
           (mapcat (fn [[node f]]
                     [{:type :info :process :nemesis :f f :target node :value [node]}
                      {:type :info :process :nemesis :f f :target node :value :done}])
                   (for [node nodes f [start stop]] [node f]))
           [{:type :info :process :nemesis :f stop :recovery true :value :all}
            {:type :info :process :nemesis :f stop :recovery true :value :done}]
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
        (doseq [i (range 2 15 2)]
          (is (false? (:valid? (check-history fault
                                 (assoc-in history [i :error] :nemesis-error)))))))
      (testing "every node and key must be read successfully after recovery"
        (is (false? (:valid? (check-history fault (pop history)))))
        (is (false? (:valid? (check-history fault
                               (assoc-in history [15 :type] :fail))))))
      (testing "reads before recovery cannot satisfy the gate"
        (is (false? (:valid? (check-history fault
                               (concat (subvec history 15) (subvec history 0 15)))))))
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

(deftest every-adapter-uses-its-production-endpoint-and-explicit-voters
  (doseq [[adapter port-key port] [[:redis :redis-port 6379]
                                  [:dynamodb :dynamo-port 8000]
                                  [:s3 :s3-port 9000]
                                  [:sqs-standard :sqs-port 9324]
                                  [:sqs-htfifo :sqs-port 9324]]]
    (doseq [fault [:partition :kill :pause]]
      (let [test (hosted/hosted-fault-test (assoc opts :adapter adapter :faults [fault]))]
        (is (= port (get-in test [:db :opts port-key])))
        (is (= "n1=n1:50051,n2=n2:50051,n3=n3:50051"
               (get-in test [:db :opts :bootstrap-members])))
        (is (= (zipmap nodes (repeat port)) (get-in test [:client :delegate :node->port])))
        (is (nil? (:final-generator test)))
        (when (= :sqs-htfifo adapter)
          (is (string? (:sqs-observation-clock-id test))))))))

(defn adapter-history [adapter]
  (mapv (fn [op]
          (if (= :nemesis (:process op))
            op
            (case adapter
              :s3 (assoc op :f (if (:final op) :read :write)
                            :value (independent/tuple (if (:final op)
                                                      (second (first (:value op))) 0) 1))
              (:sqs-standard :sqs-htfifo)
              (assoc op :f (if (:final op) :recv :send)
                        :value (if (:final op) [] "message"))
              op)))
        (good-history :partition)))

(deftest every-adapter-requires-successful-recovery-through-every-node
  (doseq [adapter (keys hosted/adapters)]
    (let [history (adapter-history adapter)
          check #(checker/check (hosted/evidence-checker nodes :partition 2 1 adapter) {} % {})]
      (is (true? (:valid? (check history))))
      (is (false? (:valid? (check (filterv #(not= "n3" (:node %)) history)))))
      (is (false? (:valid? (check (mapv #(if (:final %) (assoc % :type :info) %) history))))))))

(deftest dynamodb-recovery-reads-do-not-exceed-the-transaction-key-limit
  (let [test {:concurrency 6 :nodes nodes}
        ctx (gen/context test)
        ops (loop [g (hosted/final-read-phase nodes (atom (set (range 103))) :dynamodb)
                   result []]
              (let [[op next-gen] (gen/op g test ctx)]
                (if (or (nil? op) (= :pending op)) result
                    (recur next-gen (conj result op)))))]
    (is (= 309 (count ops)))
    (is (every? #(= 1 (count (:value %))) ops))
    (doseq [[_ reads] (group-by #(mod (:process %) 3) ops)]
      (is (= (set (range 103)) (set (map #(second (first (:value %))) reads)))))))

(deftest stress-schedule-repeats-every-node
  (doseq [fault [:partition :kill :pause]]
    (let [ops (->> (hosted/fault-schedule nodes fault 1 true)
                   (filter :f) (take 12))]
      (is (= (concat nodes nodes) (map :target (take-nth 2 ops))))
      (is (= 6 (count (filter :f (hosted/fault-schedule nodes fault 1 false))))))))

(deftest stress-requires-multiple-completed-cycles-on-every-node
  (doseq [fault [:partition :kill :pause]]
    (let [history (good-history fault)
          repeated (into (subvec history 0 13) (subvec history 1))
          check #(checker/check (hosted/evidence-checker nodes fault 2 2) {} % {})]
      (is (false? (:valid? (check history))))
      (is (true? (:valid? (check repeated))))
      (is (= (zipmap nodes (repeat 2)) (:completed-fault-cycles (check repeated))))
      (is (false? (:valid? (check (assoc-in repeated [14 :error] :nemesis-error))))))))

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
  (is (:generator (hosted/hosted-fault-test
                    (assoc opts :repeat-faults true :min-fault-cycles 2 :time-limit 20))))
  (is (nil? (:final-generator (hosted/hosted-fault-test opts))))
  (doseq [bad [{:local true} {:host "localhost"} {:nodes ["n1" "n2"]}
               {:nodes ["n1" "n1" "n3"]} {:concurrency 5} {:rate 0}
               {:min-fault-cycles 0} {:min-fault-cycles 2}
               {:adapter :unknown} {:adapter :s3 :concurrency 3}
               {:adapter :sqs-standard :drain-time 1}
               {:faults [:clock]} {:faults [:kill :pause]} {:time-limit 7}]]
    (is (thrown? clojure.lang.ExceptionInfo
                 (hosted/hosted-fault-test (merge opts bad)))))
  (is (thrown? clojure.lang.ExceptionInfo
               (hosted/hosted-fault-test
                 (merge opts (cli/parse-common-opts
                               {:nodes "n1,n2,n3" :host "localhost" :faults "kill"}
                               [6379 6380 6381]))))))
