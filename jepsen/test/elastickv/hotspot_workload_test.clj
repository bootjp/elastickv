(ns elastickv.hotspot-workload-test
  (:require [clojure.java.shell :as shell]
            [clojure.test :refer :all]
            [elastickv.cli :as cli]
            [elastickv.db :as ekdb]
            [elastickv.hotspot-workload :as hotspot]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.history :as history]
            [jepsen.nemesis :as nemesis]
            [jepsen.os :as os]))

(def before
  {:catalog-version 1
   :routes [{:route-id 1 :raft-group-id 1 :start "" :end nil
             :state "ROUTE_STATE_ACTIVE"}]})

(def after
  {:catalog-version 2
   :routes [{:route-id 1 :raft-group-id 1 :start "" :end "p"
             :state "ROUTE_STATE_ACTIVE"}
            {:route-id 2 :raft-group-id 1 :start "p" :end nil
             :state "ROUTE_STATE_ACTIVE"}]})

(deftest server-flags-enable-automatic-split
  (let [args (ekdb/server-args {:node "n1" :grpc "n1:50051" :redis "n1:6379"
                               :data-dir "/tmp/raft" :raft-redis-map "n1:50051=n1:6379"
                               :auto-split-args hotspot/auto-split-args})]
    (is (some #{"--autoSplit"} args))
    (is (some #{"--keyvizStep=5s"} args))
    (is (some #{"--autoSplitThreshold=60"} args))
    (is (some #{"--autoSplitCandidateWindows=3"} args))
    (is (not-any? #{"--autoSplitCrossGroup"} args))))

(deftest managed-test-wires-controller-and-faults
  (let [test (hotspot/elastickv-hotspot-test {})
        fs (nemesis/fs (:nemesis test))]
    (is (= hotspot/auto-split-args (get-in test [:db :opts :auto-split-args])))
    (is (nil? (:final-generator test)))
    (is (= 24 (:concurrency test)))
    (is (= 12 (count hotspot/register-keys)))
    (is (every? (set hotspot/register-keys) ["a-hot-0" "m-hot-0" "z-hot-0"]))
    (is (every? fs #{:capture-hotspot-baseline :observe-hotspot-split
                     :start-partition :stop-partition :kill :start}))))

(deftest managed-test-forwards-encryption-and-validates-concurrency
  (is (true? (get-in (hotspot/elastickv-hotspot-test {:encryption true})
                     [:db :opts :encryption])))
  (is (thrown-with-msg? clojure.lang.ExceptionInfo
                        #"even concurrency covering all nodes and registers"
                        (hotspot/elastickv-hotspot-test {:concurrency 5})))
  (is (thrown-with-msg? clojure.lang.ExceptionInfo
                        #"even concurrency covering all nodes and registers"
                        (hotspot/elastickv-hotspot-test
                          {:nodes ["n1" "n2" "n3" "n4" "n5" "n6" "n7"]
                           :concurrency 6})))
  (with-redefs [cli/run-workload!
                (fn [_ options _ _]
                  (is (= 24 (:default (apply hash-map
                                            (drop 3 (first (filter #(= "--concurrency N"
                                                                       (second %)) options))))))))]
    (hotspot/-main)))

(def fault-nodes ["n1" "n2" "n3"])

(deftest managed-client-setup-retries-until-leader-is-ready
  (let [attempts (atom 0)
        delegate (reify client/Client
                   (open! [this _ _] this)
                   (setup! [this _]
                     (when (= 1 (swap! attempts inc))
                       (throw (ex-info "leader not elected" {})))
                     this)
                   (invoke! [_ _ op] (assoc op :type :ok))
                   (teardown! [this _] this)
                   (close! [this _] this))
        wrapped (client/open! (hotspot/->StartupRetryClient delegate) {} "n1")]
    (is (some? (client/setup! wrapped {})))
    (is (= 2 @attempts))))

(defn fault-history [fault]
  (let [[start stop] (hotspot/fault-ops fault)]
    (vec (concat
           (mapcat (fn [node]
                     (for [f [start stop]
                           type [:info :info]]
                       {:process :nemesis :type type :f f :target node
                        :value (if (= f start) [node] :all)})) fault-nodes)
           [{:process :nemesis :type :info :f stop :recovery true :value :all}
            {:process :nemesis :type :info :f stop :recovery true :value :all}]))))

(deftest single-managed-fault-covers-every-node
  (doseq [fault [:partition :kill]]
    (let [test (hotspot/elastickv-hotspot-test
                 {:nodes fault-nodes :faults [fault] :fault-interval 1 :time-limit 20})
          package (hotspot/deterministic-fault-package (:db test) fault-nodes fault 1)
          [heal _] (gen/op (:final-generator package) test (gen/context test))
          ops (filter map? (hotspot/fault-cycle fault-nodes fault 1))
          [start stop] (hotspot/fault-ops fault)
          starts (filter #(= start (:f %)) ops)]
      (is (= fault-nodes (mapv :target starts)))
      (is (= fault-nodes (mapv :target (filter #(= stop (:f %)) ops))))
      (is (= stop (:f heal)))
      (is (= "n1=n1:50051,n2=n2:50051,n3=n3:50051"
             (get-in test [:db :opts :bootstrap-members])))
      (is (= os/noop (:os test)))
      (is (= :all (:value heal)))
      (is (true? (:recovery heal)))
      (is (every? (set (nemesis/fs (:nemesis test)))
                  #{:capture-hotspot-baseline :observe-hotspot-split start stop}))
      (is (nil? (:final-generator test)))
      (let [[first-op _] (gen/op (:generator test) test (gen/context test))]
        (is (= :capture-hotspot-baseline (:f first-op))))
      (when (= :partition fault)
        (doseq [{:keys [target value]} starts]
          (is (= (set (remove #{target} fault-nodes)) (set (value target))))))))
  (doseq [bad [{:nodes ["n1" "n2"]} {:nodes ["n1" "n2" "n4"]}
               {:fault-interval 0} {:recovery-time 0} {:time-limit 7}]]
    (is (thrown? clojure.lang.ExceptionInfo
                 (hotspot/elastickv-hotspot-test
                   (merge {:nodes fault-nodes :faults [:kill]
                           :fault-interval 1 :time-limit 20} bad))))))

(deftest fault-checker-rejects-missing-or-failed-cycles
  (doseq [fault [:partition :kill]]
    (let [good (fault-history fault)
          check #(checker/check (hotspot/fault-cycle-checker fault-nodes fault)
                                {} (history/history %) {})]
      (is (true? (:valid? (check good))))
      (is (= (zipmap fault-nodes (repeat 1))
             (:completed-fault-cycles (check good))))
      (is (false? (:valid? (check (subvec good 0 12)))))
      (is (false? (:valid? (check (vec (concat (subvec good 0 8)
                                               (subvec good 12)))))))
      (is (false? (:valid? (check (assoc-in good [3 :type] :fail)))))
      (is (false? (:valid? (check (assoc-in good [3 :error] :unavailable)))))
      (is (false? (:valid? (check (assoc-in good [2 :error] :unavailable)))))
      (is (false? (:valid? (check (assoc good 3 (assoc (good 3) :target "n2"))))))
      (is (false? (:valid? (check (subvec good 0 13)))))
      (is (false? (:valid? (check (vec (concat (subvec good 0 10)
                                               (subvec good 12 14)
                                               (subvec good 10 12))))))))))

(deftest final-reads-follow-the-explicit-heal
  (let [writes (mapv (fn [key-index]
                       {:type :ok :f :write :value [key-index key-index]})
                     (range (count hotspot/register-keys)))
        observed-split {:process :nemesis :type :info :f :observe-hotspot-split
                        :value {:before before :after after}}
        heal {:process :nemesis :type :info :f :start :recovery true :value :all}
        reads (vec (for [node fault-nodes
                         key-index (range (count hotspot/register-keys))]
                     {:type :ok :f :read :node node :final true
                      :value [key-index key-index]}))
        check #(checker/check (hotspot/final-read-checker :start)
                              {:nodes fault-nodes} (history/history %) {})]
    (is (true? (:valid? (check (concat writes [observed-split heal] reads)))))
    (is (false? (:valid? (check (concat writes [observed-split] reads)))))
    (is (false? (:valid? (check (concat writes [observed-split]
                                       (take 1 reads) [heal] (rest reads))))))
    (is (false? (:valid? (check (concat writes [observed-split]
                                       [(assoc heal :type :fail)] reads)))))))

(deftest observation-rejects-controller-failures
  (is (some? (hotspot/split-observation before after)))
  (is (nil? (hotspot/split-observation before before)))
  (is (nil? (hotspot/split-observation before (assoc after :catalog-version 1))))
  (is (nil? (hotspot/split-observation before
            (assoc-in after [:routes 1 :start] "m4-n"))))
  (is (nil? (hotspot/split-observation before
            (assoc-in after [:routes 1 :raft-group-id] 2))))
  (is (nil? (hotspot/split-observation before
            (-> after
                (assoc-in [:routes 0 :end] "zz")
                (assoc-in [:routes 1 :start] "zz"))))))

(deftest controller-checker-requires-a-matched-success
  (let [capture {:process :nemesis :type :info :f :capture-hotspot-baseline :value before}
        observe {:process :nemesis :type :info :f :observe-hotspot-split
                 :value {:before before :after after}}
        check #(checker/check (hotspot/controller-checker) {}
                              (history/history %) {})]
    (is (true? (:valid? (check [capture observe]))))
    (is (true? (:valid? (check [(assoc capture :value nil) capture observe]))))
    (is (false? (:valid? (check []))))
    (is (false? (:valid? (check [capture]))))
    (is (false? (:valid? (check [observe capture]))))
    (is (false? (:valid? (check [capture (assoc observe :type :fail :error :timeout)]))))
    (is (false? (:valid? (check [capture (assoc-in observe [:value :after] before)]))))))

(deftest missing-baseline-and-route-helper-failure
  (let [n (hotspot/controller-nemesis {:list-routes-bin "missing"})]
    (is (= :info (:type (nemesis/invoke! n {:nodes ["n1"]}
                                        {:type :info :f :observe-hotspot-split})))))
  (with-redefs [shell/sh (fn [& _] {:exit 1 :out "" :err "unavailable"})]
    (let [n (hotspot/controller-nemesis {:list-routes-bin "list-routes"})]
      (is (= :info (:type (nemesis/invoke! n {:nodes ["n1"]}
                                          {:type :info :f :capture-hotspot-baseline})))))))

(deftest observation-has-a-deadline
  (with-redefs [hotspot/route-snapshot! (fn [& _] before)]
    (let [n (hotspot/controller-nemesis {:observe-timeout-ms 0})
          test {:nodes ["n1"]}]
      (is (= :info (:type (nemesis/invoke! n test
                                          {:type :info :f :capture-hotspot-baseline}))))
      (is (= :info (:type (nemesis/invoke! n test
                                          {:type :info :f :observe-hotspot-split})))))))

(deftest transient-route-observation-is-pending-until-deadline
  (let [calls (atom 0)
        n (hotspot/controller-nemesis {:observe-timeout-ms 1000})
        test {:nodes ["n1"]}]
    (with-redefs [hotspot/route-snapshot!
                  (fn [& _]
                    (case (swap! calls inc)
                      1 before
                      2 (throw (ex-info "node unavailable" {}))
                      after))]
      (nemesis/invoke! n test {:type :info :f :capture-hotspot-baseline})
      (is (= :info (:type (nemesis/invoke! n test
                                          {:type :info :f :observe-hotspot-split}))))
      (is (map? (:value (nemesis/invoke! n test
                                          {:type :info :f :observe-hotspot-split}))))))
  (let [calls (atom 0)
        n (hotspot/controller-nemesis {:observe-timeout-ms 0})
        test {:nodes ["n1"]}]
    (with-redefs [hotspot/route-snapshot!
                  (fn [& _]
                    (if (= 1 (swap! calls inc)) before
                        (throw (ex-info "node unavailable" {}))))]
      (nemesis/invoke! n test {:type :info :f :capture-hotspot-baseline})
      (is (= :info (:type (nemesis/invoke! n test
                                          {:type :info :f :observe-hotspot-split})))))))

(deftest transitional-route-geometry-is-pending-until-convergence
  (let [snapshots (atom [before
                         {:catalog-version 2
                          :routes (conj (:routes after)
                                        (first (:routes before)))}
                         after])
        n (hotspot/controller-nemesis {:observe-timeout-ms 1000})
        test {:nodes ["n1"]}]
    (with-redefs [hotspot/route-snapshot!
                  (fn [& _]
                    (let [snapshot (first @snapshots)]
                      (swap! snapshots rest)
                      snapshot))]
      (nemesis/invoke! n test {:type :info :f :capture-hotspot-baseline})
      (let [pending (nemesis/invoke! n test
                                     {:type :info :f :observe-hotspot-split})]
        (is (= :info (:type pending)))
        (is (nil? (:error pending)))
        (is (nil? (:value pending))))
      (is (map? (:value (nemesis/invoke! n test
                                          {:type :info :f :observe-hotspot-split})))))))

(deftest controller-observation-does-not-block-faults
  (let [snapshot (atom before)
        n (hotspot/controller-nemesis {:observe-timeout-ms 1000})
        test {:nodes ["n1"]}]
    (with-redefs [hotspot/route-snapshot! (fn [& _] @snapshot)]
      (is (= :info (:type (nemesis/invoke! n test
                                          {:type :info :f :capture-hotspot-baseline}))))
      (let [pending (nemesis/invoke! n test {:type :info :f :observe-hotspot-split})]
        (is (= :info (:type pending)))
        (is (nil? (:value pending))))
      (reset! snapshot after)
      (is (some? (:value (nemesis/invoke! n test
                                         {:type :info :f :observe-hotspot-split})))))))

(deftest final-reads-require-all-nodes-and-written-keys
  (let [writes (mapv (fn [key-index]
                       {:type :ok :f :write :value [key-index key-index]})
                     (range (count hotspot/register-keys)))
        observed-split {:process :nemesis :type :info :f :observe-hotspot-split
                        :value {:before before :after after}}
        reads (vec (for [node ["n1" "n2"]
                         key-index (range (count hotspot/register-keys))]
                     {:type :ok :f :read :node node :final true
                      :value [key-index key-index]}))
        check #(checker/check (hotspot/final-read-checker) {:nodes ["n1" "n2"]}
                              (history/history %) {})]
    (is (true? (:valid? (check (concat writes [observed-split] reads)))))
    (is (false? (:valid? (check (concat writes [observed-split] (butlast reads))))))
    (is (false? (:valid? (check (concat (butlast writes) [observed-split] reads)))))
    (is (false? (:valid? (check (concat writes reads [observed-split])))))))
