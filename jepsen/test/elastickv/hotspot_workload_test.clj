(ns elastickv.hotspot-workload-test
  (:require [clojure.java.shell :as shell]
            [clojure.test :refer :all]
            [elastickv.cli :as cli]
            [elastickv.db :as ekdb]
            [elastickv.hotspot-workload :as hotspot]
            [jepsen.checker :as checker]
            [jepsen.history :as history]
            [jepsen.nemesis :as nemesis]))

(def before
  {:catalog-version 1
   :routes [{:route-id 1 :raft-group-id 1 :start "" :end nil
             :state "ROUTE_STATE_ACTIVE"}]})

(def after
  {:catalog-version 2
   :routes [{:route-id 1 :raft-group-id 1 :start "" :end "m4-m"
             :state "ROUTE_STATE_ACTIVE"}
            {:route-id 2 :raft-group-id 1 :start "m4-m" :end nil
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
    (is (= 6 (:concurrency test)))
    (is (every? fs #{:capture-hotspot-baseline :observe-hotspot-split
                     :start-partition :stop-partition :kill :start}))))

(deftest managed-test-forwards-encryption-and-validates-concurrency
  (is (true? (get-in (hotspot/elastickv-hotspot-test {:encryption true})
                     [:db :opts :encryption])))
  (is (thrown-with-msg? clojure.lang.ExceptionInfo
                        #"even concurrency of at least six"
                        (hotspot/elastickv-hotspot-test {:concurrency 5})))
  (with-redefs [cli/run-workload!
                (fn [_ options _ _]
                  (is (= 6 (:default (apply hash-map
                                            (drop 3 (first (filter #(= "--concurrency N"
                                                                       (second %)) options))))))))]
    (hotspot/-main)))

(deftest observation-rejects-controller-failures
  (is (some? (hotspot/split-observation before after)))
  (is (nil? (hotspot/split-observation before before)))
  (is (nil? (hotspot/split-observation before (assoc after :catalog-version 1))))
  (is (nil? (hotspot/split-observation before
            (assoc-in after [:routes 1 :start] "m4-n"))))
  (is (nil? (hotspot/split-observation before
            (assoc-in after [:routes 1 :raft-group-id] 2)))))

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
    (is (= :fail (:type (nemesis/invoke! n {:nodes ["n1"]}
                                        {:type :info :f :observe-hotspot-split})))))
  (with-redefs [shell/sh (fn [& _] {:exit 1 :out "" :err "unavailable"})]
    (let [n (hotspot/controller-nemesis {:list-routes-bin "list-routes"})]
      (is (= :fail (:type (nemesis/invoke! n {:nodes ["n1"]}
                                          {:type :info :f :capture-hotspot-baseline})))))))

(deftest observation-has-a-deadline
  (with-redefs [hotspot/route-snapshot! (fn [& _] before)]
    (let [n (hotspot/controller-nemesis {:observe-timeout-ms 0})
          test {:nodes ["n1"]}]
      (is (= :info (:type (nemesis/invoke! n test
                                          {:type :info :f :capture-hotspot-baseline}))))
      (is (= :fail (:type (nemesis/invoke! n test
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
      (is (= :fail (:type (nemesis/invoke! n test
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
