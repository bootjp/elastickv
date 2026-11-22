(ns elastickv.split-preflight-workload-test
  (:require [clojure.test :refer [deftest is]]
            [elastickv.split-preflight-workload :as pf]
            [jepsen.checker :as checker]))

(deftest membership-probes-retry-only-watermark-timeouts
  (let [invoke #'pf/invoke-membership-operation
        timeout "read register: rpc error: code = Unknown desc = grpc latest read: read fence: tso commit floor: group 1: read data group 1 leader 127.0.0.1:25101 watermark: rpc error: code = DeadlineExceeded desc = context deadline exceeded: tso: authoritative data-group commit floor is unavailable"
        unavailable "read register: rpc error: code = Unavailable desc = Route unavailable"
        op {:f :read :tag :stale :value [1 nil]}
        run (fn [operation responses]
              (let [remaining (atom responses)
                    result (invoke (fn [_]
                                     (let [response (first @remaining)]
                                       (swap! remaining rest)
                                       response))
                                   operation)]
                [result (count @remaining)]))]
    (is (= [{:type :info :error unavailable :watermark-timeouts [timeout]} 0]
           (run op [{:type :info :error timeout} {:type :info :error unavailable}])))
    (is (= [{:type :ok :value [1 4202]} 1]
           (run op [{:type :ok :value [1 4202]} {:type :info :error unavailable}])))
    (is (= [{:type :info :error "unrelated"} 1]
           (run op [{:type :info :error "unrelated"} {:type :info :error unavailable}])))
    (is (= [{:type :info :error timeout} 1]
           (run (assoc op :f :write) [{:type :info :error timeout} {:type :ok}])))
    (is (= [{:type :info :error timeout} 1]
           (run (assoc op :tag :member-read) [{:type :info :error timeout} {:type :ok}])))
    (is (= [{:type :info :error timeout :watermark-timeouts [timeout timeout]} 1]
           (run op (concat (repeat 3 {:type :info :error timeout})
                           [{:type :info :error unavailable}]))))))

(defn- history []
  (let [a {:job "7" :scan-ts "11" :phase "SPLIT_JOB_PHASE_FENCE"
           :fence "0" :plan #{1 2 3} :completed #{1} :done false :source "1" :target "2"}
        b (assoc a :completed #{1 2})
        c (assoc b :completed #{1 2 3} :done true :phase "SPLIT_JOB_PHASE_DONE")
        cut "amVwc2VuLWdycGMvcmF3LzE="
        routes [{:start "" :end cut :raft_group_id 1 :state "ROUTE_STATE_ACTIVE"}
                {:start cut :end "" :raft_group_id 2 :state "ROUTE_STATE_ACTIVE"}]
        read-ops (fn [tag nodes] (for [n nodes [k v] pf/seeds]
                                  {:type :ok :f :read :node n tag true :value [k v]}))]
    (mapv #(assoc %1 :index %2)
          (concat (for [[k v] pf/seeds] {:type :ok :f :write :value [k v]})
                  [{:f :pause-preflight :confirmed true
                    :value {:leader "n1" :victim "n2" :state "T" :before a :during b}}]
                  (read-ops :during ["n1"])
                  [{:f :resume-voter :confirmed true :value {:victim "n2" :state "S"}}
                   {:f :finish-split :confirmed true :value {:final c :routes routes}}]
                  (read-ops :final pf/nodes))
          (range))))

(deftest require-overlap-and-recovery
  (let [h (history)
        valid? #(:valid? (checker/check (pf/recovery-checker) {} % {}))]
    (is (valid? h))
    (is (valid? (assoc-in h [4 :type] :info)) "the moving range may fail closed")
    (is (false? (valid? (assoc-in h [3 :type] :info))) "the retained range must serve a read")
    (doseq [[path bad] [[[2 :value :state] "S"]
                        [[2 :value :before :completed] #{}]
                        [[2 :value :during :done] true]
                        [[2 :value :during :completed] #{2}]
                        [[2 :value :during :scan-ts] "12"]
                        [[6 :value :final :job] "8"]
                        [[6 :value :final :done] false]
                        [[6 :value :routes 1 :raft_group_id] 1]
                        [[7 :value] [0 -1]]]]
      (is (false? (valid? (assoc-in h path bad))) (str path)))
    (is (false? (valid? (pop h))))
    (is (false? (valid? (assoc-in h [2 :confirmed] false))))
    (let [last-sample (assoc (get-in h [2 :value :during]) :capability-regressed true)
          timeout (assoc h 6 {:f :finish-split :process :nemesis :error "timeout" :details last-sample})
          result (checker/check (pf/recovery-checker) {} timeout {})]
      (is (false? (:valid? result)))
      (is (= {:phase "SPLIT_JOB_PHASE_FENCE" :completed 2 :total 3 :preflight-done false
              :capability-regressed true} (:last-checkpoint result))))))

(defn- failover-history []
  (let [start {:job "9" :scan-ts "11" :phase "SPLIT_JOB_PHASE_FENCE" :fence "0"
               :plan #{1 2 3} :completed #{1} :done false :source "1" :target "2"
               :online-version 1 :online-sequence "0" :seal-sequence "0" :online-manifest "manifest"}
        after (assoc start :completed #{1 2})
        done (assoc after :completed #{1 2 3} :done true :phase "SPLIT_JOB_PHASE_DONE"
                          :online-sequence "3" :seal-sequence "3")
        cut "amVwc2VuLWdycGMvcmF3LzE="
        routes [{:start "" :end cut :raft_group_id 1 :state "ROUTE_STATE_ACTIVE"}
                {:start cut :end "" :raft_group_id 2 :state "ROUTE_STATE_ACTIVE"}]]
    (mapv #(assoc %1 :index %2)
          (concat
            [{:type :ok :f :write :tag :seed :value [0 4101]}
             {:type :ok :f :write :tag :seed :value [1 4202]}
             {:type :ok :f :write :tag :seed :value [2 4303]}
             {:f :start-online :confirmed true :value {:leader "n1" :term 4 :sample start}}
             {:type :ok :f :write :tag :captured :value [1 5101]}
             {:type :ok :f :delete :tag :captured :value [2 nil]}
             {:f :failover :confirmed true :value {:victim "n1" :leader "n2" :state "T"
                                                  :term 5 :before start :during after}}
             {:f :resume-voter :confirmed true :value {:victim "n1" :state "S"}}
             {:f :finish-split :confirmed true :value {:final done :routes routes}}
             {:type :ok :f :write :tag :post-cutover :value [3 5303]}]
            (for [n pf/nodes [k v] [[0 4101] [1 5101] [2 nil] [3 5303]]]
              {:type :ok :f :read :final true :node n :value [k v]}))
          (range))))

(deftest online-leader-failover-requires-acked-changes-and-all-ingress-readback
  (let [h (failover-history)
        valid? #(:valid? (checker/check (pf/online-failover-checker) {} % {}))]
    (is (valid? h))
    (doseq [[path bad] [[[4 :type] :info]
                        [[5 :type] :info]
                        [[6 :value :leader] "n1"]
                        [[6 :value :term] 4]
                        [[6 :value :term] nil]
                        [[7 :value :state] nil]
                        [[3 :index] 8]
                        [[6 :value :during :online-manifest] "another generation"]
                        [[6 :value :during :online-version] 0]
                        [[8 :value :final :seal-sequence] "4"]
                        [[9 :type] :info]
                        [[12 :value] [2 4303]]
                        [[12 :node] "n2"]]]
      (is (false? (valid? (assoc-in h path bad))) (str path)))
    (is (false? (valid? (pop h))))))

(defn- target-membership-history []
  (let [source (failover-history)
        start (nth source 3)
        finish (nth source 8)]
    (mapv #(assoc %1 :index %2)
          (concat
            [{:f :prepare-member :confirmed true :value {:catalog-leader "n1" :initial-target-leader "n3"}}
             (source 0) (source 1) (source 2) start (source 4) (source 5)
             {:f :promote-before-publish :confirmed true
              :value {:job "9" :certified 1 :add-index 71 :promote-index 73
                      :learner-role "learner" :voter-role "voter" :threshold 72
                      :learner-applied 72 :voter-applied 73 :old-version 3
                      :seal 3 :attempt 100 :before-owner 1 :after-promotion-owner 1}}
             {:f :await-publication :confirmed true
              :value {:job "9" :version 4 :old-ingress "n3" :cached-version 3
                      :old-owner 1 :new-owner 2 :blocked-version 4 :attempt 100
                      :new-member-version 5 :initial-target-leader "n3"}}
             {:f :read :tag :stale :type :info :node "n3" :value [1 nil]
              :error "rpc error: code = Unavailable desc = Route unavailable"}
             {:f :read :tag :stale :type :info :node "n3" :value [2 nil]
              :error "rpc error: code = Unavailable desc = Route unavailable"}
             {:f :read :tag :new-while-stale :type :ok :node "n4" :value [1 5101]}
             {:f :read :tag :new-while-stale :type :ok :node "n4" :value [2 nil]}
             {:f :lead-new-member :confirmed true :value {:leader "n4" :target-applied 81}}
             {:f :read :tag :joined-leader :type :ok :node "n4" :value [1 5101]}
             {:f :read :tag :joined-leader :type :ok :node "n4" :value [2 nil]}
             (-> finish (assoc :f :heal-member-views)
                 (update :value assoc :healed-version 5 :target-leader "n4" :target-applied 82))
             (source 9)]
            (for [node ["n3" "n4"] [k v] [[0 4101] [1 5101] [2 nil] [3 5303]]]
              {:f :read :tag :member-read :type :ok :node node :value [k v]}))
          (range))))

(deftest target-membership-requires-a-paused-cutover-and-correct-recovered-reads
  (let [h (target-membership-history)
        valid? #(:valid? (checker/check (pf/membership-checker) {} % {}))]
    (is (valid? h))
    (doseq [[path bad] [[[5 :type] :info]
                        [[7 :value :voter-role] "learner"]
                        [[7 :value :learner-applied] 71]
                        [[7 :value :after-promotion-owner] 2]
                        [[8 :value :attempt] 101]
                        [[8 :value :cached-version] 4]
                        [[9 :error] "rpc error: code = Unknown"]
                        [[9 :error] "rpc error: code = Unavailable desc = connection refused"]
                        [[10 :type] :ok]
                        [[11 :value] [1 4202]]
                        [[12 :value] [2 4303]]
                        [[13 :value :leader] "n2"]
                        [[14 :type] :info]
                        [[15 :value] [2 4303]]
                        [[16 :value :target-leader] "n2"]
                        [[20 :value] [2 4303]]]]
      (let [bad-history (cond-> (assoc-in h path bad)
                          (= path [10 :type]) (assoc-in [10 :value] [2 4303]))]
        (is (false? (valid? bad-history)) (str path))))
    (is (false? (valid? (pop h))))))

(deftest target-membership-requires-evidence-of-the-selected-fault
  (let [base (target-membership-history)
        cert [7 :value :fault]
        valid? (fn [fault h] (:valid? (checker/check (pf/membership-checker fault) {} h {})))]
    (doseq [[kind evidence corrupt]
            [[:kill {:kind :kill :victim "n3" :exit-code 137 :killed-pid 54 :restarted-pid 81
                     :old-term 4 :replacement "n4" :replacement-term 5 :restart-applied 73}
              {:exit-code 0 :restarted-pid 54 :replacement-term 4 :restart-applied 72}]
             [:partition {:kind :partition :victim "n4" :blocked-sender "n3" :threshold 72
                          :isolated-applied 70 :rejected-index 71 :rejection-gate :namespace-admission
                          :rejection "code = FailedPrecondition desc = namespace member 4 support/configuration/activation unproven: namespace admission protocol is not proven for this configuration"}
              {:blocked-sender nil :isolated-applied 72 :rejected-index 72 :rejection-gate :catch-up
               :rejection "context deadline exceeded"}]
             [:pause {:kind :pause :victim "n4" :pid 54 :resumed-pid 54
                      :stopped-state "T" :publication-state "T" :resumed-state "S" :published-version 4}
              {:publication-state "S" :resumed-state "T" :resumed-pid 81 :published-version 3}]]]
      (let [with-fault (assoc-in base cert evidence)]
        (is (valid? kind with-fault) (str kind))
        (is (false? (valid? kind base)) (str kind " with no injection"))
        (is (false? (valid? :none with-fault)) (str kind " under baseline mode"))
        (is (false? (valid? kind (assoc-in with-fault [20 :value] [2 4303]))) (str kind " with a resurrected delete"))
        (doseq [[k bad] corrupt]
          (is (false? (valid? kind (assoc-in base cert (assoc evidence k bad)))) (str kind " " k)))))))
