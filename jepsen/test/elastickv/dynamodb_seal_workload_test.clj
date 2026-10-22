(ns elastickv.dynamodb-seal-workload-test
  (:require [clojure.java.shell :as shell]
            [clojure.test :refer :all]
            [cognitect.aws.client.api :as aws]
            [elastickv.dynamodb-seal-workload :as seal]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.db :as db]
            [jepsen.nemesis :as nemesis]))

(deftest requires-recorded-successful-leader-change
  (let [check #(checker/check (seal/leader-change-checker) {} % {})
        op {:process :nemesis :type :info :f :leader-transfer}]
    (is (not (:valid? (check []))))
    (is (not (:valid? (check [(assoc op :value {:from "a" :to "a"})]))))
    (is (:valid? (check [(assoc op :value {:from "a" :to "b"})])))
    (is (false? (:valid? (check [(assoc op :value {:from "a" :to "b"})
                                (assoc op :error "transfer failed")]))))))

(deftest records-actual-transfer-and-failure
  (let [leader (atom "a")
        n (seal/leader-transfer-nemesis "/raftadmin" ["a" "b"])
        op {:type :info :f :leader-transfer}]
    (with-redefs [shell/sh (fn [binary address command]
                            (is (= "/raftadmin" binary))
                            (case command
                              "state" {:exit 0 :out (str "state: " (if (= address @leader) "LEADER" "FOLLOWER") "\n")}
                              "leadership_transfer" (do (reset! leader "b") {:exit 0 :out "ok\n"})))]
      (is (= {:from "a" :to "b"} (:value (nemesis/invoke! n {} op)))))
    (with-redefs [shell/sh (fn [& _] {:exit 1 :err "RPC failed"})]
      (is (= "raftadmin failed" (:error (nemesis/invoke! n {} op)))))))

(deftest acceptance-is-local-and-opt-in
  (is (thrown? clojure.lang.ExceptionInfo (seal/seal-test {})))
  (let [test (seal/seal-test {:local true :raftadmin-bin "/raftadmin" :raft-addresses ["a" "b"]})]
    (is (= "elastickv-dynamodb-snapshot-seal" (:name test)))
    (is (= #{:leader-transfer} (nemesis/fs (:nemesis test))))))

(deftest resets-the-append-table-once-before-client-setup
  (let [calls (atom [])
        closed (atom 0)
        test {:nodes ["n1" "n2"] :client ::client}]
    (with-redefs [client/open! (fn [& _] {:ddb ::ddb})
                  client/close! (fn [& _] (swap! closed inc))
                  aws/invoke (fn [ddb request]
                               (is (= ::ddb ddb))
                               (swap! calls conj request)
                               {})]
      (doseq [node (:nodes test)] (db/setup! (seal/fresh-append-db) test node))
      (is (= [{:op :DeleteTable :request {:TableName "jepsen_append"}}] @calls))
      (is (= 1 @closed)))))

(deftest reset-only-ignores-missing-table-and-always-closes
  (doseq [[response fails?] [[{:cognitect.anomalies/category :not-found
                             :__type "ResourceNotFoundException"} false]
                           [{:cognitect.anomalies/category :fault
                             :__type "InternalServerError"} true]]]
    (let [closed (atom false)]
      (with-redefs [client/open! (fn [& _] {:ddb ::ddb})
                    client/close! (fn [& _] (reset! closed true))
                    aws/invoke (fn [& _] response)]
        (let [run #(db/setup! (seal/fresh-append-db) {:nodes ["n1"]} "n1")]
          (if fails?
            (is (thrown? clojure.lang.ExceptionInfo (run)))
            (is (nil? (run)))))
        (is @closed)))))
