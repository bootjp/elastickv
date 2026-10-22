(ns elastickv.dynamodb-seal-workload-test
  (:require [clojure.java.shell :as shell]
            [clojure.test :refer :all]
            [elastickv.dynamodb-seal-workload :as seal]
            [jepsen.checker :as checker]
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
