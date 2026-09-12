(ns elastickv.split-workload-test
  (:require [clojure.java.shell :as shell]
            [clojure.test :refer :all]
            [elastickv.split-workload :as split]
            [jepsen.nemesis :as nemesis]))

(defn- routes-json []
  (str "{\"catalog_version\":7,\"routes\":["
       "{\"route_id\":100,\"raft_group_id\":1,\"start\":\"\",\"end\":\"\",\"state\":\"ROUTE_STATE_ACTIVE\"}"
       "]}"))

(deftest split-package-is-deterministic
  (let [package (split/split-package {:split-at-seconds 1
                                      :split-verify-after-seconds 2})]
    (is (some? (:generator package)))
    (is (some? (:nemesis package)))
    (is (= #{:start-cross-group-split :verify-cross-group-split}
           (nemesis/fs (:nemesis package))))))

(deftest split-register-keys-cover-both-routes
  (let [split-key (var-get (ns-resolve 'elastickv.split-workload 'split-key))
        register-keys (var-get (ns-resolve 'elastickv.split-workload 'register-keys))]
    (is (some #(neg? (compare % split-key)) register-keys))
    (is (some #(not (neg? (compare % split-key))) register-keys))))

(deftest split-nemesis-starts-cross-group-job
  (let [calls (atom [])]
    (with-redefs [shell/sh (fn [& args]
                             (swap! calls conj args)
                             (if (.contains (first args) "list-routes")
                               {:exit 0 :out (routes-json) :err ""}
                               {:exit 0 :out "catalog_version: 8\njob_id: 44\n" :err ""}))]
      (let [result (nemesis/invoke!
                     (split/split-nemesis
                       {:list-routes-bin "elastickv-list-routes"
                        :split-bin "elastickv-split"
                        :target-group-id 2
                        :grpc-host-port "n1:50051"})
                     {:nodes ["n1"] :raft-groups {1 50051, 2 50052}}
                     {:type :info :f :start-cross-group-split})]
        (is (= 44 (get-in result [:value :job-id])))
        (is (= [["elastickv-list-routes" "--address" "n1:50051"]
                ["elastickv-split" "--address" "n1:50051"
                 "--route-id" "100" "--split-key" "m2-split"
                 "--expected-version" "7" "--target-group-id" "2"]]
               @calls))))))
