(ns elastickv.composed1-nemesis-test
  (:require [clojure.java.shell :as shell]
            [clojure.test :refer :all]
            [elastickv.composed1-nemesis :as nem]
            [jepsen.nemesis :as jepsen-nemesis]))

(def ^:private runtime-opts (var-get #'nem/runtime-opts))

(defn- b64 [s]
  (.encodeToString (java.util.Base64/getEncoder)
                   (.getBytes s java.nio.charset.StandardCharsets/UTF_8)))

(defn- routes-json []
  (str "{\"catalog_version\":7,\"routes\":["
       "{\"route_id\":100,\"raft_group_id\":1,\"start\":\"\",\"end\":\""
       (b64 (nem/dynamo-table-route-key "jepsen_append_t3"))
       "\",\"state\":\"ROUTE_STATE_ACTIVE\"},"
       "{\"route_id\":101,\"raft_group_id\":2,\"start\":\""
       (b64 (nem/dynamo-table-route-key "jepsen_append_t3"))
       "\",\"end\":\"\",\"state\":\"ROUTE_STATE_ACTIVE\"}"
       "]}"))

(deftest encode-dynamo-segment-matches-raw-url-base64
  (is (= "amVwc2VuX2FwcGVuZF90Mw"
         (nem/encode-dynamo-segment "jepsen_append_t3")))
  (is (= "amVwc2VuX2FwcGVuZF90NA"
         (nem/encode-dynamo-segment "jepsen_append_t4")))
  (is (not (re-find #"=" (nem/encode-dynamo-segment "jepsen_append_t4")))
      "DynamoDB route segments use RawURLEncoding without padding"))

(deftest parses-list-routes-json-boundaries
  (let [snapshot (nem/parse-routes-json (routes-json))
        routes   (:routes snapshot)]
    (is (= 7 (:catalog-version snapshot)))
    (is (= 2 (count routes)))
    (is (= "" (:start (first routes))))
    (is (= (nem/dynamo-table-route-key "jepsen_append_t3")
           (:end (first routes))))
    (is (= (nem/dynamo-table-route-key "jepsen_append_t3")
           (:start (second routes))))
    (is (nil? (:end (second routes))))))

(deftest finds-route-covering-anchor-key
  (let [snapshot (nem/parse-routes-json (routes-json))
        route    (nem/route-containing-key
                   (:routes snapshot)
                   (nem/dynamo-table-route-key "jepsen_append_t4"))]
    (is (= 101 (:route-id route)))
    (is (= 2 (:raft-group-id route)))))

(deftest fresh-interior-split-key-stays-inside-route
  (let [route {:start (nem/dynamo-table-route-key "jepsen_append_t3")
               :end   nil
               :state "ROUTE_STATE_ACTIVE"}
        split (nem/fresh-interior-split-key route 42)]
    (is (nem/route-covers? route split))
    (is (not= (:start route) split))))

(deftest fresh-interior-split-key-refuses-empty-left-range
  (let [route {:start ""
               :end   (nem/dynamo-table-route-key "jepsen_append_t3")
               :state "ROUTE_STATE_ACTIVE"}]
    (is (nil? (nem/fresh-interior-split-key route 42))
        "A... sorts after !ddb..., so the leftmost range needs another anchor")))

(deftest route-shuffle-plan-targets-current-route
  (let [snapshot (nem/parse-routes-json (routes-json))
        plan     (nem/plan-route-shuffle snapshot {:counter 9})]
    (is (= 7 (:catalog-version plan)))
    (is (= 101 (:route-id plan)))
    (is (= "jepsen_append_t4" (:anchor-table plan)))
    (is (nem/route-covers? (second (:routes snapshot)) (:split-key plan)))))

(deftest runtime-opts-ignore-nil-overrides
  (let [resolved (runtime-opts
                   {:list-routes-bin nil
                    :split-bin       nil
                    :grpc-host-port  nil}
                   {:nodes           [:n1 :n2]
                    :list-routes-bin nil
                    :split-bin       nil
                    :grpc-host-port  nil})]
    (is (= "elastickv-list-routes" (:list-routes-bin resolved)))
    (is (= "elastickv-split" (:split-bin resolved)))
    (is (= "n1:50051" (:grpc-host-port resolved)))))

(deftest route-shuffle-invoke-uses-defaults-when-opts-are-nil
  (let [calls (atom [])]
    (with-redefs [shell/sh (fn [& args]
                             (swap! calls conj args)
                             (case (first args)
                               "elastickv-list-routes"
                               {:exit 0 :out (routes-json) :err ""}

                               "elastickv-split"
                               {:exit 0 :out "split ok" :err ""}))]
      (let [result (jepsen-nemesis/invoke!
                     (nem/route-shuffle-nemesis
                       {:list-routes-bin nil
                        :split-bin       nil
                        :grpc-host-port  nil})
                     {:nodes           [:n1]
                      :list-routes-bin nil
                      :split-bin       nil
                      :grpc-host-port  nil}
                     {:type :info :f :route-shuffle})]
        (is (= :info (:type result)))
        (is (= "jepsen_append_t4" (get-in result [:value :anchor-table])))
        (is (= [["elastickv-list-routes" "--address" "n1:50051"]
                ["elastickv-split"
                 "--address" "n1:50051"
                 "--route-id" "101"
                 "--split-key" (get-in result [:value :split-key])
                 "--expected-version" "7"]]
               @calls))))))

(deftest route-shuffle-helper-failures-are-failed-ops
  (with-redefs [shell/sh (fn [& _]
                           {:exit 127 :out "" :err "missing helper"})]
    (let [result (jepsen-nemesis/invoke!
                   (nem/route-shuffle-nemesis {})
                   {:nodes [:n1]}
                   {:type :info :f :route-shuffle})]
      (is (= :fail (:type result)))
      (is (re-find #"elastickv-list-routes --address n1:50051 failed"
                   (:error result)))
      (is (= 127 (get-in result [:value :exit])))
      (is (= "missing helper" (get-in result [:value :stderr]))))))

(deftest route-shuffle-package-is-opt-in
  (is (nil? (:generator (nem/route-shuffle-package {}))))
  (is (some? (:generator (nem/route-shuffle-package
                           {:composed1-route-shuffle true
                            :route-shuffle-interval 1})))))
