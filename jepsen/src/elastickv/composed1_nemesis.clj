(ns elastickv.composed1-nemesis
  "Route-shuffle nemesis for the Composed-1 M5 DynamoDB workload."
  (:require [clojure.data.json :as json]
            [clojure.java.shell :as shell]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.generator :as gen]
            [jepsen.nemesis :as nemesis])
  (:import (java.nio.charset StandardCharsets)
           (java.util Base64)))

(def ^:private default-list-routes-bin "elastickv-list-routes")
(def ^:private default-split-bin "elastickv-split")
(def ^:private default-grpc-port 50051)

(def ^:private route-shuffle-anchor-tables
  ["jepsen_append_t4"
   "jepsen_append_t3"
   "jepsen_append_t2"
   "jepsen_append_t1"])

(def ^:private split-suffixes
  (seq "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"))

(defn encode-dynamo-segment
  "Mirrors adapter/dynamodb.go's base64.RawURLEncoding segment encoder."
  [s]
  (let [encoder (.withoutPadding (Base64/getUrlEncoder))]
    (.encodeToString encoder (.getBytes (str s) StandardCharsets/UTF_8))))

(defn dynamo-table-route-key
  "Returns the route key for a DynamoDB table name."
  [table-name]
  (str "!ddb|route|table|" (encode-dynamo-segment table-name)))

(defn- decode-route-boundary [s infinity?]
  (cond
    (and infinity? (str/blank? s)) nil
    (str/blank? s) ""
    :else (String. (.decode (Base64/getDecoder) s) StandardCharsets/UTF_8)))

(defn- decode-route [route]
  {:route-id      (long (:route_id route))
   :raft-group-id (long (:raft_group_id route))
   :start         (decode-route-boundary (:start route) false)
   :end           (decode-route-boundary (:end route) true)
   :state         (:state route)})

(defn parse-routes-json
  "Parses elastickv-list-routes JSON output into route maps with string bounds."
  [json-str]
  (let [parsed (json/read-str json-str :key-fn keyword)]
    {:catalog-version (long (:catalog_version parsed))
     :routes          (mapv decode-route (:routes parsed))}))

(defn route-covers?
  "True when route's half-open range contains key."
  [{:keys [start end state]} key]
  (and (= "ROUTE_STATE_ACTIVE" state)
       (not (pos? (compare start key)))
       (or (nil? end) (neg? (compare key end)))))

(defn route-containing-key
  [routes key]
  (first (filter #(route-covers? % key) routes)))

(defn fresh-interior-split-key
  "Returns a key strictly inside route, or nil if the route is too narrow."
  ([route]
   (fresh-interior-split-key route (System/nanoTime)))
  ([route counter]
   (some (fn [suffix]
           (let [candidate (str (:start route) suffix counter)]
             (when (route-covers? route candidate)
               candidate)))
         split-suffixes)))

(defn plan-route-shuffle
  "Chooses a current route and an interior split key for one shuffle."
  ([snapshot]
   (plan-route-shuffle snapshot {}))
  ([{:keys [catalog-version routes] :as _snapshot}
    {:keys [anchor-tables counter]
     :or   {anchor-tables route-shuffle-anchor-tables
            counter       (System/nanoTime)}}]
   (some (fn [table]
           (let [anchor-key (dynamo-table-route-key table)]
             (when-let [route (route-containing-key routes anchor-key)]
               (when-let [split-key (fresh-interior-split-key route counter)]
                 {:catalog-version catalog-version
                  :route-id        (:route-id route)
                  :raft-group-id   (:raft-group-id route)
                  :anchor-table    table
                  :anchor-key      anchor-key
                  :split-key       split-key}))))
         anchor-tables)))

(defn- default-grpc-host-port-for [test]
  (let [node (first (:nodes test))]
    (if node
      (str (name node) ":" default-grpc-port)
      (str "127.0.0.1:" default-grpc-port))))

(defn- runtime-opts [opts test]
  (merge {:list-routes-bin default-list-routes-bin
          :split-bin       default-split-bin
          :grpc-host-port  (default-grpc-host-port-for test)}
         (select-keys test [:list-routes-bin :split-bin :grpc-host-port])
         opts))

(defn- read-route-snapshot! [{:keys [list-routes-bin grpc-host-port]}]
  (let [result (shell/sh list-routes-bin "--address" grpc-host-port)]
    (when-not (zero? (:exit result))
      (throw (ex-info (str list-routes-bin " --address " grpc-host-port " failed")
                      {:exit   (:exit result)
                       :stdout (:out result)
                       :stderr (:err result)})))
    (parse-routes-json (:out result))))

(defn- run-split! [{:keys [split-bin grpc-host-port]} plan]
  (let [result (shell/sh split-bin
                         "--address" grpc-host-port
                         "--route-id" (str (:route-id plan))
                         "--split-key" (:split-key plan)
                         "--expected-version" (str (:catalog-version plan)))]
    (when-not (zero? (:exit result))
      (throw (ex-info (str split-bin " SplitRange failed")
                      {:exit   (:exit result)
                       :stdout (:out result)
                       :stderr (:err result)
                       :plan   plan})))
    result))

(defn route-shuffle-nemesis
  "Builds a nemesis that performs one SplitRange per :route-shuffle op."
  [opts]
  (let [counter (atom 0)]
    (reify
      nemesis/Reflection
      (fs [_this] #{:route-shuffle})

      nemesis/Nemesis
      (setup! [this _test] this)

      (invoke! [_this test op]
        (case (:f op)
          :route-shuffle
          (try
            (let [opts     (runtime-opts opts test)
                  snapshot (read-route-snapshot! opts)
                  plan     (plan-route-shuffle
                              snapshot
                              {:counter (swap! counter inc)
                               :anchor-tables (:route-shuffle-anchor-tables opts)})]
              (if-not plan
                (do
                  (warn "route-shuffle skipped: no active route has an interior split key")
                  (assoc op :value {:skipped :no-interior-route}))
                (let [result (run-split! opts plan)]
                  (info "route-shuffle split"
                        (:split-key plan)
                        "route" (:route-id plan)
                        "version" (:catalog-version plan))
                  (assoc op :value (merge plan
                                           {:stdout (:out result)
                                            :stderr (:err result)})))))
            (catch Throwable t
              (warn t "route-shuffle failed")
              (assoc op :value {:error (.getMessage t)})))

          op))

      (teardown! [this _test] this))))

(defn route-shuffle-package
  "A combined.nemesis-compatible package for the route-shuffle nemesis."
  [opts]
  {:generator (when (:composed1-route-shuffle opts)
                (->> (gen/repeat {:type :info, :f :route-shuffle})
                     (gen/stagger (or (:route-shuffle-interval opts) 30))))
   :final-generator nil
   :nemesis   (route-shuffle-nemesis opts)
   :perf      #{{:name  "route-shuffle"
                 :fs    #{:route-shuffle}
                 :start #{:route-shuffle}
                 :stop  #{}
                 :color "#D4A0E9"}}})
