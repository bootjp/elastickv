(ns elastickv.dynamodb-write-skew
  "Deterministic read/write antidependency history for the local two-group fixture."
  (:require [clojure.data.json :as json]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.generator :as gen])
  (:import (java.nio.charset StandardCharsets)
           (java.util Arrays Base64)
           (java.util.concurrent CyclicBarrier TimeUnit)))

;; The M5 layout routes tables 1 and 2 to group 1, and table 3 to group 2.
;; Cover distinct keys in one table, keys across groups, and two tables in group 1.
;; Each round has its own keys and markers for correlation with committed Raft entries.
(def rounds [{:round :same-group :keys [0 8] :tokens [101 102]}
             {:round :cross-group :keys [4 2] :tokens [201 202]}
             {:round :two-tables-same-group :keys [12 13] :tokens [301 302]}])

(defn- operations [{:keys [round keys tokens]}]
  (let [[x y] keys
        [a b] tokens
        op (fn [step ops] {:f :txn :round round :step step :value ops})
        left (op :race [[:r y nil] [:append x a]])
        right (op :race [[:r x nil] [:append y b]])
        read-both [[:r x nil] [:r y nil]]]
    (gen/phases
      (gen/any (gen/on-threads #{0} (gen/once left))
               (gen/on-threads #{1} (gen/once right)))
      (gen/once (op :observe read-both))
      (gen/once (op :serial [[:r y nil] [:append x (+ 10 a)]]))
      (gen/once (op :serial [[:r x nil] [:append y (+ 10 b)]]))
      (gen/once (op :final read-both)))))

(defn- routes-for-keys [catalog table-name]
  (let [routes (:routes (json/read-str catalog :key-fn keyword))
        base64 (.withoutPadding (Base64/getUrlEncoder))
        decode #(.decode (Base64/getDecoder) ^String %)]
    (into {} (for [k (mapcat :keys rounds)
                   :let [encoded (.encodeToString base64 (.getBytes ^String (table-name k) StandardCharsets/UTF_8))
                         key (.getBytes (str "!ddb|route|table|" encoded) StandardCharsets/UTF_8)
                         matching (filter (fn [{:keys [start end state]}]
                                            (and (= state "ROUTE_STATE_ACTIVE")
                                                 (<= (Arrays/compareUnsigned ^bytes (decode start) ^bytes key) 0)
                                                 (or (empty? end) (neg? (Arrays/compareUnsigned ^bytes key ^bytes (decode end))))))
                                          routes)]]
               [k (when (= 1 (count matching)) (:raft_group_id (first matching)))]))))

(defrecord BarrierClient [delegate barrier routes]
  client/Client
  (open! [this test node] (assoc this :delegate (client/open! delegate test node)))
  (setup! [_ test]
    (client/setup! delegate
      (assoc test :capture-skew-routes
             (fn [catalog table-name] (reset! routes (routes-for-keys catalog table-name))))))
  (invoke! [_ test op]
    (let [observation (atom nil)
          after-read (fn [snapshot]
                       (reset! observation snapshot)
                       (when (= :race (:step op))
                         (try (.await ^CyclicBarrier barrier 15 TimeUnit/SECONDS)
                              (catch Exception e
                                (throw (ex-info "Both pre-reads did not finish" {:barrier-error true :write-snapshot snapshot} e))))))
          reply (client/invoke! delegate (assoc test :after-skew-read after-read) op)]
      (assoc reply :observed-snapshot @observation
                   :route-groups (select-keys @routes (map second (:value op))))))
  (teardown! [_ test] (client/teardown! delegate test))
  (close! [_ test] (client/close! delegate test)))

(defn- readback [ops step keys]
  (let [reads (filter #(and (= step (:step %)) (= :ok (:type %))) ops)
        mops (:value (first reads))]
    (when (and (= 1 (count reads)) (= keys (mapv second mops)) (every? #(= :r (first %)) mops))
      (mapv last mops))))

(defn- race-shape [{mops :value}]
  (let [[[read-op r] [write-op w token]] mops]
    (when (and (= 2 (count mops)) (= [:r :append] [read-op write-op]))
      [r w token])))

(defn round-evidence [{:keys [round keys tokens]} history]
  (let [events (filter #(= round (:round %)) history)
        ops (remove #(= :invoke (:type %)) events)
        calls (filter #(and (= :invoke (:type %)) (= :race (:step %))) events)
        race (filter #(= :race (:step %)) ops)
        winners (filter #(= :ok (:type %)) race)
        empty-snapshot (zipmap keys (repeat nil))
        loser? #(and (= :fail (:type %)) (= :transact-write (:phase %))
                     (= empty-snapshot (:condition-snapshot %)))
        winner (some-> winners first :value last last)
        expected (mapv #(when (= winner %) [%]) tokens)
        observed (readback ops :observe keys)
        final (readback ops :final keys)
        routes (zipmap keys (if (= round :cross-group) [1 2] [1 1]))
        race-witness? (and (= 2 (count calls) (count race))
                           (= 2 (count (set (map :process calls))))
                           (= [:invoke :invoke] (mapv :type (take 2 (filter #(= :race (:step %)) events))))
                           (= (set (map vector (reverse keys) keys tokens)) (set (map race-shape calls)))
                           (= (set (map (juxt :process race-shape) calls)) (set (map (juxt :process race-shape) race)))
                           (every? #(and (= routes (:route-groups %))
                                         (= empty-snapshot (:observed-snapshot %))) race))
        serial (filter #(= :serial (:step %)) ops)
        [x y] keys
        [a b] tokens
        after-first (update (zipmap keys expected) x (fnil conj []) (+ 10 a))
        complete? (and race-witness? (= 1 (count winners) (count (filter loser? race)))
                       (= (mapv vector (reverse keys) keys [(+ 10 a) (+ 10 b)]) (mapv race-shape serial))
                       (every? #(= :ok (:type %)) serial)
                       (every? :observed-snapshot serial) (some? observed) (some? final))]
    {:valid? (cond
               (and race-witness? (= 2 (count winners))) false
               (not complete?) :unknown
               :else (and (= expected observed)
                          (= [(zipmap keys expected) after-first] (mapv :observed-snapshot serial))
                          (= (mapv #(conj (vec %1) (+ 10 %2)) expected tokens) final)))
     :keys keys :race-routes (mapv :route-groups race) :snapshots (mapv :observed-snapshot race) :winner winner
     :race-outcomes (mapv #(select-keys % [:type :error :phase]) race)
     :observed observed :final final}))

(defn coverage-result [h]
  (let [results (into {} (map (fn [r] [(:round r) (round-evidence r h)]) rounds))]
    (assoc results :valid? (checker/merge-valid (map :valid? (vals results))))))

(defn test-spec [base]
  (let [routes (atom {})
        coverage (reify checker/Checker (check [_ _ h _] (coverage-result h)))]
    (assoc base :name "elastickv-dynamodb-write-skew"
           :concurrency 2 :client (->BarrierClient (:client base) (CyclicBarrier. 2) routes)
           :generator (gen/clients (apply gen/phases (map operations rounds)))
           :checker (checker/compose {:consistency (:checker base) :coverage coverage}))))
