(ns elastickv.split-regression-test
  "Controller/oracle unit fixtures, not replicated runtime acceptance."
  (:require [clojure.data.json :as json]
            [clojure.java.shell :as shell]
            [clojure.test :refer :all]
            [elastickv.split-workload :as split]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.history :as history]
            [jepsen.nemesis :as nemesis]))

(defn- route [id owner start end]
  {:route-id id :raft-group-id owner :start start :end end :state "ROUTE_STATE_ACTIVE"})

(defn- snapshot [version & routes]
  {:catalog-version version :routes (vec routes)})

(def root (route 100 1 "" nil))
(def left (route 10 1 "" "m2-split"))
(def child (route 20 1 "m2-split" nil))
(def retained (route 30 1 "m2-split" "m2-t"))
(def moving (route 40 2 "m2-t" nil))
(def retained-again (route 50 2 "m2-t" "m2-u"))
(def moving-again (route 60 1 "m2-u" nil))
(def snapshots [(snapshot 7 root)
                (snapshot 8 left child)
                (snapshot 10 left retained moving)
                (snapshot 12 left retained retained-again moving-again)])
(def fences [(snapshot 9 left retained
                       (assoc moving :raft-group-id 1 :state "ROUTE_STATE_WRITE_FENCED"))
             (snapshot 11 left retained retained-again
                       (assoc moving-again :raft-group-id 2 :state "ROUTE_STATE_WRITE_FENCED"))])

(defn- lifecycle-fixture []
  (vec
    (mapcat
      (fn [step source key target job-id]
        (let [plan {:step step :source source :route-id (:route-id source)
                    :before (nth snapshots step) :address "n1:50051"
                    :split-key key :same-owner? (zero? step)
                    :target-group-id target :job-id job-id}
              after (nth snapshots (inc step))
              result (assoc plan :phase "SPLIT_JOB_PHASE_DONE" :after after
                            :right (last (:routes after))
                            :observations (cond-> []
                                            (pos? step) (conj {:phase "SPLIT_JOB_PHASE_FENCE"
                                                               :snapshot (nth fences (dec step))})
                                            true (conj {:phase "SPLIT_JOB_PHASE_DONE" :snapshot after})))]
          [{:process :nemesis :type :info :f :start-repeated-split :value plan}
           {:process :nemesis :type :info :f :verify-repeated-split :value result}]))
      (range 3) [root child moving] ["m2-split" "m2-t" "m2-u"] [1 2 1] [nil 44 45])))

(defn- check-lifecycle [events]
  (checker/check (split/repeated-split-checker) {} (history/history events) {}))

(defn- b64 [s]
  (.encodeToString (java.util.Base64/getEncoder) (.getBytes (or s "") "UTF-8")))

(defn- encode-snapshot [s]
  (json/write-str
    {:catalog_version (:catalog-version s)
     :routes (mapv (fn [r] {:route_id (:route-id r) :raft_group_id (:raft-group-id r)
                            :start (b64 (:start r)) :end (b64 (:end r)) :state (:state r)})
                   (:routes s))}))

(def opts {:list-routes-bin "elastickv-list-routes" :split-bin "elastickv-split"
           :grpc-host-port "n1:50051" :repeat-splits true})
(def cluster {:nodes ["n1" "n2" "n3"] :raft-groups {1 50051, 2 50052}})

(defn- invoke [nem f step]
  (nemesis/invoke! nem cluster {:process :nemesis :type :info :f f :value {:step step}}))

(deftest repeated-split-scheduling-is-bounded-and-ordered
  (let [ctx (gen/context {:concurrency 8})
        package (split/repeated-split-package {:split-at-seconds 0})
        ops (loop [g (:generator package), ops []]
              (let [[op next-gen] (gen/op g {} ctx)]
                (cond
                  (nil? op) ops
                  (= :pending op) (throw (ex-info "unexpected pending generator" {}))
                  (> (count ops) 6) (throw (ex-info "unbounded split generator" {}))
                  :else (recur next-gen (conj ops op)))))]
    (is (= [[:start-repeated-split 0] [:verify-repeated-split 0]
            [:start-repeated-split 1] [:verify-repeated-split 1]
            [:start-repeated-split 2] [:verify-repeated-split 2]]
           (mapv #(vector (:f %) (get-in % [:value :step])) (filter :f ops))))
    (is (= #{:start-repeated-split :verify-repeated-split} (nemesis/fs (:nemesis package))))))

(deftest repeated-controller-queries-current-child-and-current-version
  (let [current (atom 0), calls (atom [])]
    (with-redefs [shell/sh
                  (fn [bin & args]
                    (swap! calls conj (into [bin] args))
                    {:exit 0 :err ""
                     :out (cond
                            (= bin "elastickv-list-routes") (encode-snapshot (nth snapshots @current))
                            (= "--get-job-id" (nth args 2)) "phase: SPLIT_JOB_PHASE_DONE\n"
                            :else (let [step @current]
                                    (swap! current inc)
                                    (str "catalog_version: " (+ 8 step) "\n"
                                         (when (pos? step) (str "job_id: " (+ 43 step) "\n")))))})]
      (let [nem (split/repeated-split-nemesis opts)
            results (vec (mapcat (fn [step] [(invoke nem :start-repeated-split step)
                                             (invoke nem :verify-repeated-split step)]) (range 3)))
            starts (filter #(some #{"--route-id"} %) @calls)]
        (is (every? #(= :info (:type %)) results))
        (is (true? (:valid? (check-lifecycle results))))
        (is (= [["elastickv-split" "--address" "n1:50051" "--route-id" "100"
                 "--split-key" "m2-split" "--expected-version" "7"]
                ["elastickv-split" "--address" "n1:50051" "--route-id" "20"
                 "--split-key" "m2-t" "--expected-version" "8" "--target-group-id" "2"]
                ["elastickv-split" "--address" "n1:50051" "--route-id" "40"
                 "--split-key" "m2-u" "--expected-version" "10" "--target-group-id" "1"]]
               starts))
        (is (= :fail (:type (invoke nem :start-repeated-split 3))))))))

(deftest repeated-oracle-checks-partitions-lineage-and-stable-moving-id
  (let [h (lifecycle-fixture)]
    (is (true? (:valid? (check-lifecycle h))))
    (is (= 20 (get-in h [3 :value :route-id])) "SourceRouteID remains original lineage")
    (is (= 40 (get-in h [3 :value :right :route-id])) "working moving child is distinct")
    (doseq [[label bad] [["empty" []]
                         ["only first split" (subvec h 0 2)]
                         ["missing verify" (pop h)]
                         ["duplicate lifecycle" (into h (subvec h 0 2))]
                         ["reordered" (assoc h 0 (nth h 1) 1 (nth h 0))]
                         ["failed start" (assoc-in h [2 :type] :fail)]
                         ["failed verify" (assoc-in h [3 :type] :fail)]
                         ["wrong job" (assoc-in h [3 :value :job-id] 999)]
                         ["job reused" (-> h (assoc-in [4 :value :job-id] 44)
                                               (assoc-in [5 :value :job-id] 44))]
                         ["zero job" (-> h (assoc-in [2 :value :job-id] 0)
                                              (assoc-in [3 :value :job-id] 0))]
                         ["wrong step" (assoc-in h [2 :value :step] 0)]
                         ["still fenced" (assoc-in h [3 :value :phase] "SPLIT_JOB_PHASE_FENCE")]
                         ["oversized preflight abandoned" (assoc-in h [3 :value :phase] "SPLIT_JOB_PHASE_ABANDONED")]
                         ["retired source reused" (assoc-in h [4 :value :source] child)]
                         ["cutover replaced working child" (assoc-in h [3 :value :observations 0 :snapshot :routes 2 :route-id] 99)]
                         ["whole parent fenced" (assoc-in h [3 :value :observations 0 :snapshot]
                                                         (snapshot 9 left (assoc child :state "ROUTE_STATE_WRITE_FENCED")))]
                         ["retained side moved" (assoc-in h [3 :value :after :routes 1 :raft-group-id] 2)]
                         ["gap" (assoc-in h [3 :value :after :routes 1 :end] "m2-s")]
                         ["overlap" (update-in h [3 :value :after :routes] conj child)]
                         ["unrelated route changed" (assoc-in h [3 :value :after :routes 0 :raft-group-id] 2)]
                         ["catalog regressed" (assoc-in h [3 :value :after :catalog-version] 7)]
                         ["no observations" (assoc-in h [3 :value :observations] [])]]]
      (is (false? (:valid? (check-lifecycle bad))) label))
    (is (true? (:valid? (check-lifecycle
                         (into [{:process :nemesis :type :info :f :start-repeated-split :value {:step 0}}] h))))
        "invocation placeholders are not successful completions")))

(deftest oversized-helper-rejection-is-not-migration-success
  (let [calls (atom 0)
        diagnostic "rpc error: code = ResourceExhausted desc = new version exceeds moving-range transfer budget"]
    (with-redefs [shell/sh (fn [bin & _]
                            (swap! calls inc)
                            (if (= bin "elastickv-list-routes")
                              {:exit 0 :out (encode-snapshot (first snapshots)) :err ""}
                              {:exit 1 :out "" :err diagnostic}))]
      (let [nem (split/repeated-split-nemesis opts)
            start (invoke nem :start-repeated-split 0)
            verify (invoke nem :verify-repeated-split 0)
            next (invoke nem :start-repeated-split 1)]
        (is (= :fail (:type start) (:type verify) (:type next)))
        (is (= 1 (get-in start [:details :exit])))
        (is (= diagnostic (get-in start [:details :err])))
        (is (= 2 @calls) "failed sequence must not dispatch any later split")
        (is (false? (:valid? (check-lifecycle [start verify next]))))))))

(deftest preflight-abandonment-cannot-advance-to-another-split
  (let [current (atom 0), phase (atom "SPLIT_JOB_PHASE_ABANDONED")]
    (with-redefs [shell/sh (fn [bin & args]
                            {:exit 0 :err ""
                             :out (cond
                                    (= bin "elastickv-list-routes") (encode-snapshot (nth snapshots @current))
                                    (= "--get-job-id" (nth args 2)) (str "phase: " @phase "\n")
                                    :else (let [step @current]
                                            (swap! current inc)
                                            (if (zero? step) "catalog_version: 8\n" "job_id: 44\n")))})]
      (let [nem (split/repeated-split-nemesis opts)]
        (invoke nem :start-repeated-split 0)
        (invoke nem :verify-repeated-split 0)
        (invoke nem :start-repeated-split 1)
        (let [rejected (invoke nem :verify-repeated-split 1)]
          (is (= :fail (:type rejected)))
          (is (= "SPLIT_JOB_PHASE_ABANDONED" (get-in rejected [:details :observations 0 :phase]))))
        (reset! phase "SPLIT_JOB_PHASE_DONE")
        (is (= :fail (:type (invoke nem :start-repeated-split 2))))))))

(deftest controller-fails-closed-on-incomplete-or-invalid-completion
  (doseq [phase ["SPLIT_JOB_PHASE_FENCE" "SPLIT_JOB_PHASE_CUTOVER"
                "SPLIT_JOB_PHASE_FAILED" "SPLIT_JOB_PHASE_DONE" "UNKNOWN"]]
    (let [current (atom 0)]
      (with-redefs [shell/sh
                    (fn [bin & args]
                      {:exit 0 :err ""
                       :out (cond
                              (= bin "elastickv-list-routes")
                              (encode-snapshot (if (= 2 @current)
                                                 (assoc-in (nth snapshots 2) [:routes 2 :raft-group-id] 1)
                                                 (nth snapshots @current)))
                              (= "--get-job-id" (nth args 2)) (str "phase: " phase "\n")
                              :else (let [step @current]
                                      (swap! current inc)
                                      (if (zero? step) "catalog_version: 8\n" "job_id: 44\n")))})]
        (let [nem (split/repeated-split-nemesis (assoc opts :split-timeout-ms -1))]
          (is (= :info (:type (invoke nem :start-repeated-split 0))))
          (is (= :info (:type (invoke nem :verify-repeated-split 0))))
          (is (= :info (:type (invoke nem :start-repeated-split 1))))
          (is (= :fail (:type (invoke nem :verify-repeated-split 1))) phase)
          (is (= :fail (:type (invoke nem :start-repeated-split 2)))))))))

(deftest controller-does-not-dispatch-out-of-order-operations
  (doseq [[f step] [[:verify-repeated-split 0] [:start-repeated-split 1]]]
    (let [calls (atom 0)]
      (with-redefs [shell/sh (fn [& _] (swap! calls inc))]
        (is (= :fail (:type (invoke (split/repeated-split-nemesis opts) f step))))
        (is (zero? @calls))))))

(deftest repeated-registers-cover-every-final-partition
  (let [keys (var-get (ns-resolve 'elastickv.split-workload 'repeated-register-keys))]
    (doseq [r (:routes (last snapshots))]
      (is (some #(and (not (neg? (compare % (:start r))))
                      (or (nil? (:end r)) (neg? (compare % (:end r))))) keys)))))

(deftest repeated-final-reads-require-every-key-from-every-node
  (let [test {:concurrency 8 :nodes ["n1" "n2" "n3"]}
        ctx (gen/context test)
        reads (loop [g (split/repeated-final-reads 3), ops []]
                (let [[op next-gen] (gen/op g test ctx)]
                  (if (or (nil? op) (= :pending op)) ops
                    (recur next-gen (conj ops (assoc (dissoc (into {} op) :index)
                                                     :type :ok :node (nth (:nodes test) (mod (:process op) 3))))))))
        writes (mapv (fn [k] {:process 0 :type :ok :f :write :value [k 1]}) (range 4))
        prefix (into writes (lifecycle-fixture))
        check #(checker/check (split/repeated-read-checker) test (history/history %) {})]
    (is (= 12 (count reads)))
    (is (every? :final reads))
    (is (true? (:valid? (check (into prefix reads)))))
    (is (false? (:valid? (check (into prefix (pop reads))))))
    (is (false? (:valid? (check (into prefix (assoc-in reads [0 :type] :info))))))
    (is (false? (:valid? (check (into (into (pop writes) (lifecycle-fixture)) reads)))))
    (is (false? (:valid? (check (into (into writes reads) (lifecycle-fixture))))))
    (is (false? (:valid? (check (into writes reads)))))
    (is (false? (:valid? (check (into prefix (map #(dissoc % :node) reads))))))
    (is (true? (:valid? (check (into prefix (map #(update % :process + 8) reads)))))
        "process rollover must not change the observed node")))

(deftest repeated-mode-is-opt-in-and-rejects-incompatible-settings
  (is (= 6 (:concurrency (split/elastickv-split-test {:local true}))))
  (is (= 8 (:concurrency (split/elastickv-split-test {:local true :repeat-splits true}))))
  (is (thrown? clojure.lang.ExceptionInfo
               (split/elastickv-split-test {:local true :repeat-splits true :abandon-at-fence true})))
  (doseq [concurrency [0 5 6 9]]
    (is (thrown? clojure.lang.ExceptionInfo
                 (split/elastickv-split-test {:local true :repeat-splits true :concurrency concurrency})))))
