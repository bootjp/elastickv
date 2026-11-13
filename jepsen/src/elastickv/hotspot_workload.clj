(ns elastickv.hotspot-workload
  "Bounded M4 automatic hotspot split test over linearizable Redis registers."
  (:gen-class)
  (:require [clojure.java.shell :as shell]
            [clojure.tools.logging :refer [warn]]
            [elastickv.cli :as cli]
            [elastickv.composed1-nemesis :as routes]
            [elastickv.db :as ekdb]
            [elastickv.split-workload :as split]
            [jepsen [checker :as checker]
                    [generator :as gen]
                    [independent :as independent]
                    [net :as net]]
            [jepsen.control :as control]
            [jepsen.db :as jdb]
            [jepsen.nemesis :as nemesis]
            [jepsen.nemesis.combined :as combined]
            [jepsen.os :as os]
            [jepsen.os.debian :as debian]))

(def register-keys ["m4-a" "m4-m" "m4-z"])

(def auto-split-args
  ["--autoSplit" "--keyvizStep=5s" "--autoSplitEvalInterval=1s"
   "--autoSplitCooldown=5s" "--autoSplitThreshold=60"
   "--autoSplitCandidateWindows=3" "--autoSplitMaxRoutes=2"
   "--autoSplitMaxPerCycle=1" "--autoSplitDefaultBuckets=16"])

(defn- grpc-address [opts test]
  (or (:grpc-host-port opts)
      (str (or (:grpc-host opts) (name (first (:nodes test)))) ":"
           (or (:grpc-port opts) 50051))))

(defn route-snapshot! [opts test]
  (let [bin (or (:list-routes-bin opts)
                (str (System/getProperty "user.dir")
                     "/target/elastickv-jepsen/elastickv-list-routes"))
        result (shell/sh bin "--address" (grpc-address opts test))]
    (when-not (zero? (:exit result))
      (throw (ex-info "list-routes failed" result)))
    (routes/parse-routes-json (:out result))))

(defn- active-routes [snapshot]
  (filter #(= "ROUTE_STATE_ACTIVE" (:state %)) (:routes snapshot)))

(defn split-observation [before after]
  (let [old (vec (active-routes before))
        new (vec (active-routes after))
        parent (first old)
        children (when parent
                   (filter #(and (= (:raft-group-id parent) (:raft-group-id %))
                                 (not (neg? (compare (:start %) (:start parent))))
                                 (or (nil? (:end parent))
                                     (and (:end %) (not (pos? (compare (:end %) (:end parent))))))) new))
        ordered (sort-by :start children)]
    (when (and (= 1 (count old)) (= 2 (count new)) (= 2 (count ordered))
               (> (:catalog-version after) (:catalog-version before))
               (= (:start parent) (:start (first ordered)))
               (neg? (compare (:start (first ordered)) (:end (first ordered))))
               (= (:end (first ordered)) (:start (second ordered)))
               (or (nil? (:end (second ordered)))
                   (neg? (compare (:start (second ordered)) (:end (second ordered)))))
               (= (:end parent) (:end (second ordered)))
               (every? #(= 1 (count (filter (fn [r] (routes/route-covers? r %)) new)))
                       register-keys))
      {:before before :after after :split-key (:end (first ordered))})))

(defn controller-nemesis [opts]
  (let [baseline (atom nil)
        deadline (atom nil)
        observed (atom nil)]
    (reify
      nemesis/Reflection
      (fs [_] #{:capture-hotspot-baseline :observe-hotspot-split})
      nemesis/Nemesis
      (setup! [this _] this)
      (invoke! [_ test op]
        (try
          (case (:f op)
            :capture-hotspot-baseline
            (let [snapshot (route-snapshot! opts test)]
              (when-not (= 1 (count (active-routes snapshot)))
                (throw (ex-info "M4 requires one initial active route" {:snapshot snapshot})))
              (reset! baseline snapshot)
              (reset! deadline (+ (System/currentTimeMillis)
                                  (or (:observe-timeout-ms opts) 90000)))
              (assoc op :type :info :value snapshot))
            :observe-hotspot-split
            (do
              (when-not @baseline
                (throw (ex-info "missing hotspot baseline" {})))
              (if @observed
                (assoc op :type :info :value @observed)
                (let [after (route-snapshot! opts test)
                      observation (split-observation @baseline after)]
                  (cond
                    observation (do (reset! observed observation)
                                    (assoc op :type :info :value observation))
                    (not= 1 (count (active-routes after)))
                    (throw (ex-info "automatic hotspot split has invalid route geometry"
                                    {:before @baseline :after after}))
                    (>= (System/currentTimeMillis) @deadline)
                    (throw (ex-info "automatic hotspot split not observed before deadline"
                                    {:before @baseline :after after}))
                    :else (assoc op :type :info)))))
            op)
          (catch Throwable t
            (warn t "M4 controller observation failed")
            (assoc op :type :fail :error (or (.getMessage t) (str t))))))
      (teardown! [this _] this))))

(defn controller-package [opts]
  {:generator (gen/phases
                (gen/once {:type :info :f :capture-hotspot-baseline})
                (gen/sleep (or (:observe-after-seconds opts) 15))
                (gen/stagger 5 (gen/repeat {:type :info :f :observe-hotspot-split})))
   :final-generator nil
   :nemesis (controller-nemesis opts)
   :perf #{{:name "automatic-hotspot-split"
            :fs #{:capture-hotspot-baseline :observe-hotspot-split}
            :start #{:capture-hotspot-baseline}
            :stop #{:observe-hotspot-split}
            :color "#4A90E2"}}})

(defn controller-checker []
  (reify checker/Checker
    (check [_ _ history _]
      (let [ops (filter #(and (= :nemesis (:process %))
                              (contains? #{:capture-hotspot-baseline :observe-hotspot-split} (:f %))
                              (contains? #{:info :ok :fail} (:type %))) history)
            captures (vec (filter #(= :capture-hotspot-baseline (:f %)) ops))
            observations (filter #(and (= :observe-hotspot-split (:f %))
                                       (map? (:value %))) ops)
            capture (first captures)
            observe (first observations)]
        {:valid? (and (= 1 (count captures))
                      (some? observe)
                      (not-any? #(or (:error %) (= :fail (:type %))) ops)
                      (< (:index capture) (:index observe))
                      (= (:value capture) (get-in observe [:value :before]))
                      (some? (split-observation (:value capture)
                                                (get-in observe [:value :after]))))
         :completions (vec ops)}))))

(defn final-reads [node-count]
  (apply gen/any
         (for [node-index (range node-count)]
           (gen/on-threads #(and (integer? %) (= node-index (mod % node-count)))
                           (map (fn [key-index]
                                  {:f :read :value (independent/tuple key-index nil) :final true})
                                (range (count register-keys)))))))

(defn final-read-checker []
  (reify checker/Checker
    (check [_ test history _]
      (let [writes (set (keep #(when (and (= :ok (:type %)) (= :write (:f %)))
                                  (first (:value %))) history))
            reads (filter #(and (:final %) (= :ok (:type %)) (= :read (:f %))) history)
            observed (set (map #(vector (:node %) (first (:value %))) reads))
            expected (set (for [node (:nodes test), key-index (range (count register-keys))]
                            [node key-index]))
            observed-split (last (filter #(and (= :observe-hotspot-split (:f %))
                                              (map? (:value %))) history))]
        {:valid? (and (= (set (range (count register-keys))) writes)
                      (= expected observed)
                      (some? observed-split)
                      (every? #(< (:index observed-split) (:index %)) reads))
         :missing-final-reads (vec (sort (remove observed expected)))
         :written-keys writes}))))

(defn elastickv-hotspot-test
  ([] (elastickv-hotspot-test {}))
  ([opts]
   (let [nodes (or (:nodes opts) split/default-nodes)
         local? (:local opts)
         node->port (or (:node->port opts)
                        (cli/ports->node-map
                          (or (:redis-ports opts) (repeat (count nodes) (or (:redis-port opts) 6379))) nodes))
         db (if local? jdb/noop
              (ekdb/db {:grpc-port (or (:grpc-port opts) 50051)
                        :redis-port node->port
                        :auto-split-args auto-split-args}))
         faults (when-not local?
                  (combined/nemesis-package
                    {:db db :faults (cli/normalize-faults (or (:faults opts) [:partition :kill]))
                     :interval (or (:fault-interval opts) 15)}))
         package (combined/compose-packages
                   (cond-> [(controller-package opts)] faults (conj faults)))
         workload (split/split-register-workload
                    (assoc opts :node->port node->port :register-keys register-keys
                           :independent-concurrency (count register-keys)
                           :max-writes-per-key (or (:max-writes-per-key opts) 10000)))]
     (merge workload
            {:name (or (:name opts) "elastickv-m4-automatic-hotspot")
             :nodes nodes :db db :redis-host (:redis-host opts)
             :os (if local? os/noop debian/os)
             :net (if local? net/noop net/iptables)
             :ssh (merge {:username "vagrant" :private-key-path "/home/vagrant/.ssh/id_rsa"
                          :strict-host-key-checking false}
                         (when local? {:dummy true}) (:ssh opts))
             :remote control/ssh :nemesis (:nemesis package)
             :checker (checker/compose {:register (:checker workload)
                                        :controller (controller-checker)
                                        :final-reads (final-read-checker)})
             :concurrency (or (:concurrency opts) 6)
             :generator (->> (:generator workload)
                             (gen/nemesis (:generator package))
                             (gen/stagger (/ (double (or (:rate opts) 20))))
                             (gen/time-limit (or (:time-limit opts) 120)))
             :final-generator (final-reads (count nodes))}))))

(def hotspot-cli-opts
  [[nil "--redis-port PORT" "Redis port." :default 6379 :parse-fn #(Integer/parseInt %)]
   [nil "--grpc-host-port HOST:PORT" "Distribution gRPC address."]
   [nil "--list-routes-bin PATH" "Path to elastickv-list-routes."]
   [nil "--observe-after-seconds SECONDS" "Delay before route observation."
    :default 15 :parse-fn #(Integer/parseInt %)]
   [nil "--observe-timeout-ms MS" "Maximum route observation duration."
    :default 90000 :parse-fn #(Long/parseLong %)]])

(defn -main [& args]
  (cli/run-workload! args
                     (into (mapv (fn [option]
                                   (case (second option)
                                     "--time-limit SECONDS" (assoc option 4 120)
                                     "--faults LIST" (assoc option 4 "partition,kill")
                                     "--fault-interval SECONDS" (assoc option 4 10)
                                     option)) cli/common-cli-opts)
                           hotspot-cli-opts)
                     #(cli/parse-common-opts % nil) elastickv-hotspot-test))
