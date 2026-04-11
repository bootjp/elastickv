(ns elastickv.cli
  "Shared CLI infrastructure for Jepsen workloads."
  (:require [clojure.string :as str]
            [clojure.tools.cli :as tools.cli]
            [clojure.tools.logging :refer [warn]]
            [jepsen.control :as control]
            [jepsen.core :as jepsen]))

(def default-nodes-str "n1,n2,n3,n4,n5")

(def common-cli-opts
  "CLI options shared by all workloads."
  [[nil "--nodes NODES" "Comma separated node names."
    :default default-nodes-str]
   [nil "--local" "Run locally without SSH or nemesis."
    :default false]
   [nil "--host HOST" "Host override for clients."
    :default nil]
   [nil "--grpc-port PORT" "gRPC/Raft port."
    :default 50051
    :parse-fn #(Integer/parseInt %)]
   [nil "--raft-engine ENGINE" "Raft engine implementation (hashicorp or etcd)."
    :default "hashicorp"]
   [nil "--raft-groups GROUPS" "Comma separated raft groups (groupID=port,...)"
    :parse-fn (fn [s]
                (->> (str/split s #",")
                     (remove str/blank?)
                     (map (fn [part]
                            (let [[gid port] (str/split part #"=" 2)]
                              (when-not (and gid port)
                                (throw (IllegalArgumentException.
                                         (str "Invalid raft group format: " part ". Expected groupID=port"))))
                              [(Long/parseLong gid) (Integer/parseInt port)])))
                     (into {})))]
   [nil "--shard-ranges RANGES" "Shard ranges (start:end=groupID,...)"
    :default nil]
   [nil "--faults LIST" "Comma separated faults (partition,kill,clock)."
    :default "partition,kill,clock"]
   [nil "--ssh-key PATH" "SSH private key path."
    :default "/home/vagrant/.ssh/id_rsa"]
   [nil "--ssh-user USER" "SSH username."
    :default "vagrant"]
   [nil "--time-limit SECONDS" "How long to run the workload."
    :default 30
    :parse-fn #(Integer/parseInt %)]
   [nil "--rate HZ" "Approx ops/sec per worker."
    :default 5
    :parse-fn #(Double/parseDouble %)]
   [nil "--concurrency N" "Number of worker threads."
    :default 5
    :parse-fn #(Integer/parseInt %)]
   ["-h" "--help"]])

(defn ports->node-map
  [ports nodes]
  (zipmap nodes ports))

(defn normalize-faults [faults]
  (->> faults
       (map (fn [f] (case f :reboot :kill f)))
       vec))

(defn fail-on-invalid!
  "Raises when Jepsen completed analysis and found the history invalid.
   jepsen/run! returns the test map with results under :results.
   Treats anything other than true (e.g. false, :unknown) as a failure."
  [result]
  (let [valid? (:valid? (:results result))]
    (when-not (true? valid?)
      (throw (ex-info "Jepsen analysis invalid" {:result (:results result)}))))
  result)

(defn parse-common-opts
  "Parse nodes, faults, ssh config from raw CLI options.
   `per-node-ports` is the parsed per-node port vector (or nil)."
  [options per-node-ports]
  (let [local? (or (:local options) (and (:host options) (seq per-node-ports)))
        nodes-raw (if (and per-node-ports (= (:nodes options) default-nodes-str))
                    (str/join "," (map (fn [i] (str "n" i))
                                      (range 1 (inc (count per-node-ports)))))
                    (:nodes options))
        node-list (-> nodes-raw
                      (str/split #",")
                      (->> (remove str/blank?) vec))
        faults (-> (:faults options)
                   (str/split #",")
                   (->> (remove str/blank?)
                        (map (comp keyword str/lower-case))
                        vec))]
    (assoc options
      :nodes node-list
      :faults faults
      :local local?
      :ssh {:username (:ssh-user options)
            :private-key-path (:ssh-key options)
            :strict-host-key-checking false})))

(defn run-workload!
  "Shared -main implementation.
   `cli-opts`    - full option spec (common + workload-specific)
   `prepare-fn`  - (fn [parsed-options]) -> options ready for test-fn
   `test-fn`     - (fn [options]) -> Jepsen test map"
  [args cli-opts prepare-fn test-fn]
  (try
    (let [{:keys [options errors summary]} (tools.cli/parse-opts args cli-opts)
          options (prepare-fn options)]
      (cond
        (:help options)  (println summary)
        (seq errors)     (do (binding [*out* *err*]
                               (println "Error parsing options:" (str/join "; " errors)))
                           (System/exit 1))
        :else (let [run! #(fail-on-invalid! (jepsen/run! (test-fn options)))]
                (if (:local options)
                  (binding [control/*dummy* true] (run!))
                  (run!))
                (shutdown-agents)
                (System/exit 0))))
    (catch Throwable t
      (warn t "Workload failed")
      (shutdown-agents)
      (System/exit 1))))
