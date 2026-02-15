(ns elastickv.redis-workload
  (:gen-class)
  (:require [clojure.string :as str]
            [clojure.tools.cli :as tools.cli]
            [elastickv.db :as ekdb]
            [jepsen.db :as jdb]
            [jepsen [client :as client]
                    [core :as jepsen]
                    [generator :as gen]
                    [net :as net]]
            [jepsen.control :as control]
            [jepsen.os :as os]
            [jepsen.nemesis :as nemesis]
            [jepsen.nemesis.combined :as combined]
            [jepsen.os.debian :as debian]
            [jepsen.redis.client :as rc]
            [jepsen.redis.append :as redis-append]
            [jepsen.tests.cycle.append :as append]))

(def default-nodes ["n1" "n2" "n3" "n4" "n5"])

(defrecord ElastickvRedisClient [node->port conn]
  client/Client
  (open! [this test node]
    (let [p (get node->port node 6379)
          h (or (:redis-host test) (name node))
          c (rc/open h {:port p :timeout-ms 10000})]
      (assoc this :conn c)))

  (close! [this test]
    (when-let [c (:conn this)]
      (rc/close! c))
    this)

  (setup! [this test])
  (teardown! [this test])

  (invoke! [this test op]
    (let [conn (:conn this)]
      (rc/with-exceptions op #{}
        (rc/with-conn conn
          (->> (if (< 1 (count (:value op)))
                 (->> (:value op)
                      (mapv (partial redis-append/apply-mop! conn))
                      (rc/with-txn conn)
                      (mapv (fn [[f k v] r]
                              [f k (if (= f :r) r v)])
                            (:value op)))
                 (->> (:value op)
                      (mapv (partial redis-append/apply-mop! conn))))
               (mapv (partial redis-append/parse-read conn))
               (assoc op :type :ok, :value)))))))

(defn elastickv-append-workload
  "Append workload that reuses jepsen-io/redis logic but targets elastickv."
  [opts]
  (let [workload (append/test {:key-count (or (:key-count opts) 12)
                               :min-txn-length 1
                               :max-txn-length (or (:max-txn-length opts) 4)
                               :max-writes-per-key (or (:max-writes-per-key opts) 128)
                               :consistency-models [:strict-serializable]})
        client (->ElastickvRedisClient (or (:node->port opts)
                                           (zipmap default-nodes (repeat 6379)))
                                       nil)]
    (assoc workload :client client)))

(defn ports->node-map
  [ports nodes]
  (into {}
        (map (fn [n p] [n p]) nodes ports)))

(defn- normalize-faults [faults]
  (->> faults
       (map (fn [f]
              (case f
                :reboot :kill
                f)))
       vec))

(defn elastickv-redis-test
  "Builds a Jepsen test map that drives elastickv's Redis protocol with the
   jepsen-io/redis append workload."
  ([] (elastickv-redis-test {}))
  ([opts]
   (let [nodes      (or (:nodes opts) default-nodes)
         redis-ports (or (:redis-ports opts) (repeat (count nodes) (or (:redis-port opts) 6379)))
         node->port (or (:node->port opts) (ports->node-map redis-ports nodes))
         local?     (:local opts)
         db         (if local?
                      jdb/noop
                      (ekdb/db {:grpc-port (or (:grpc-port opts) 50051)
                                :redis-port node->port
                                :raft-groups (:raft-groups opts)
                                :shard-ranges (:shard-ranges opts)}))
         rate       (double (or (:rate opts) 5))
         time-limit (or (:time-limit opts) 30)
         faults     (if local?
                      []
                      (normalize-faults (or (:faults opts) [:partition :kill])))
         nemesis-p  (when-not local?
                      (combined/nemesis-package {:db db
                                                 :faults faults
                                                 :interval (or (:fault-interval opts) 40)}))
         nemesis-gen (if nemesis-p
                       (:generator nemesis-p)
                       (gen/once {:type :info :f :noop}))
         workload   (elastickv-append-workload (assoc opts :node->port node->port))]
     (merge workload
            {:name            (or (:name opts) "elastickv-redis-append")
             :nodes           nodes
             :db              db
             :redis-host      (:redis-host opts)
             :os              (if local? os/noop debian/os)
             :net             (if local? net/noop net/iptables)
             :ssh             (merge {:username "vagrant"
                                      :private-key-path "/home/vagrant/.ssh/id_rsa"
                                      :strict-host-key-checking false}
                                     (when local? {:dummy true})
                                     (:ssh opts))
             :remote          control/ssh
             :nemesis         (if nemesis-p
                                (:nemesis nemesis-p)
                                nemesis/noop)
             ; Jepsen 0.3.x can't fressian-serialize some combined final gens; skip.
             :final-generator nil
             :concurrency     (or (:concurrency opts) 5)
             :generator       (->> (:generator workload)
                                   (gen/nemesis nemesis-gen)
                                   (gen/stagger (/ rate))
                                   (gen/time-limit time-limit))}))))

(def cli-opts
  [[nil "--nodes NODES" "Comma separated node names."
    :default "n1,n2,n3,n4,n5"]
   [nil "--local" "Run locally without SSH or nemesis."
    :default false]
   [nil "--host HOST" "Redis host override for clients."
    :default nil]
   [nil "--ports PORTS" "Comma separated Redis ports (per node)."
    :default nil
    :parse-fn (fn [s]
                (->> (str/split s #",")
                     (remove str/blank?)
                     (mapv #(Integer/parseInt %))))]
   [nil "--redis-port PORT" "Redis port (applied to all nodes)."
    :default 6379
    :parse-fn #(Integer/parseInt %)]
   [nil "--grpc-port PORT" "gRPC/Raft port."
    :default 50051
    :parse-fn #(Integer/parseInt %)]
   [nil "--raft-groups GROUPS" "Comma separated raft groups (groupID=port,...)"
    :parse-fn (fn [s]
                (->> (str/split s #",")
                     (remove str/blank?)
                     (map (fn [part]
                            (let [[gid port] (str/split part #"=" 2)]
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

(defn -main
  [& args]
  (let [{:keys [options errors summary]} (tools.cli/parse-opts args cli-opts)
        default-nodes "n1,n2,n3,n4,n5"
        ports (:ports options)
        local? (or (:local options) (and (:host options) (seq ports)))
        nodes-raw (if (and ports (= (:nodes options) default-nodes))
                    (str/join "," (map (fn [i] (str "n" i)) (range 1 (inc (count ports)))))
                    (:nodes options))
        node-list (-> nodes-raw
                      (str/split #",")
                      (->> (remove str/blank?)
                           vec))
        faults    (-> (:faults options)
                      (str/split #",")
                      (->> (remove str/blank?)
                           (map (comp keyword str/lower-case))
                           vec))
        options   (assoc options
                    :nodes node-list
                    :faults faults
                    :local local?
                    :redis-host (:host options)
                    :redis-ports ports
                    :redis-port (:redis-port options)
                    :grpc-port (:grpc-port options)
                    :raft-groups (:raft-groups options)
                    :shard-ranges (:shard-ranges options)
                    :ssh {:username (:ssh-user options)
                          :private-key-path (:ssh-key options)
                          :strict-host-key-checking false})]
    (cond
      (:help options) (println summary)
      (seq errors) (binding [*out* *err*]
                     (println "Error parsing options:" (str/join "; " errors)))
      (:local options) (binding [control/*dummy* true]
                         (jepsen/run! (elastickv-redis-test options)))
      :else (jepsen/run! (elastickv-redis-test options)))))
