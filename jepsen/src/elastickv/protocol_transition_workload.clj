(ns elastickv.protocol-transition-workload
  "Seed and observe the same named fixture on opposite sides of a cluster restart."
  (:require [clojure.string :as str]
            [cognitect.aws.client.api :as aws]
            [elastickv.cli :as cli]
            [elastickv.s3-workload :as s3]
            [elastickv.sqs-common :as sqs]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.tests :as tests]
            [taoensso.carmine :as car]))

(def increments 18)

(defn- redis! [c & args] (car/wcar c (car/redis-call args)))

(defn- expect! [label expected observed]
  (when-not (= expected observed)
    (throw (ex-info (str "Unexpected " (name label)) {:expected expected :observed observed}))))

(defn- rejects? [pred f]
  (try (f) false (catch Exception e (boolean (pred e)))))

(defn- ttl! [c k phase]
  (let [long-key (str k "-long") expired (str k "-expired")
        stamp-key (str k "-stamp") ttl 3600000]
    (when (= phase :seed)
      (redis! c "SET" k "prior")
      (doseq [args [["SETEX" k "9223372036854775807" "new"]
                    ["EVAL" "return redis.call('SETEX',KEYS[1],ARGV[1],'new')" 1 k "9223372036854775807"]
                    ["EVAL" "return redis.call('SET',KEYS[1],'new','PX',0)" 1 k]
                    ["EVAL" "return redis.call('PEXPIRE',KEYS[1],ARGV[1])" 1 k "9223372036854775807"]]]
        (expect! :invalid-ttl true
                 (rejects? #(str/includes? (.getMessage %) "invalid expire time") #(apply redis! c args))))
      (let [script "redis.call('SET',KEYS[1],'v','PX',25); local a=redis.call('PTTL',KEYS[1]); for i=1,100 do redis.call('PTTL',KEYS[1]) end; return {a,redis.call('PTTL',KEYS[1])}"]
        (expect! :script-clock [25 25] (redis! c "EVAL" script 1 (str k "-script"))))
      (let [before (System/currentTimeMillis)]
        (redis! c "SET" long-key "live" "PX" ttl)
        (redis! c "SET" stamp-key (str before "," (System/currentTimeMillis))))
      (redis! c "SET" expired "temporary" "PX" 1))
    (expect! :prior-value "prior" (redis! c "GET" k))
    (expect! :persistent -1 (redis! c "PTTL" k))
    (expect! :live-value "live" (redis! c "GET" long-key))
    (let [[lo hi] (mapv parse-long (str/split (redis! c "GET" stamp-key) #","))
          before (System/currentTimeMillis)
          remaining (redis! c "PTTL" long-key)
          after (System/currentTimeMillis)]
      ;; Both calls can cross a millisecond boundary. The two inferred
      ;; intervals for the original deadline must overlap.
      (expect! :absolute-deadline true
               (and (pos? remaining) (<= (+ lo ttl) (+ after remaining 2))
                    (<= (+ before remaining) (+ hi ttl 2)))))
    (when (= phase :recover)
      (expect! :expired nil (redis! c "GET" expired))
      (expect! :missing-pttl -2 (redis! c "PTTL" expired)))
    :checked))

(defn- lua! [c k write?]
  (let [script (if write?
                 "local v=redis.call('INCR',KEYS[1]); redis.call('SET',KEYS[2],tostring(v)); return {v,redis.call('GET',KEYS[2])}"
                 "return {redis.call('GET',KEYS[1]) or '0',redis.call('GET',KEYS[2]) or '0'}")
        [v mirror] (redis! c "EVAL" script 2 (str k "-count") (str k "-mirror"))
        value (parse-long (str v))]
    (expect! :mirror value (parse-long (str mirror)))
    value))

(defn- visibility! [c conn queue phase]
  (let [queue-url (:QueueUrl (sqs/invoke! conn (if (= phase :seed) :CreateQueue :GetQueueUrl)
                                          {:QueueName queue}))
        invoke (fn [op opts] (sqs/invoke! conn op (assoc opts :QueueUrl queue-url)))
        receive #(first (:Messages (invoke :ReceiveMessage {:WaitTimeSeconds % :VisibilityTimeout 14400})))
        change (fn [receipt seconds]
                 (invoke :ChangeMessageVisibility {:ReceiptHandle receipt :VisibilityTimeout seconds}))
        receipt-key (str queue "-receipt")]
    (if (= phase :seed)
      (do (invoke :SendMessage {:MessageBody queue})
          (let [msg (receive 3)
                receipt (:ReceiptHandle msg)]
            (expect! :delivered queue (:Body msg))
            (Thread/sleep 5)
            (expect! :receipt-budget true
                     (rejects? #(= "InvalidAttributeValue" (:type (ex-data %))) #(change receipt 43200)))
            (change receipt 14400)
            (redis! c "SET" receipt-key receipt)))
      (let [receipt (redis! c "GET" receipt-key)]
        (expect! :still-hidden nil (:Body (receive 0)))
        (change receipt 0)
        (let [msg (receive 3) fresh (:ReceiptHandle msg)]
          (expect! :redelivered queue (:Body msg))
          (expect! :rotated false (= receipt fresh))
          (expect! :stale-receipt true
                   (rejects? #(= "InvalidReceiptHandle" (:type (ex-data %))) #(change receipt 10)))
          (invoke :DeleteMessage {:ReceiptHandle fresh}))))
    :checked))

(defn- multipart! [redis url k phase]
  (let [pending-key (str k "-upload")]
    (s3/check-multipart-fixture! url k (= phase :seed))
    (if (= phase :seed)
      (let [[id tag] (s3/check-pending-upload! url k nil)]
        (redis! redis "SET" pending-key (str id "," tag)))
      (let [pending (redis! redis "GET" pending-key)]
        (when-not pending (throw (ex-info "Missing seeded upload ID" {})))
        (s3/check-pending-upload! url k (str/split pending #"," 2))))
    :checked))

(defrecord ProtocolClient [ports id phase node redis sqs-client s3-url]
  client/Client
  (open! [this _ n]
    (let [[r q s] (ports n)]
      (assoc this :node n :redis {:spec {:host "127.0.0.1" :port r}}
             :sqs-client (sqs/make-client "127.0.0.1" q "us-east-1" true)
             :s3-url (str "http://127.0.0.1:" s))))
  (setup! [_ _])
  (teardown! [_ _])
  (close! [_ _] (aws/stop sqs-client))
  (invoke! [_ _ op]
    (let [k (str id "-" node)]
      (try
        (assoc op :node node :type :ok :value
               (case (:f op)
                 :ttl (ttl! redis k phase)
                 :visibility (visibility! redis sqs-client k phase)
                 :multipart (multipart! redis s3-url k phase)
                 :increment (lua! redis id true)
                 (:read :final-read) (lua! redis id false)))
        (catch Exception e
          (assoc op :node node :type :fail :error (.getMessage e)))))))

(defn transition-checker [phase nodes]
  (reify checker/Checker
    (check [_ _ history _]
      (let [ops (remove #(= :nemesis (:process %)) history)
            results (remove #(= :invoke (:type %)) ops)
            pairs (mapcat #(partition-all 2 %) (vals (group-by :process ops)))
            paired? (every? (fn [[call reply]]
                              (and (= :invoke (:type call)) (= :ok (:type reply))
                                   (= (:f call) (:f reply)))) pairs)
            lua (filter #(#{:increment :read :final-read} (:f (first %))) pairs)
            rank (fn [op] (- (* 2 (:value op)) (if (= :increment (:f op)) 1 0)))
            failures (filterv #(not= :ok (:type %)) results)
            coverage (every? (fn [f] (= (set nodes) (set (map :node (filter #(= f (:f %)) results)))))
                             [:ttl :visibility :multipart :final-read])
            writes (map :value (filter #(= :increment (:f %)) results))
            ordered (and paired? (empty? failures)
                         (every? #(<= 0 (:value (second %)) increments) lua)
                         (every? true? (for [[_ a] lua [b c] lua :when (< (:index a) (:index b))]
                                         (<= (rank a) (rank c)))))
            reads (filter #(= :final-read (:f %)) results)
            valid (and coverage ordered
                       (= (if (= phase :seed) increments 0) (count (filter #(= :read (:f %)) results)))
                       (if (= phase :seed) (= (sort writes) (range 1 (inc increments))) (empty? writes))
                       (every? #(= increments (:value %)) reads))]
        {:valid? (boolean valid) :covered-all-nodes? coverage :ordered? ordered
         :failures failures :counts (frequencies (map :f results))}))))

(defn workload [{:keys [phase fixture redis-ports sqs-ports s3-ports]}]
  (let [nodes ["n1" "n2" "n3"]
        ports (zipmap nodes (mapv vector redis-ports sqs-ports s3-ports))
        each (fn [f] (gen/each-thread (gen/once {:f f})))]
    (when-not (and (#{:seed :recover} phase) (re-matches #"[a-zA-Z0-9-]{1,64}" (or fixture ""))
                   (every? #(= 3 (count %)) [redis-ports sqs-ports s3-ports]))
      (throw (ex-info "Expected a fixture, phase and three ports per adapter" {})))
    (merge tests/noop-test
           {:name (str "elastickv-protocol-" (name phase)) :nodes nodes :concurrency 3
            :ssh {:dummy true}
            :client (map->ProtocolClient {:ports ports :id fixture :phase phase})
            :generator (gen/clients (gen/phases (apply gen/any
                                      (map-indexed (fn [i ops]
                                        (gen/on-threads #{i} (map #(hash-map :f %) ops)))
                                        [[:ttl :multipart :visibility] [:visibility :ttl :multipart]
                                         [:multipart :visibility :ttl]]))
                                      (when (= phase :seed)
                                        (mapcat (fn [_] [{:f :increment} {:f :read}]) (range increments)))
                                      (each :final-read)))
            :checker (transition-checker phase nodes)})))

(defn- ports [s] (mapv parse-long (str/split s #",")))

(defn -main [& args]
  (cli/run-workload! args
    [[nil "--phase PHASE" "seed or recover" :parse-fn keyword]
     [nil "--fixture ID" "Unique ID shared across a restart"]
     [nil "--redis-ports PORTS" "Redis ports" :parse-fn ports :default [63791 63792 63793]]
     [nil "--sqs-ports PORTS" "SQS ports" :parse-fn ports :default [63501 63502 63503]]
     [nil "--s3-ports PORTS" "S3 ports" :parse-fn ports :default [63901 63902 63903]]]
    #(assoc % :local true) workload))
