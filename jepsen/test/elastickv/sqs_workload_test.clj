(ns elastickv.sqs-workload-test
  (:require [clojure.test :refer :all]
            [elastickv.sqs-common :as sqs]
            [elastickv.sqs-htfifo-workload :as htfifo]
            [elastickv.sqs-workload :as workload]
            [jepsen.checker :as checker]
            [jepsen.client :as client]))

(defn result [history]
  (checker/check (workload/standard-checker) {} history {}))

(defn sent [value] {:type :ok :f :send :value value})
(defn received [values deleted] {:type :ok :f :recv :value values :deleted deleted})

(deftest standard-delivery-contract
  (doseq [[label history valid? expected]
          [["unordered duplicates are allowed"
            [(sent "a") (sent "b") (received ["b" "a" "a"] ["a" "b"])]
            true {:duplicate-deliveries 1 :lost #{} :unexpected #{}}]
           ["acknowledged send is lost"
            [(sent "a") (received [] [])] false {:lost #{"a"}}]
           ["indeterminate send may arrive"
            [(sent "a") {:type :info :f :send :value "b"}
             (received ["a" "b"] ["a" "b"])]
            true {:in-flight-sends 1}]
           ["indeterminate send need not arrive"
            [(sent "a") {:type :info :f :send :value "b"} (received ["a"] ["a"])]
            true {:lost #{}}]
           ["unexpected body" [(sent "a") (received ["a" "corrupt"] ["a" "corrupt"])]
            false {:unexpected #{"corrupt"}}]
           ["failed send cannot explain a delivery"
            [(sent "a") {:type :fail :f :send :value "b"} (received ["a" "b"] ["a" "b"])]
            false {:unexpected #{"b"}}]
           ["failed delete is retried"
            [(sent "a") (received ["a"] []) (received ["a"] ["a"])]
            true {:duplicate-deliveries 1 :unacked #{}}]
           ["no successful delete" [(sent "a") (received ["a"] [])]
            false {:lost #{} :unacked #{"a"}}]
           ["malformed message" [(sent "a") (received ["a" nil] ["a"])]
            false {:unexpected #{nil}}]
           ["no work" [] false {:committed-sends 0}]]]
    (testing label
      (let [r (result history)]
        (is (= valid? (boolean (:valid? r))))
        (is (= expected (select-keys r (keys expected))))))))

(deftest client-keeps-send-identity-on-ambiguous-failure
  (let [c (:client (workload/sqs-workload {}))]
    (with-redefs [sqs/invoke! (fn [& _] (throw (ex-info "connection lost" {})))]
      (let [op (client/invoke! c {} {:type :invoke :f :send})]
        (is (= :info (:type op)))
        (is (= "jepsen-standard-1" (:value op)))))))

(deftest client-receives-bodies-even-when-delete-fails
  (let [calls (atom [])
        c (assoc (:client (workload/sqs-workload {})) :queue-url (atom "queue"))]
    (with-redefs [sqs/invoke! (fn [_ operation request]
                               (swap! calls conj [operation request])
                               (case operation
                                 :ReceiveMessage {:Messages [{:Body "a" :ReceiptHandle "receipt-a"}
                                                             {:Body "b" :ReceiptHandle "receipt-b"}]}
                                 :DeleteMessage (if (= "receipt-a" (:ReceiptHandle request))
                                                  (throw (ex-info "lost acknowledgement" {}))
                                                  {})))]
      (let [op (client/invoke! c {} {:type :invoke :f :recv})]
        (is (= :ok (:type op)))
        (is (= ["a" "b"] (:value op)))
        (is (= ["b"] (:deleted op)))
        (is (= ["lost acknowledgement"] (:delete-errors op)))
        (is (= [:ReceiveMessage :DeleteMessage :DeleteMessage] (mapv first @calls)))))))

(deftest clients-share-unique-message-identities
  (let [c (:client (workload/sqs-workload {}))
        bodies (atom [])]
    (with-redefs [sqs/invoke! (fn [_ _ request] (swap! bodies conj (:MessageBody request)) {})]
      (client/invoke! c {} {:f :send})
      (client/invoke! (assoc c :conn :another-worker) {} {:f :send})
      (is (= ["jepsen-standard-1" "jepsen-standard-2"] @bodies)))))

(deftest standard-queue-setup-does-not-request-fifo
  (let [request (atom nil)
        c (:client (workload/sqs-workload {:queue-name "standard"}))]
    (with-redefs [sqs/invoke! (fn [_ operation in]
                               (is (= :CreateQueue operation))
                               (reset! request in)
                               {:QueueUrl "url"})]
      ;; setup! return values are ignored by Jepsen. A separately opened
      ;; execution client must see the URL through shared state.
      (client/setup! c {})
      (is (= "url" @(:queue-url c)))
      (is (= {:QueueName "standard"} @request)))))

(deftest constructor-and-cli-options
  (let [opts (workload/prepare-opts {:nodes "n1,n2,n3" :sqs-ports [9321 9322 9323]
                                   :host "127.0.0.1" :faults "partition,kill"})
        test-map (workload/elastickv-sqs-test opts)]
    (is (= "elastickv-sqs-standard" (:name test-map)))
    (is (= {"n1" 9321 "n2" 9322 "n3" 9323} (:node->port (:client test-map))))
    (is (= "127.0.0.1" (:sqs-host test-map)))
    (is (boolean (:local opts)))))


(deftest setup-url-survives-execution-client-open
  (doseq [constructor [workload/elastickv-sqs-test htfifo/elastickv-sqs-htfifo-test]]
    (let [test-map (constructor {:local true :nodes ["n1"]})
          base (:client test-map)
          sent-request (atom nil)]
      (with-redefs [sqs/make-client (fn [& _] :fake-connection)
                    sqs/invoke! (fn [_ operation request]
                                  (case operation
                                    :CreateQueue {:QueueUrl "http://n1/queue"}
                                    :SendMessage (do (reset! sent-request request) {})))]
        ;; Core sets up a client before the interpreter opens its own copy.
        ;; setup!'s return value is deliberately ignored here, as in Jepsen.
        (client/setup! (client/open! base test-map "n1") test-map)
        (let [execution-client (client/open! base test-map "n1")
              op (client/invoke! execution-client test-map {:f :send})]
          (is (= :ok (:type op)))
          (is (= "http://n1/queue" (:QueueUrl @sent-request))))))))
