(ns elastickv.s3-workload-test
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [clojure.tools.cli :as tools.cli]
            [clj-http.client :as http]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.history :as history]
            [jepsen.independent :as independent]
            [elastickv.cli :as cli]
            [elastickv.s3-workload :as workload])
  (:import [com.sun.net.httpserver HttpHandler HttpServer]
           [java.net InetSocketAddress URLDecoder]
           [java.util.concurrent CountDownLatch Executors TimeUnit]))

(deftest builds-test-spec
  (let [test-map (workload/elastickv-s3-test {})]
    (is (map? test-map))
    (is (= "elastickv-s3-register" (:name test-map)))
    (is (= ["n1" "n2" "n3" "n4" "n5"] (:nodes test-map)))))

(deftest default-concurrency-is-10
  (let [test-map (workload/elastickv-s3-test {})]
    (is (= 10 (:concurrency test-map)))))

(deftest custom-options-override-defaults
  (let [test-map (workload/elastickv-s3-test
                   {:time-limit  60
                    :concurrency 20
                    :s3-port     9999})]
    (is (= 20 (:concurrency test-map)))))

(deftest register-checker-races-knossos-analyzers
  (let [linear-checker (get-in (workload/s3-register-checker)
                               [:checkers :linear])]
    (is (nil? (:algorithm linear-checker)))))

(deftest host-override-uses-provided-host
  (let [test-map (workload/elastickv-s3-test
                   {:s3-host "127.0.0.1"
                    :node->port {"n1" 9000 "n2" 9001}})
        c        (:client test-map)
        opened   (client/open! c test-map "n1")]
    (is (re-find #"http://127\.0\.0\.1:9000" (:url opened)))))

(def ^:private etag "\"70efdf2ec9b086079795c442636b55fb\"")
(def ^:private upload-id "private/a+b&c")
(def ^:private created
  {:status 200
   :body (str "<InitiateMultipartUploadResult xmlns='http://s3.amazonaws.com/doc/2006-03-01/'>"
              "<UploadId>private/a+b&amp;c</UploadId></InitiateMultipartUploadResult>")})
(def ^:private uploaded {:status 200 :headers {"etag" etag}})
(def ^:private completed
  {:status 200
   :body "<CompleteMultipartUploadResult><ETag>&quot;0123456789abcdef0123456789abcdef-1&quot;</ETag></CompleteMultipartUploadResult>"})
(def ^:private no-upload
  {:status 404 :body "<Error><Code>NoSuchUpload</Code></Error>"})

(defn- invoke-write
  "Stub the HTTP boundary, retaining clj-http request options and real workers."
  ([] (invoke-write {}))
  ([overrides]
   (let [calls (atom [])
         parts (atom 0)
         started (CountDownLatch. 2)
         c (workload/->S3Client {} "http://stub:9000")
         request (fn [method url opts]
                   (let [stage (if (= :post method)
                                 (if (contains? (:query-params opts) "uploads") :create :complete)
                                 (if (= 1 (swap! parts inc)) :initial :replace))
                         _ (swap! calls conj {:stage stage :method method :url url :opts opts})
                         response (get overrides stage
                                       (case stage :create created :complete completed uploaded))]
                     (when (#{:replace :complete} stage)
                       (.countDown started)
                       (when-not (.await started 2 TimeUnit/SECONDS)
                         (throw (ex-info "HTTP race did not start both requests" {}))))
                     (if (fn? response) (response) response)))]
     (with-redefs [http/post #(request :post %1 %2)
                   http/put #(request :put %1 %2)]
       {:result (client/invoke! c {:multipart true}
                               {:type :invoke :f :write :value (independent/tuple 3 17)})
        :calls @calls}))))

(deftest multipart-protocol-flow
  (let [{:keys [result calls]} (invoke-write)
        by-stage (into {} (map (juxt :stage identity) calls))]
    (is (= :ok (:type result)))
    (is (= (independent/tuple 3 17) (:value result)))
    (is (= 4 (count calls)))
    (is (= [:create :initial] (mapv :stage (take 2 calls))))
    (is (= #{:replace :complete} (set (map :stage (drop 2 calls)))))
    (is (every? #(= "http://stub:9000/jepsen-test/key-multipart-3" (:url %)) calls))
    (is (= {"uploads" ""} (get-in by-stage [:create :opts :query-params])))
    (doseq [stage [:initial :replace]]
      (is (= "17" (get-in by-stage [stage :opts :body])))
      (is (= {"uploadId" upload-id "partNumber" 1}
             (get-in by-stage [stage :opts :query-params]))))
    (is (= {"uploadId" upload-id} (get-in by-stage [:complete :opts :query-params])))
    (is (= (str "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"
                etag "</ETag></Part></CompleteMultipartUpload>")
           (get-in by-stage [:complete :opts :body])))
    (doseq [{:keys [opts]} calls]
      (is (pos? (:conn-timeout opts)))
      (is (pos? (:connection-request-timeout opts)))
      (is (pos? (:socket-timeout opts)))
      (is (= :none (:redirect-strategy opts)))
      (is (false? ((:retry-handler opts) nil 1 nil)))))
  (let [c (workload/->S3Client {} "http://stub:9000")]
    (with-redefs [http/get (fn [url _]
                           (is (str/ends-with? url "/key-multipart-3"))
                           {:status 200 :body " 17\n"})]
      (is (= (independent/tuple 3 17)
             (:value (client/invoke! c {:multipart true}
                                    {:f :read :value (independent/tuple 3 nil)})))))))

(deftest replacement-no-such-upload-needs-confirmed-completion
  (is (= :ok (get-in (invoke-write {:replace no-upload}) [:result :type])))
  (doseq [completion [no-upload
                      {:status 503 :body "<Error><Code>SlowDown</Code></Error>"}
                      {:status 200 :body "<Error><Code>InternalError</Code></Error>"}
                      (fn [] (throw (java.net.SocketTimeoutException. "lost reply")))]]
    (let [{:keys [result calls]} (invoke-write {:replace no-upload :complete completion})]
      (is (= :info (:type result)))
      (is (= 1 (count (filter #(= :complete (:stage %)) calls))))))
  (doseq [replacement [{:status 404 :body "<Error><Code>NoSuchKey</Code></Error>"}
                       {:status 500 :body "failure"}
                       {:status 200 :headers {}}
                       {:status 200 :headers {"etag" "bad"}}
                       {:status 200 :headers {"etag" "\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\""}}
                       {:status 404 :body "<Error><Code>NoSuchUpload</Code><Code>NoSuchKey</Code></Error>"}]]
    (is (= :info (get-in (invoke-write {:replace replacement}) [:result :type])))))

(deftest malformed-initiation-never-starts-parts-or-completion
  (doseq [body ["" "<InitiateMultipartUploadResult>"
                "<InitiateMultipartUploadResult/>"
                "<InitiateMultipartUploadResult><UploadId/></InitiateMultipartUploadResult>"
                "<InitiateMultipartUploadResult><UploadId> </UploadId></InitiateMultipartUploadResult>"
                "<InitiateMultipartUploadResult><UploadId>bad id</UploadId></InitiateMultipartUploadResult>"
                "<InitiateMultipartUploadResult><UploadId><x>nested</x></UploadId></InitiateMultipartUploadResult>"
                "<InitiateMultipartUploadResult><UploadId>a</UploadId><UploadId>b</UploadId></InitiateMultipartUploadResult>"
                "<Error><Code>SlowDown</Code></Error>"
                "<InitiateMultipartUploadResult><UploadId>a</UploadId><Error/></InitiateMultipartUploadResult>"
                "<!DOCTYPE x [<!ENTITY id 'arbitrary'>]><InitiateMultipartUploadResult><UploadId>&id;</UploadId></InitiateMultipartUploadResult>"
                "<!DOCTYPE x [<!ENTITY id SYSTEM 'file:///etc/passwd'>]><InitiateMultipartUploadResult><UploadId>&id;</UploadId></InitiateMultipartUploadResult>"
                "<!DOCTYPE x SYSTEM 'http://127.0.0.1:1/evil'><InitiateMultipartUploadResult><UploadId>a</UploadId></InitiateMultipartUploadResult>"]]
    (let [{:keys [result calls]} (invoke-write {:create {:status 200 :body body}})]
      (is (= :info (:type result)) body)
      (is (= [:create] (mapv :stage calls)) body)))
  (doseq [status [301 400 500 503]]
    (is (= :info (get-in (invoke-write {:create (assoc created :status status)}) [:result :type])))))

(deftest first-part-must-return-a-valid-server-etag
  (doseq [response [{:status 200}
                    {:status 200 :headers {"etag" ""}}
                    {:status 200 :headers {"etag" "not-an-etag"}}
                    {:status 200 :headers {"etag" "<injection/>"}}
                    {:status 200 :headers {"etag" "70efdf2ec9b086079795c442636b55fb"}}
                    no-upload
                    {:status 503 :body "<Error><Code>SlowDown</Code></Error>"}]]
    (let [{:keys [result calls]} (invoke-write {:initial response})]
      (is (= :info (:type result)))
      (is (= [:create :initial] (mapv :stage calls))))))

(deftest completion-validates-http-and-xml-without-replaying
  (doseq [response [{:status 301 :body (:body completed)}
                    {:status 503 :body (:body completed)}
                    {:status 200 :body ""}
                    {:status 200 :body "<CompleteMultipartUploadResult>"}
                    {:status 200 :body "<CompleteMultipartUploadResult/>"}
                    {:status 200 :body "<CompleteMultipartUploadResult><ETag>bad</ETag></CompleteMultipartUploadResult>"}
                    {:status 200 :body "<Error><Code>InternalError</Code></Error>"}
                    {:status 200 :body "<CompleteMultipartUploadResult><ETag>ok</ETag><Error><Code>InternalError</Code></Error></CompleteMultipartUploadResult>"}
                    (fn [] (throw (java.net.SocketTimeoutException. "unknown completion")))
                    (fn [] (throw (org.apache.http.NoHttpResponseException. "unknown completion")))]]
    (let [{:keys [result calls]} (invoke-write {:complete response})]
      (is (= :info (:type result)))
      (is (= 1 (count (filter #(= :complete (:stage %)) calls)))))))

(deftest race-timeout-cancels-and-joins-workers
  (let [active (atom 0)
        stopped (CountDownLatch. 1)]
    (with-redefs-fn {#'workload/multipart-race-timeout-ms 100}
      #(let [{:keys [result]} (invoke-write
                               {:replace (fn []
                                           (swap! active inc)
                                           (try (Thread/sleep 10000)
                                                uploaded
                                                (finally
                                                  (swap! active dec)
                                                  (.countDown stopped))))})]
         (is (= :info (:type result)))
         (is (zero? @active))
         (is (zero? (.getCount stopped)))))))

(deftest uncertain-completion-still-joins-replacement
  (let [finished (atom false)]
    (is (= :info (get-in (invoke-write
                          {:complete #(throw (java.net.SocketTimeoutException. "unknown"))
                           :replace #(do (Thread/sleep 20) (reset! finished true) uploaded)})
                         [:result :type])))
    (is @finished)))

(defn- with-http-stub
  [respond f]
  (let [server (HttpServer/create (InetSocketAddress. "127.0.0.1" 0) 0)
        executor (Executors/newFixedThreadPool 4)
        calls (atom [])]
    (.setExecutor server executor)
    (.createContext server "/"
                    (reify HttpHandler
                      (handle [_ exchange]
                        (try
                          (let [uri (.getRequestURI exchange)
                                query (into {} (for [pair (str/split (or (.getRawQuery uri) "") #"&")
                                                     :when (seq pair)
                                                     :let [[k v] (str/split pair #"=" 2)]]
                                                 [(URLDecoder/decode k "UTF-8")
                                                  (URLDecoder/decode (or v "") "UTF-8")]))
                                request {:method (.getRequestMethod exchange)
                                         :path (.getPath uri) :query query
                                         :body (slurp (.getRequestBody exchange))}
                                _ (swap! calls conj request)
                                response (respond request)]
                            (when-not (:drop response)
                              (doseq [[k v] (:headers response)]
                                (.set (.getResponseHeaders exchange) k v))
                              (let [bytes (.getBytes ^String (or (:body response) "") "UTF-8")]
                                (.sendResponseHeaders exchange (:status response)
                                                      (if (seq bytes) (alength bytes) -1))
                                (when (seq bytes)
                                  (with-open [out (.getResponseBody exchange)]
                                    (.write out bytes))))))
                          (finally (.close exchange))))))
    (try
      (.start server)
      (f (str "http://127.0.0.1:" (.getPort (.getAddress server))) calls)
      (finally
        (.stop server 0)
        (.shutdownNow executor)
        (is (.awaitTermination executor 5 TimeUnit/SECONDS))))))

(deftest multipart-over-real-http-does-not-replay-an-uncertain-completion
  (doseq [outcome [:success :no-upload :lost-reply :embedded-error]]
    (let [parts (atom 0)
          raced (CountDownLatch. 2)]
      (with-http-stub
        (fn [{:keys [method query]}]
          (cond
            (contains? query "uploads") created
            (and (= "PUT" method) (= 1 (swap! parts inc))) uploaded
            :else
            (do
              (.countDown raced)
              (when-not (.await raced 2 TimeUnit/SECONDS)
                (throw (ex-info "HTTP calls did not overlap" {})))
              (if (= "PUT" method)
                (if (= :success outcome) uploaded no-upload)
                (case outcome
                  :lost-reply {:drop true}
                  :embedded-error {:status 200 :body "<Error><Code>InternalError</Code></Error>"}
                  completed)))))
        (fn [url calls]
          (let [c (workload/->S3Client {} url)
                result (client/invoke! c {:multipart true}
                                       {:f :write :value (independent/tuple 3 17)})
                requests @calls
                completions (filter #(and (= "POST" (:method %))
                                          (contains? (:query %) "uploadId")) requests)]
            (is (= (if (#{:success :no-upload} outcome) :ok :info) (:type result))
                (str outcome ": " (:error result)))
            (is (= 4 (count requests)))
            (is (= 1 (count completions)))
            (is (= upload-id (get-in (first completions) [:query "uploadId"])))
            (is (every? #(= "/jepsen-test/key-multipart-3" (:path %)) requests))
            (is (= ["17" "17"] (mapv :body (filter #(= "PUT" (:method %)) requests))))))))))

(deftest real-http-timeout-closes-sockets-and-joins-workers
  (let [parts (atom 0)
        raced (CountDownLatch. 2)]
    (with-redefs-fn {#'workload/multipart-race-timeout-ms 500}
      #(with-http-stub
         (fn [{:keys [method query]}]
           (cond
             (contains? query "uploads") created
             (and (= "PUT" method) (= 1 (swap! parts inc))) uploaded
             :else
             (do
               (.countDown raced)
               (.await raced 2 TimeUnit/SECONDS)
               (if (= "PUT" method)
                 (do (.await (CountDownLatch. 1) 10 TimeUnit/SECONDS) uploaded)
                 completed))))
         (fn [url calls]
           (let [c (workload/->S3Client {} url)
                 result (client/invoke! c {:multipart true}
                                        {:f :write :value (independent/tuple 3 17)})]
             (is (= :info (:type result)))
             ;; A timed-out worker is cancelled, its socket closed, and its
             ;; executor joined before invoke! can return this outcome.
             (is (not= "Multipart HTTP workers did not terminate" (:error result)))
             (is (= 4 (count @calls)))))))))

(defn- events
  [f k v & [attrs]]
  (let [op (merge {:process 0 :f f :value (independent/tuple k v)} attrs)]
    [(assoc op :type :invoke) (assoc op :type :ok)]))

(defn- final-check
  [key-count ops]
  (checker/check (workload/multipart-final-reads-checker key-count)
                 {} (history/history ops) {}))

(deftest final-read-gate-requires-every-key-and-nonvacuous-writes
  (let [write (events :write 0 17)
        read0 (events :read 0 17 {:final true})
        read1 (events :read 1 nil {:final true})
        valid (vec (concat write read0 read1))]
    (is (true? (:valid? (final-check 2 valid))))
    (is (= 1 (:completed-writes (final-check 2 valid))))
    (is (= 2 (:completed-reads (final-check 2 valid))))
    (doseq [ops [[] write read0 (concat read0 read1)
                 (concat write read0)
                 (concat write read0 (take 1 read1))
                 (concat write read0 (events :read 2 nil {:final true}))
                 (concat write read0 read0 read1)
                 (concat write (events :read 0 nil {:final true}) read1)
                 (concat write (events :read 0 :bad {:final true}) read1)]]
      (is (false? (:valid? (final-check 2 ops)))))
    (is (false? (:valid? (final-check 0 []))))
    (doseq [type [:info :fail]]
      (let [ops (concat write read0 [(first read1) (assoc (second read1) :type type :error "HTTP 500")])]
        (is (false? (:valid? (final-check 2 ops))))))))

(deftest failed-final-get-cannot-pass-the-gate
  (let [op (assoc (first (events :read 0 nil)) :final true)
        c (workload/->S3Client {} "http://stub:9000")]
    (doseq [response [{:status 500 :body "missing chunks"}
                      {:status 200 :body "not a long"}
                      {:status 404 :body ""}]]
      (with-redefs [http/get (fn [_ _] response)]
        (let [result (client/invoke! c {:multipart true} op)]
          (is (false? (:valid? (final-check 1 (concat (events :write 0 17) [op result]))))))))))

(deftest multipart-cli-and-checker-are-opt-in
  (let [default (tools.cli/parse-opts [] workload/s3-cli-opts)
        enabled (tools.cli/parse-opts ["--multipart"] workload/s3-cli-opts)
        normal (workload/elastickv-s3-test {:local true})
        multipart (workload/elastickv-s3-test {:local true :multipart true})]
    (is (empty? (:errors enabled)))
    (is (false? (get-in default [:options :multipart])))
    (is (true? (get-in enabled [:options :multipart])))
    (is (nil? (:multipart normal)))
    (is (true? (:multipart multipart)))
    (is (= "elastickv-s3-register" (:name normal)))
    (is (= "elastickv-s3-multipart-register" (:name multipart)))
    (is (nil? (:final-generator multipart)))
    (is (= #{:register :complete-final-reads} (set (keys (:checkers (:checker multipart))))))
    (is (nil? (:checkers (:checker normal))))))

(deftest multipart-test-generator-ends-with-all-final-reads
  (let [test (workload/elastickv-s3-test {:local true :multipart true :time-limit 0 :key-count 3})
        ctx (gen/context test)
        ops (loop [g (:generator test), ops []]
              (let [[op g'] (gen/op g test ctx)]
                (if (or (nil? op) (= :pending op))
                  ops
                  (recur g' (conj ops op)))))
        finals (filter :final ops)]
    (is (= [0 1 2] (sort (map (comp first :value) finals))))
    (is (every? #(= :read (:f %)) finals))))

(deftest default-mode-still-uses-put-and-get
  (let [calls (atom [])
        c (workload/->S3Client {} "http://stub:9000")]
    (with-redefs [http/put (fn [url opts] (swap! calls conj [:put url (:body opts)]) {:status 200})
                  http/post (fn [& _] (throw (ex-info "Unexpected multipart" {})))
                  http/get (fn [url _] (swap! calls conj [:get url]) {:status 200 :body "17"})]
      (is (= :ok (:type (client/invoke! c {} {:f :write :value (independent/tuple 3 17)}))))
      (is (= (independent/tuple 3 17)
             (:value (client/invoke! c {} {:f :read :value (independent/tuple 3 nil)}))))
      (is (= [[:put "http://stub:9000/jepsen-test/key-3" "17"]
              [:get "http://stub:9000/jepsen-test/key-3"]] @calls)))))
