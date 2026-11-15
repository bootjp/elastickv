(ns elastickv.elle-checker-test
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.test :refer :all]
            [elle.list-append :as append]
            [elastickv.dynamodb-workload :as dynamo]
            [elastickv.elle-checker :as elle]
            [elastickv.redis-workload :as redis]
            [elastickv.s3-workload :as s3]
            [jepsen.checker :as checker]
            [jepsen.history :as history]
            [jepsen.independent :as independent]
            [jepsen.store :as store])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn with-directory [f]
  (let [dir (.toFile (Files/createTempDirectory "elle-test" (make-array FileAttribute 0)))]
    (try (f dir)
         (finally
           (doseq [file (reverse (file-seq dir))] (io/delete-file file))))))

(defn s3-history [observed write-type]
  [{:process 0 :time 0 :type :invoke :f :write :value (independent/tuple :x 1)}
   {:process 0 :time 1 :type write-type :f :write :value (independent/tuple :x 1)}
   {:process 1 :time 2 :type :invoke :f :read :value (independent/tuple :x nil)}
   {:process 1 :time 3 :type :ok :f :read :value (independent/tuple :x observed) :final true :node "n3"}])

(deftest s3-translation-preserves-events-and-uncertainty
  (let [events (conj (s3-history 1 :info) {:process :nemesis :type :info :f :stop})
        normalized (elle/transaction-history :s3 events)]
    (is (= (mapv :type events) (mapv :type normalized)))
    (is (= (mapv :process events) (mapv :process normalized)))
    (is (= [[:w :x 1]] (:value (second normalized))))
    (is (= [[:r :x 1]] (:value (nth normalized 3))))
    (is (:final (nth normalized 3)))
    (is (= "n3" (:node (nth normalized 3))))
    (is (= :stop (:f (last normalized))))
    (is (thrown? Exception (elle/s3-operation {:f :recv :value []})))
    (is (thrown? Exception (elle/s3-operation {:f :write :value 1})))))

(deftest s3-elle-detects-stale-initial-value-and-aborted-read
  (with-directory
    (fn [dir]
      (is (true? (:valid? (elle/check-history :s3 (s3-history 1 :ok) dir))))
      (is (false? (:valid? (elle/check-history :s3 (s3-history nil :ok) dir))))
      (let [dirty (elle/check-history :s3 (s3-history 1 :fail) dir)]
        (is (false? (:valid? dirty)))
        (is (some #{:G1a} (:anomaly-types dirty))))
      ;; An ambiguous write alone is insufficient for a definitive verdict.
      (is (= :unknown (:valid? (elle/check-history :s3 (s3-history 1 :info) dir))))
      (is (thrown? Exception (elle/check-history :s3 (concat (s3-history 1 :ok)
                                                           (s3-history 1 :ok)) dir))))))

(deftest append-cycle-generates-diagnostics-and-retains-replay-input
  (with-directory
    (fn [dir]
      (let [input (io/file dir "input.edn")
            output (io/file dir "analysis")
            transactions [{:process 0 :type :ok :f :txn :value [[:append :x 1] [:r :y [1]]]}
                    {:process 1 :type :ok :f :txn :value [[:append :x 2] [:append :y 1]]}
                    {:process 2 :type :ok :f :txn :value [[:r :x [1 2]]]}]
            events (into (mapv #(assoc % :type :invoke) transactions) transactions)]
        (spit input (apply str (map #(str (pr-str %) "\n") events)))
        (let [result (elle/analyze-file! :list-append input output)]
          (is (false? (:valid? result)))
          (is (some #{:G1c} (:anomaly-types result)))
          (is (= false (:valid? (edn/read-string (slurp (io/file output "elle-results.edn"))))))
          (is (= (mapv :value events) (mapv :value (elle/read-history (io/file output "elle-history.edn")))))
          (is (.isFile (io/file output "G1c.txt"))))
        (is (some #(.endsWith (.getName %) ".svg") (file-seq output)))))))

(deftest redis-and-dynamodb-already-use-strict-elle-append
  (with-directory
    (fn [dir]
      (doseq [build [redis/elastickv-append-workload dynamo/dynamodb-append-workload]]
        (let [seen (atom nil)]
          (with-redefs [store/base-dir (.getPath dir)
                        append/check (fn [opts h]
                                       (reset! seen [opts h])
                                       {:valid? false :anomaly-types [:G1c]})]
            (is (false? (:valid? (checker/check (:checker (build {}))
                                               {:name "append" :start-time "test"}
                                               (history/history []) {}))))
            (is (= elle/models (:consistency-models (first @seen))))))))))

(deftest s3-keeps-knossos-and-adds-elle-in-normal-and-multipart-workloads
  (doseq [multipart? [false true]]
    (let [check (:checker (s3/s3-register-workload {:multipart multipart?}))
          composed (if multipart? (get-in check [:checkers :register]) check)]
      (is (some? (get-in composed [:checkers :register])))
      (is (some? (get-in composed [:checkers :elle]))))))

(deftest replay-rejects-incompatible-input
  (is (thrown? Exception (elle/transaction-history :sqs [])))
  (with-directory
    (fn [dir]
      (let [input (io/file dir "bad.edn")]
        (spit input "#=(System/exit 0)")
        (is (thrown? Exception (elle/read-history input)))
        (spit input "[]")
        (is (thrown? Exception (elle/read-history input)))
        (spit input "{:process :nemesis :type :info :f :stop :value #example.DB {:node 1}}")
        (is (= (tagged-literal 'example.DB {:node 1})
               (:value (first (elle/read-history input)))))))))

(deftest s3-checker-persists-normalized-evidence-in-the-jepsen-store
  (with-directory
    (fn [dir]
      (with-redefs [store/base-dir (.getPath dir)]
        (let [test {:name "s3" :start-time "test"}
              result (checker/check (elle/s3-checker) test
                                    (history/history (s3-history 1 :ok))
                                    {:subdirectory "consistency"})
              out (store/path test "consistency" "elle")]
          (is (true? (:valid? result)))
          (is (.isFile (io/file out "elle-results.edn")))
          (is (= :txn (:f (first (elle/read-history (io/file out "elle-history.edn")))))))))))

(deftest filesystem-initial-state-is-not-an-unobserved-write
  (with-directory
    (fn [dir]
      (let [events (mapv #(if (= :write (:f %))
                            (assoc % :value (independent/tuple :x 0)) %)
                         (s3-history -1 :ok))
            normalized (elle/transaction-history :filesystem events)]
        (is (false? (:valid? (elle/check-history :filesystem events dir))))
        (is (= [[:r :x nil]] (:value (last normalized))))
        (is (= [[:w :x 0]] (:value (first normalized))))
        (is (thrown? Exception (elle/transaction-history :filesystem
                                 [{:type :invoke :process 0 :f :write :value [:x -1]}]))))
      (doseq [kind [:grpc :filesystem :independent-register]]
        (is (true? (:valid? (elle/check-history kind (s3-history 1 :ok) dir))))))))
