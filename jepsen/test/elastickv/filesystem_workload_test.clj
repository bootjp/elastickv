(ns elastickv.filesystem-workload-test
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.tools.cli :as tools.cli]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.history :as history]
            [jepsen.independent :as independent]
            [elastickv.filesystem-workload :as workload])
  (:import (java.nio.file Files)
           (java.nio.file.attribute FileAttribute)))

(defn- scratch-dir []
  (str (Files/createTempDirectory "elastickv-fs-workload" (make-array FileAttribute 0))))

(defn- delete-tree! [dir]
  (doseq [f (reverse (file-seq (io/file dir)))]
    (io/delete-file f true)))

;; ---------------------------------------------------------------------------
;; Record encoding
;; ---------------------------------------------------------------------------

(deftest encode-value-pads-to-fixed-width
  (doseq [v [-1 0 7 123456789]]
    (let [data (workload/encode-value v)]
      (is (= workload/record-width (alength data)))
      (is (= v (workload/decode-value data))))))

(deftest encode-value-rejects-oversized-values
  (is (thrown? clojure.lang.ExceptionInfo
               (workload/encode-value 12345678901234567))))

(deftest decode-value-treats-blank-record-as-nil
  (is (nil? (workload/decode-value (byte-array 0))))
  (is (nil? (workload/decode-value (.getBytes "        ")))))

(deftest local-read-rejects-short-records
  (let [dir (scratch-dir)]
    (try
      (let [path (workload/register-path dir 0)]
        (.mkdirs (io/file (workload/workload-dir dir)))
        (spit path "7")
        (is (thrown? java.io.EOFException (workload/local-read path))))
      (finally
        (delete-tree! dir)))))

(deftest decode-value-rejects-garbage
  (is (thrown? NumberFormatException
               (workload/decode-value (.getBytes "not-a-number")))))

;; ---------------------------------------------------------------------------
;; Paths and option parsing
;; ---------------------------------------------------------------------------

(deftest register-paths-live-under-workload-dir
  (is (= "/mnt/x/jepsen" (workload/workload-dir "/mnt/x")))
  (is (= "/mnt/x/jepsen/reg-3" (workload/register-path "/mnt/x" 3)))
  (let [tmp (workload/temp-path "/mnt/x" 3)]
    (is (str/starts-with? tmp "/mnt/x/jepsen/reg-3.tmp-"))
    (is (not= tmp (workload/temp-path "/mnt/x" 3)))))

(deftest node-mount-dir-mapping
  (is (= {"n1" "/a" "n2" "/b"}
         (workload/node->mount-dir ["n1" "n2"] ["/a" "/b"] nil)))
  (is (= {"n1" "/mnt/fs" "n2" "/mnt/fs"}
         (workload/node->mount-dir ["n1" "n2"] nil "/mnt/fs")))
  (is (= {"n1" workload/default-mount-dir}
         (workload/node->mount-dir ["n1"] nil nil)))
  (is (thrown? clojure.lang.ExceptionInfo
               (workload/node->mount-dir ["n1" "n2"] ["/a"] nil))))

(deftest parse-write-mode-accepts-known-modes
  (is (= :overwrite (workload/parse-write-mode "overwrite")))
  (is (= :rename (workload/parse-write-mode "RENAME")))
  (is (thrown? IllegalArgumentException (workload/parse-write-mode "truncate"))))

;; ---------------------------------------------------------------------------
;; Retries
;; ---------------------------------------------------------------------------

(deftest retryable-errors-are-eagain-and-eio
  (is (workload/retryable-error? (java.io.IOException. "Resource temporarily unavailable")))
  (is (workload/retryable-error? (java.io.IOException. "Input/output error")))
  (is (workload/retryable-error?
        (RuntimeException. "dd: error writing 'reg-1': Input/output error")))
  (is (not (workload/retryable-error? (java.io.IOException. "No space left on device"))))
  (is (not (workload/retryable-error? (java.nio.file.NoSuchFileException. "reg-1")))))

(defn- nonzero-exit
  "Mimics the exception jepsen.control throws for a non-zero remote exit: the
   message quotes the command line, the data carries the streams."
  [cmd err]
  (ex-info (str "Command exited with non-zero status 1 on node n1:\n" cmd "\n\nSTDERR:\n" err)
           {:type :jepsen.control/nonzero-exit :cmd cmd :exit 1 :out "" :err err}))

(deftest error-text-ignores-the-remote-command-line
  (let [cmd (workload/remote-read-cmd "/mnt/fs" "/mnt/fs/jepsen/reg-1")]
    (is (= "head: error reading 'reg-1': Input/output error\n"
           (workload/error-text (nonzero-exit cmd "head: error reading 'reg-1': Input/output error"))))
    (is (= "boom" (workload/error-text (java.io.IOException. "boom"))))
    (is (= "" (workload/error-text (java.io.IOException.))))))

(deftest data-loss-kind-classifies-lost-registers-only
  (let [cmd (workload/remote-read-cmd "/mnt/fs" "/mnt/fs/jepsen/reg-1")]
    (is (= :missing (workload/data-loss-kind (java.nio.file.NoSuchFileException. "reg-1"))))
    (is (= :missing (workload/data-loss-kind (java.io.FileNotFoundException. "/mnt/fs/jepsen/reg-1 (No such file or directory)"))))
    (is (= :missing (workload/data-loss-kind (java.io.IOException. "No such file or directory"))))
    (is (= :missing (workload/data-loss-kind (nonzero-exit cmd "stat: cannot statx '/mnt/fs/jepsen/reg-1': No such file or directory"))))
    (is (= :truncated (workload/data-loss-kind (java.io.EOFException.))))
    (is (= :truncated (workload/data-loss-kind (nonzero-exit cmd "short record"))))
    (is (= :corrupt (workload/data-loss-kind (NumberFormatException. "For input string: \"garbage\""))))
    ;; The read could not be performed, but nothing says the register is gone.
    (is (nil? (workload/data-loss-kind (java.io.FileNotFoundException. "reg-1 (Resource temporarily unavailable)"))))
    (is (nil? (workload/data-loss-kind (java.io.FileNotFoundException. "reg-1 (Transport endpoint is not connected)"))))
    (is (nil? (workload/data-loss-kind (java.io.IOException. "Input/output error"))))
    (is (nil? (workload/data-loss-kind (ex-info "mount unavailable: /mnt/fs" {}))))
    (is (nil? (workload/data-loss-kind (nonzero-exit cmd "mount unavailable: /mnt/fs"))))
    ;; The command line quotes the markers; only the streams count.
    (is (nil? (workload/data-loss-kind (nonzero-exit cmd "ssh_exchange_identification: Connection closed"))))
    (is (nil? (workload/data-loss-kind (RuntimeException. "ssh failed"))))))

(deftest reads-retry-data-loss-shaped-errors
  (is (workload/read-retryable-error? (java.io.IOException. "Input/output error")))
  (is (workload/read-retryable-error? (java.nio.file.NoSuchFileException. "reg-1")))
  (is (workload/read-retryable-error? (java.io.EOFException.)))
  (is (workload/read-retryable-error? (NumberFormatException. "x")))
  (is (not (workload/read-retryable-error? (ex-info "mount unavailable: /mnt/fs" {}))))
  (is (not (workload/read-retryable-error? (java.io.FileNotFoundException. "reg-1 (Transport endpoint is not connected)")))))

(deftest with-retries-accepts-a-custom-predicate
  (let [calls (atom 0)]
    (is (thrown? java.nio.file.NoSuchFileException
                 (workload/with-retries 3 workload/read-retryable-error?
                   (fn []
                     (swap! calls inc)
                     (throw (java.nio.file.NoSuchFileException. "reg-1"))))))
    (is (= 3 @calls))))

(deftest with-retries-retries-only-retryable-errors
  (let [calls (atom 0)]
    (is (= :done (workload/with-retries 3 (fn []
                                             (if (< (swap! calls inc) 3)
                                               (throw (java.io.IOException. "Input/output error"))
                                               :done)))))
    (is (= 3 @calls)))
  (let [calls (atom 0)]
    (is (thrown-with-msg? java.io.IOException #"Input/output error"
                          (workload/with-retries 2 (fn []
                                                     (swap! calls inc)
                                                     (throw (java.io.IOException. "Input/output error"))))))
    (is (= 2 @calls)))
  (let [calls (atom 0)]
    (is (thrown? java.nio.file.NoSuchFileException
                 (workload/with-retries 5 (fn []
                                            (swap! calls inc)
                                            (throw (java.nio.file.NoSuchFileException. "reg-1"))))))
    (is (= 1 @calls))))

;; ---------------------------------------------------------------------------
;; Test spec
;; ---------------------------------------------------------------------------

(deftest builds-test-spec
  (let [test-map (workload/elastickv-filesystem-test {})]
    (is (map? test-map))
    (is (= "elastickv-filesystem-register-overwrite" (:name test-map)))
    (is (= ["n1" "n2" "n3" "n4" "n5"] (:nodes test-map)))
    (is (= 10 (:concurrency test-map)))
    (is (= :overwrite (:write-mode test-map)))
    (is (= workload/default-max-retries (:max-retries (:client test-map))))
    (is (= (zipmap ["n1" "n2" "n3" "n4" "n5"] (repeat workload/default-mount-dir))
           (:node->mount-dir test-map)))))

(deftest rename-mode-changes-name-and-threads-through
  (let [test-map (workload/elastickv-filesystem-test {:write-mode :rename
                                                      :concurrency 4
                                                      :nodes ["n1" "n2"]
                                                      :mount-dirs ["/tmp/a" "/tmp/b"]
                                                      :local true})]
    (is (= "elastickv-filesystem-register-rename" (:name test-map)))
    (is (= 4 (:concurrency test-map)))
    (is (= {"n1" "/tmp/a" "n2" "/tmp/b"} (:node->mount-dir test-map)))
    (is (true? (:local test-map)))))

(deftest unknown-write-mode-is-rejected
  (is (thrown? clojure.lang.ExceptionInfo
               (workload/elastickv-filesystem-test {:write-mode :truncate}))))

(defn- drain-generator
  "Pulls every op out of a pure (seq/limit/repeat/mix) generator with a
   fixed context; nothing here blocks on completions."
  [g]
  (let [test {:concurrency 2}
        ctx  (gen/context test)]
    (loop [g g, ops []]
      (let [[op g'] (gen/op g test ctx)]
        (if (or (nil? op) (= :pending op))
          ops
          (recur g' (conj ops op)))))))

(deftest key-generator-issues-exactly-max-writes-writes
  (dotimes [_ 5]
    (let [ops    (drain-generator (workload/register-key-generator 25))
          writes (filter #(= :write (:f %)) ops)
          reads  (filter #(= :read (:f %)) ops)]
      (is (= 25 (count writes)))
      (is (= (range 25) (map :value writes)))
      (is (<= 1 (count reads) 25))
      (is (= (count ops) (+ (count writes) (count reads)))))))

(deftest final-read-phase-reads-every-register-from-every-mount
  ;; Thread t serves node (mod t node-count); 3 nodes x 2 threads each.
  (let [test {:concurrency 6 :nodes ["n1" "n2" "n3"]}
        ctx  (gen/context test)
        ops  (loop [g (workload/final-read-phase 2 3), ops []]
               (let [[op g'] (gen/op g test ctx)]
                 (if (or (nil? op) (= :pending op))
                   ops
                   (recur g' (conj ops op)))))
        by-node (group-by #(mod (:process %) 3) ops)]
    (is (= 6 (count ops)))
    (is (every? :final ops))
    (is (= #{0 1 2} (set (keys by-node))))
    (doseq [[_ node-ops] by-node]
      (is (= [0 1] (sort (map (comp first :value) node-ops)))))))

(deftest concurrency-must-cover-every-node
  ;; 2 workers is a multiple of threads-per-key but leaves n3 without a
  ;; client, so its mount would never be exercised or final-read.
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"at least the node count"
                        (workload/elastickv-filesystem-test {:nodes ["n1" "n2" "n3"] :mount-dir "/mnt/fs" :local true
                                                             :concurrency 2})))
  (is (= 6 (:concurrency (workload/elastickv-filesystem-test {:nodes ["n1" "n2" "n3"] :mount-dir "/mnt/fs" :local true
                                                               :concurrency 6})))))

(deftest concurrency-must-be-a-multiple-of-threads-per-key
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"multiple of --threads-per-key"
                        (workload/elastickv-filesystem-test {:nodes ["n1"] :mount-dirs ["/tmp/a"] :local true
                                                             :concurrency 5})))
  (is (thrown? clojure.lang.ExceptionInfo
               (workload/elastickv-filesystem-test {:nodes ["n1"] :mount-dirs ["/tmp/a"] :local true
                                                    :concurrency 8 :threads-per-key 3 :key-count 1})))
  (is (= 9 (:concurrency (workload/elastickv-filesystem-test {:nodes ["n1"] :mount-dirs ["/tmp/a"] :local true
                                                               :concurrency 9 :threads-per-key 3})))))

(deftest cli-default-concurrency-matches-threads-per-key
  (let [{:keys [options errors]} (tools.cli/parse-opts [] workload/cli-opts)]
    (is (empty? errors))
    (is (= workload/default-concurrency (:concurrency options)))
    (is (zero? (mod (:concurrency options) (:threads-per-key options))))
    (is (= 1 (count (filter #(= "--concurrency N" (second %)) workload/cli-opts))))))

(deftest final-read-ops-cover-every-register
  (let [ops (workload/final-read-ops 3)]
    (is (= 3 (count ops)))
    (is (every? #(= :read (:f %)) ops))
    (is (every? :final ops))
    (is (= [0 1 2] (map (comp first :value) ops)))
    (is (every? #(nil? (second (:value %))) ops))))

(defn- check-final-reads [history]
  (checker/check (workload/final-reads-checker) {} (history/history history) {}))

(deftest final-reads-checker-rejects-lost-registers
  (let [res (check-final-reads [{:process 0 :type :invoke :f :write :value 1}
                                {:process 0 :type :ok :f :write :value 1}
                                {:process 1 :type :invoke :f :read :value nil :final true}
                                {:process 1 :type :ok :f :read :value :missing :final true :error "NoSuchFileException: reg-0"}])]
    (is (false? (:valid? res)))
    (is (= 1 (count (:lost res))))
    (is (= :missing (:value (first (:lost res)))))))

(deftest final-reads-checker-reports-unobserved-final-reads
  (let [res (check-final-reads [{:process 0 :type :invoke :f :read :value nil :final true}
                                {:process 0 :type :fail :f :read :value nil :final true :error "IOException: Input/output error"}])]
    (is (= :unknown (:valid? res)))
    (is (= 1 (count (:unobserved res)))))
  (let [res (check-final-reads [{:process 0 :type :invoke :f :write :value 1}
                                {:process 0 :type :ok :f :write :value 1}])]
    (is (= :unknown (:valid? res)))))

(deftest final-reads-checker-accepts-observed-registers
  (let [res (check-final-reads [{:process 0 :type :invoke :f :read :value nil}
                                {:process 0 :type :fail :f :read :value nil :error "transient"}
                                {:process 1 :type :invoke :f :read :value nil :final true}
                                {:process 1 :type :ok :f :read :value 7 :final true}])]
    (is (true? (:valid? res)))))

(deftest register-model-rejects-data-loss-sentinels
  ;; A sentinel read is a linearizability violation, so the lost write is
  ;; reported by knossos as well as by the final-reads checker.
  (let [check (fn [history]
                (:valid? (checker/check (workload/filesystem-register-checker)
                                        {:name "fs-test" :start-time 0}
                                        (history/history history)
                                        {})))]
    (is (true? (check [{:process 0 :type :invoke :f :write :value 3}
                       {:process 0 :type :ok :f :write :value 3}
                       {:process 1 :type :invoke :f :read :value nil :final true}
                       {:process 1 :type :ok :f :read :value 3 :final true}])))
    (is (false? (check [{:process 0 :type :invoke :f :write :value 3}
                        {:process 0 :type :ok :f :write :value 3}
                        {:process 1 :type :invoke :f :read :value nil :final true}
                        {:process 1 :type :ok :f :read :value :truncated :final true}])))))

(deftest test-spec-generator-ends-with-final-reads
  (let [test-map (workload/elastickv-filesystem-test {:nodes ["n1"] :mount-dirs ["/tmp/a"] :local true :key-count 2})]
    (is (nil? (:final-generator test-map)))
    (is (some? (:generator test-map)))))

(deftest register-checker-races-knossos-analyzers
  (let [linear-checker (get-in (workload/filesystem-register-checker)
                               [:checkers :linear])]
    (is (nil? (:algorithm linear-checker)))))

(deftest remote-db-receives-mount-map
  (let [test-map (workload/elastickv-filesystem-test {:nodes ["n1" "n2"]
                                                      :mount-dir "/mnt/fs"})]
    (is (= {"n1" "/mnt/fs" "n2" "/mnt/fs"}
           (get-in test-map [:db :opts :filesystem-mount])))))

;; ---------------------------------------------------------------------------
;; Remote command rendering
;; ---------------------------------------------------------------------------

(deftest remote-commands-quote-paths-and-keep-record-width
  (let [write (workload/remote-overwrite-cmd "/mnt/it's/jepsen/reg-1" 42)]
    (is (str/includes? write "dd of='/mnt/it'\\''s/jepsen/reg-1' bs=16 conv=notrunc"))
    (is (str/includes? write "printf '%s' '              42'")))
  (let [rename (workload/remote-rename-cmd "/mnt/fs" 1 7)]
    (is (re-find #"^\{ printf '%s' '               7' > '/mnt/fs/jepsen/reg-1\.tmp-[0-9a-f]+' && mv -f '/mnt/fs/jepsen/reg-1\.tmp-[0-9a-f]+' '/mnt/fs/jepsen/reg-1'; \} \|\| \{ rm -f '/mnt/fs/jepsen/reg-1\.tmp-[0-9a-f]+'; exit 1; \}$" rename))
    (is (zero? (:exit (clojure.java.shell/sh "bash" "-n" "-c" rename)))))
  (let [read (workload/remote-read-cmd "/mnt/fs" "/mnt/fs/jepsen/reg-1")]
    (is (zero? (:exit (clojure.java.shell/sh "bash" "-n" "-c" read))))
    (is (str/starts-with? read "{ mountpoint -q '/mnt/fs' && [ -d '/mnt/fs/jepsen' ]; } || { echo 'mount unavailable: /mnt/fs' >&2; exit 1; }; "))
    (is (str/includes? read "s=$(stat -c %s '/mnt/fs/jepsen/reg-1') || exit 1; "))
    (is (str/includes? read "[ \"$s\" -ge 16 ] || { echo 'short record' >&2; exit 1; }; "))
    (is (str/ends-with? read "exec head -c 16 '/mnt/fs/jepsen/reg-1'")))
  (let [setup (workload/remote-setup-cmd "/mnt/fs" [0 1])]
    (is (str/starts-with? setup "mkdir -p '/mnt/fs/jepsen'"))
    (is (str/includes? setup "> '/mnt/fs/jepsen/reg-0'"))
    (is (str/includes? setup "> '/mnt/fs/jepsen/reg-1'"))))

;; ---------------------------------------------------------------------------
;; Local client against a scratch directory
;; ---------------------------------------------------------------------------

(defn- local-test-map [mode dir]
  (workload/elastickv-filesystem-test {:nodes      ["n1"]
                                       :mount-dirs [dir]
                                       :local      true
                                       :write-mode mode
                                       :key-count  2
                                       :require-fuse-mount false}))

(deftest local-setup-rejects-non-fuse-directories-by-default
  (let [dir (scratch-dir)]
    (try
      (let [test-map (workload/elastickv-filesystem-test {:nodes ["n1"] :mount-dirs [dir] :local true :key-count 1})
            c        (client/open! (:client test-map) test-map "n1")]
        (is (true? (:require-fuse-mount (:client test-map))))
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"not a FUSE mount"
                              (client/setup! c test-map)))
        (is (not (workload/local-fuse-mount? dir)))
        (is (not (workload/local-fuse-mount? (str dir "/missing")))))
      (finally
        (delete-tree! dir)))))

(deftest local-client-overwrite-round-trips
  (let [dir (scratch-dir)]
    (try
      (let [test-map (local-test-map :overwrite dir)
            c        (client/open! (:client test-map) test-map "n1")]
        (client/setup! c test-map)
        (is (= workload/initial-value (workload/local-read (workload/register-path dir 0))))
        (let [w (client/invoke! c test-map {:type :invoke :f :write :value (independent/tuple 0 5)})]
          (is (= :ok (:type w))))
        (let [r (client/invoke! c test-map {:type :invoke :f :read :value (independent/tuple 0 nil)})]
          (is (= :ok (:type r)))
          (is (= (independent/tuple 0 5) (:value r))))
        (let [r (client/invoke! c test-map {:type :invoke :f :read :value (independent/tuple 1 nil)})]
          (is (= (independent/tuple 1 workload/initial-value) (:value r))))
        (is (= workload/record-width (.length (io/file (workload/register-path dir 0)))))
        (client/close! c test-map))
      (finally
        (delete-tree! dir)))))

(deftest local-client-rename-round-trips-without-leftover-temp-files
  (let [dir (scratch-dir)]
    (try
      (let [test-map (local-test-map :rename dir)
            c        (client/open! (:client test-map) test-map "n1")]
        (client/setup! c test-map)
        (let [w (client/invoke! c test-map {:type :invoke :f :write :value (independent/tuple 1 9)})]
          (is (= :ok (:type w))))
        (let [r (client/invoke! c test-map {:type :invoke :f :read :value (independent/tuple 1 nil)})]
          (is (= (independent/tuple 1 9) (:value r))))
        (is (= ["reg-0" "reg-1"]
               (sort (map #(.getName %) (.listFiles (io/file (workload/workload-dir dir))))))))
      (finally
        (delete-tree! dir)))))

(deftest local-client-setup-is-idempotent-and-resets-values
  (let [dir (scratch-dir)]
    (try
      (let [test-map (local-test-map :overwrite dir)
            c        (client/open! (:client test-map) test-map "n1")]
        (client/setup! c test-map)
        (client/invoke! c test-map {:type :invoke :f :write :value (independent/tuple 0 3)})
        (client/setup! c test-map)
        (is (= workload/initial-value (workload/local-read (workload/register-path dir 0)))))
      (finally
        (delete-tree! dir)))))

(deftest local-client-ambiguous-write-is-indeterminate
  (let [dir (scratch-dir)]
    (try
      (let [test-map (workload/elastickv-filesystem-test {:nodes ["n1"] :mount-dirs [dir]
                                                          :local true :key-count 1 :max-retries 2 :require-fuse-mount false})
            c        (client/open! (:client test-map) test-map "n1")]
        (client/setup! c test-map)
        (with-redefs [workload/local-overwrite! (fn [_ _] (throw (java.io.IOException. "Input/output error")))]
          (let [w (client/invoke! c test-map {:type :invoke :f :write :value (independent/tuple 0 1)})]
            (is (= :info (:type w)))
            (is (str/includes? (:error w) "Input/output error"))))
        (with-redefs [workload/local-read (fn [_] (throw (java.io.IOException. "Resource temporarily unavailable")))]
          (let [r (client/invoke! c test-map {:type :invoke :f :read :value (independent/tuple 0 nil)})]
            (is (= :fail (:type r))))))
      (finally
        (delete-tree! dir)))))

(deftest an-ambiguous-write-cannot-replay-over-a-newer-write
  (doseq [mode [:overwrite :rename]]
    (let [dir (scratch-dir)]
      (try
        (workload/local-setup! dir [0])
        (let [path (workload/register-path dir 0)
              calls (atom 0)
              observed (atom [])
              overwrite! workload/local-overwrite!
              rename! workload/local-rename-write!
              apply! (fn [v]
                       (case mode
                         :overwrite (overwrite! path v)
                         :rename (rename! dir 0 v)))
              attempt! (fn [v]
                         (swap! calls inc)
                         (apply! v)
                         (when (= 1 @calls)
                           (swap! observed conj (workload/local-read path))
                           (apply! 29)
                           (swap! observed conj (workload/local-read path))
                           (throw (java.io.IOException. "Resource temporarily unavailable"))))]
          (with-redefs [workload/local-overwrite! (fn [_ v] (attempt! v))
                        workload/local-rename-write! (fn [_ _ v] (attempt! v))]
            (let [result (workload/invoke-write!
                           {:local? true :mount-dir dir :write-mode mode :max-retries 3}
                           {} "n1" {:type :invoke :f :write :value (independent/tuple 0 24)} 0 24)]
              (is (= [24 29] @observed))
              (is (= :info (:type result)) (str mode))
              (is (= 1 @calls) (str mode " must not replay a possibly applied write"))
              (is (= 29 (workload/local-read path)) (str mode " must retain the newer write")))))
        (finally
          (delete-tree! dir))))))

(deftest remote-ambiguous-writes-are-not-replayed
  (doseq [mode [:overwrite :rename]]
    (let [calls (atom 0)]
      (with-redefs-fn
        {#'workload/remote-exec! (fn [& _]
                                  (swap! calls inc)
                                  (throw (ex-info "SSH command failed"
                                                  {:exit 1 :err "Input/output error"})))}
        (fn []
          (let [result (workload/invoke-write!
                         {:local? false :mount-dir "/mnt/fs" :write-mode mode :max-retries 3}
                         {} "n1" {:type :invoke :f :write :value (independent/tuple 0 24)} 0 24)]
            (is (= :info (:type result)))
            (is (= 1 @calls))))))))

(deftest writes-retry-only-pre-mutation-errors
  (let [dir (scratch-dir)]
    (try
      (let [test-map (workload/elastickv-filesystem-test {:nodes ["n1"] :mount-dirs [dir]
                                                          :local true :key-count 1 :max-retries 3
                                                          :require-fuse-mount false})
            c        (client/open! (:client test-map) test-map "n1")
            write    (fn [] (client/invoke! c test-map {:type :invoke :f :write :value (independent/tuple 0 1)}))
            eagain   (fn [] (java.io.FileNotFoundException. "reg-0 (Resource temporarily unavailable)"))]
        (client/setup! c test-map)
        ;; Every attempt fails at open: nothing reached the backend -> :fail.
        (with-redefs [workload/local-overwrite! (fn [_ _] (throw (eagain)))]
          (is (= :fail (:type (write)))))
        ;; A write/close EIO may follow application: never try it again.
        (let [calls (atom 0)]
          (with-redefs [workload/local-overwrite! (fn [_ _]
                                                    (if (= 1 (swap! calls inc))
                                                      (throw (java.io.IOException. "Input/output error"))
                                                      (throw (eagain))))]
            (let [w (write)]
              (is (= :info (:type w)))
              (is (= 1 @calls))
              (is (str/includes? (:error w) "Input/output error")))))
        ;; Retrying an open failure remains safe because it had no effect.
        (let [calls (atom 0)]
          (with-redefs [workload/local-overwrite! (fn [_ _]
                                                    (when (< (swap! calls inc) 3)
                                                      (throw (eagain))))]
            (is (= :ok (:type (write))))
            (is (= 3 @calls))))
        ;; A genuinely missing file is still a definite failure.
        (with-redefs [workload/local-overwrite! (fn [_ _] (throw (java.nio.file.NoSuchFileException. "reg-0")))]
          (is (= :fail (:type (write))))))
      (finally
        (delete-tree! dir)))))

(deftest pre-mutation-errors-are-open-failures-only
  (is (workload/pre-mutation-error? (java.nio.file.NoSuchFileException. "x")))
  (is (workload/pre-mutation-error? (java.io.FileNotFoundException. "x (Resource temporarily unavailable)")))
  (is (workload/pre-mutation-error? (java.nio.file.FileAlreadyExistsException. "x")))
  (is (not (workload/pre-mutation-error? (java.io.IOException. "Input/output error"))))
  (is (not (workload/pre-mutation-error? (RuntimeException. "ssh failed")))))

(deftest local-client-maps-missing-file-errors
  (let [dir (scratch-dir)]
    (try
      (let [test-map (local-test-map :overwrite dir)
            c        (client/open! (:client test-map) test-map "n1")
            w        (client/invoke! c test-map {:type :invoke :f :write :value (independent/tuple 0 1)})
            r        (client/invoke! c test-map {:type :invoke :f :read :value (independent/tuple 0 nil)})]
        (is (= :fail (:type w)))
        (is (str/includes? (:error w) "NoSuchFileException"))
        (is (= :fail (:type r)))
        ;; No workload directory yet: the ENOENT is an unavailable mount, not
        ;; a lost register.
        (is (str/includes? (:error r) "mount unavailable")))
      (finally
        (delete-tree! dir)))))

(deftest persistent-read-errors-surface-as-data-loss-sentinels-in-final-reads
  (let [dir (scratch-dir)]
    (try
      (let [test-map (workload/elastickv-filesystem-test {:nodes ["n1"] :mount-dirs [dir]
                                                          :local true :key-count 2 :max-retries 3
                                                          :require-fuse-mount false})
            c        (client/open! (:client test-map) test-map "n1")
            read     (fn [k] (client/invoke! c test-map {:type :invoke :f :read :value (independent/tuple k nil) :final true}))
            path     (workload/register-path dir 0)]
        (client/setup! c test-map)
        ;; Deleted register: ENOENT is retried, then reported as :missing.
        (let [calls (atom 0)
              real  workload/local-read]
          (io/delete-file path)
          (with-redefs [workload/local-read (fn [p] (swap! calls inc) (real p))]
            (let [r (read 0)]
              (is (= :ok (:type r)))
              (is (= (independent/tuple 0 :missing) (:value r)))
              (is (str/includes? (:error r) "No such file or directory"))
              (is (= 3 @calls))))
          ;; The same exhaustion during the main phase cannot be told apart
          ;; from the LOOKUP/OPEN race against a concurrent rename: :fail.
          (reset! calls 0)
          (with-redefs [workload/local-read (fn [p] (swap! calls inc) (real p))]
            (let [r (client/invoke! c test-map {:type :invoke :f :read :value (independent/tuple 0 nil)})]
              (is (= :fail (:type r)))
              (is (str/includes? (:error r) "No such file or directory"))
              (is (= 3 @calls)))))
        ;; Truncated register.
        (spit path "7")
        (let [r (read 0)]
          (is (= :ok (:type r)))
          (is (= (independent/tuple 0 :truncated) (:value r))))
        ;; Garbage record.
        (spit path "not-a-number!!!!")
        (let [r (read 0)]
          (is (= :ok (:type r)))
          (is (= (independent/tuple 0 :corrupt) (:value r))))
        ;; A transient ENOENT that heals within the retries reads normally.
        (let [calls (atom 0)
              real  workload/local-read]
          (with-redefs [workload/local-read (fn [p]
                                              (if (= 1 (swap! calls inc))
                                                (throw (java.nio.file.NoSuchFileException. p))
                                                (real p)))]
            (let [r (read 1)]
              (is (= (independent/tuple 1 workload/initial-value) (:value r)))
              (is (= 2 @calls)))))
        ;; A read that could not be performed stays a :fail.
        (with-redefs [workload/local-read (fn [_] (throw (java.io.FileNotFoundException. (str path " (Transport endpoint is not connected)"))))]
          (is (= :fail (:type (read 0)))))
        ;; Without the workload directory (bare directory after an unmount)
        ;; the ENOENT says nothing about the register.
        (delete-tree! (workload/workload-dir dir))
        (let [r (read 0)]
          (is (= :fail (:type r)))
          (is (str/includes? (:error r) "mount unavailable"))))
      (finally
        (delete-tree! dir)))))

(deftest local-mount-availability-requires-workload-dir
  (let [dir (scratch-dir)]
    (try
      (is (not (workload/local-mount-available? dir false)))
      (.mkdirs (io/file (workload/workload-dir dir)))
      (is (workload/local-mount-available? dir false))
      (is (not (workload/local-mount-available? dir true)))
      (finally
        (delete-tree! dir)))))

(defn- leftover-temp-files [dir]
  (filter #(str/includes? (.getName %) ".tmp-")
          (.listFiles (io/file (workload/workload-dir dir)))))

(deftest local-rename-write-cleans-temp-file-when-move-fails
  (let [dir (scratch-dir)]
    (try
      (.mkdirs (io/file (workload/workload-dir dir)))
      ;; A non-empty directory at the destination makes the atomic replace
      ;; fail after the temp file has been written, so the move path runs.
      (let [dest (io/file (workload/register-path dir 0))]
        (.mkdirs dest)
        (spit (io/file dest "child") "x"))
      (is (thrown? java.io.IOException (workload/local-rename-write! dir 0 5)))
      (is (empty? (leftover-temp-files dir)))
      (is (.isDirectory (io/file (workload/register-path dir 0))))
      (finally
        (delete-tree! dir)))))

(deftest teardown-sweeps-leftover-temp-files
  (let [dir (scratch-dir)]
    (try
      (let [test-map (local-test-map :rename dir)
            c        (client/open! (:client test-map) test-map "n1")]
        (client/setup! c test-map)
        (spit (str (workload/workload-dir dir) "/reg-0.tmp-deadbeef") "x")
        (spit (str (workload/workload-dir dir) "/reg-1.tmp-cafe") "y")
        (is (= 2 (count (leftover-temp-files dir))))
        (client/teardown! c test-map)
        (is (empty? (leftover-temp-files dir)))
        (is (= 2 (count (.listFiles (io/file (workload/workload-dir dir)))))))
      (finally
        (delete-tree! dir)))))

(deftest remote-sweep-command-targets-only-temp-files
  (let [cmd (workload/remote-sweep-cmd "/mnt/fs")]
    (is (= "rm -f '/mnt/fs/jepsen'/reg-*.tmp-* 2>/dev/null || true" cmd))
    (is (zero? (:exit (clojure.java.shell/sh "bash" "-n" "-c" cmd))))))

(deftest local-rename-write-cleans-temp-file-when-write-fails
  (let [dir (scratch-dir)]
    (try
      (.mkdirs (io/file (workload/workload-dir dir)))
      (with-redefs [workload/encode-value (fn [_] (throw (java.io.IOException. "Resource temporarily unavailable")))]
        (is (thrown? java.io.IOException (workload/local-rename-write! dir 0 5))))
      (is (empty? (leftover-temp-files dir)))
      (finally
        (delete-tree! dir)))))

(deftest open-rejects-unknown-node
  (let [test-map (workload/elastickv-filesystem-test {:nodes ["n1"] :mount-dirs ["/tmp/a"] :local true})]
    (is (thrown? clojure.lang.ExceptionInfo
                 (client/open! (:client test-map) test-map "n9")))))
