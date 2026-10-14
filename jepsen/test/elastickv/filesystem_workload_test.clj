(ns elastickv.filesystem-workload-test
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [jepsen.client :as client]
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

(deftest final-read-ops-cover-every-register
  (let [ops (workload/final-read-ops 3)]
    (is (= 3 (count ops)))
    (is (every? #(= :read (:f %)) ops))
    (is (= [0 1 2] (map (comp first :value) ops)))
    (is (every? #(nil? (second (:value %))) ops))))

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
  (is (= "[ \"$(stat -c %s '/mnt/fs/jepsen/reg-1')\" -ge 16 ] && head -c 16 '/mnt/fs/jepsen/reg-1'"
         (workload/remote-read-cmd "/mnt/fs/jepsen/reg-1")))
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

(deftest local-client-write-retries-exhausted-is-indeterminate
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

(deftest write-classification-uses-every-attempt
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
        ;; First attempt fails after open (write/close EIO), later attempts
        ;; fail at open: the first one may have committed -> :info.
        (let [calls (atom 0)]
          (with-redefs [workload/local-overwrite! (fn [_ _]
                                                    (if (= 1 (swap! calls inc))
                                                      (throw (java.io.IOException. "Input/output error"))
                                                      (throw (eagain))))]
            (let [w (write)]
              (is (= :info (:type w)))
              (is (= 3 @calls))
              (is (str/includes? (:error w) "Resource temporarily unavailable")))))
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
        (is (str/includes? (:error r) "FileNotFoundException")))
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
