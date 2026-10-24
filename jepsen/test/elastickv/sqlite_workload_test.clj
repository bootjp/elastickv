(ns elastickv.sqlite-workload-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer :all]
            [elastickv.sqlite-workload :as workload]
            [jepsen.client :as client]
            [jepsen.independent :as independent])
  (:import (java.nio.file Files)
           (java.nio.file.attribute FileAttribute)))

(deftest sqlite-client-writes-and-reads-real-database
  (let [dir (str (Files/createTempDirectory "elastickv-sqlite" (make-array FileAttribute 0)))
        test (workload/elastickv-sqlite-test
               {:local true :nodes ["n1"] :node->mount-dir {"n1" dir}
                :concurrency 1 :key-count 1 :require-fuse-mount false})
        c (client/open! (:client test) test "n1")]
    (try
      (client/setup! c test)
      (is (= -1 (workload/read-value (workload/database-path dir 0))))
      (is (= :ok (:type (client/invoke! c test
                         {:f :write :value (independent/tuple 0 42)}))))
      (is (= (independent/tuple 0 42)
             (:value (client/invoke! c test
                       {:f :read :value (independent/tuple 0 nil)}))))
      (is (= "SQLite format 3\u0000" (let [header (byte-array 16)]
                                          (with-open [in (io/input-stream (workload/database-path dir 0))]
                                            (.read in header))
                                          (String. header 0 16 "US-ASCII"))))
      (finally
        (doseq [f (reverse (file-seq (io/file dir)))]
          (io/delete-file f true))))))

(deftest sqlite-final-read-detects-missing-database
  (let [dir (str (Files/createTempDirectory "elastickv-sqlite" (make-array FileAttribute 0)))
        test (workload/elastickv-sqlite-test
               {:local true :nodes ["n1"] :node->mount-dir {"n1" dir}
                :concurrency 1 :key-count 1 :require-fuse-mount false})
        c (client/open! (:client test) test "n1")]
    (try
      (client/setup! c test)
      (io/delete-file (workload/database-path dir 0))
      (is (= (independent/tuple 0 :missing)
             (:value (client/invoke! c test
                       {:f :read :value (independent/tuple 0 nil) :final true}))))
      (finally
        (doseq [f (reverse (file-seq (io/file dir)))]
          (io/delete-file f true))))))

(deftest sqlite-final-read-distinguishes-corruption-from-mount-failure
  (let [dir (str (Files/createTempDirectory "elastickv-sqlite" (make-array FileAttribute 0)))
        test (workload/elastickv-sqlite-test
               {:local true :nodes ["n1"] :node->mount-dir {"n1" dir}
                :concurrency 1 :key-count 1 :require-fuse-mount false})
        c (client/open! (:client test) test "n1")
        op {:f :read :value (independent/tuple 0 nil) :final true}]
    (try
      (client/setup! c test)
      (spit (workload/database-path dir 0) "not a SQLite database")
      (is (= (independent/tuple 0 :corrupt)
             (:value (client/invoke! c test op))))
      (with-redefs [workload/require-sqlite-mount!
                   (fn [_ _] (throw (ex-info "mount unavailable" {})))]
        (is (= :fail (:type (client/invoke! c test op)))))
      (finally
        (doseq [f (reverse (file-seq (io/file dir)))]
          (io/delete-file f true))))))
