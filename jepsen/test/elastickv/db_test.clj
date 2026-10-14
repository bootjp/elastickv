(ns elastickv.db-test
  (:require [clojure.java.shell :as sh]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [elastickv.db :as db]))

(def ^:private awkward-dir "/mnt/elastickv test's dir")

(defn- bash-parses? [script]
  (zero? (:exit (sh/sh "bash" "-n" "-c" script))))

(deftest unmount-command-quotes-the-mount-path
  (let [cmd (#'db/unmount-filesystem-cmd awkward-dir)]
    (is (bash-parses? cmd))
    (is (str/includes? cmd "mountpoint -q '/mnt/elastickv test'\\''s dir'"))
    (is (str/includes? cmd "fusermount3 -uz '/mnt/elastickv test'\\''s dir'"))
    (is (str/includes? cmd "umount -l '/mnt/elastickv test'\\''s dir'"))
    (is (not (str/includes? cmd " /mnt/elastickv test's dir ")))))

(deftest wait-for-mount-command-quotes-and-times-out
  (let [cmd (#'db/wait-for-mount-cmd awkward-dir)]
    (is (bash-parses? cmd))
    (is (str/includes? cmd "mountpoint -q '/mnt/elastickv test'\\''s dir'"))
    (is (str/includes? cmd "exit 1"))))

(deftest wait-for-mount-command-succeeds-on-a-real-mountpoint
  (let [cmd (#'db/wait-for-mount-cmd "/")]
    (is (zero? (:exit (sh/sh "bash" "-c" cmd))))))

(deftest filesystem-mount-dir-resolution
  (is (nil? (#'db/filesystem-mount-dir nil "n1")))
  (is (= "/mnt/fs" (#'db/filesystem-mount-dir "/mnt/fs" "n1")))
  (is (= "/b" (#'db/filesystem-mount-dir {"n1" "/a" "n2" "/b"} "n2")))
  (is (nil? (#'db/filesystem-mount-dir {"n1" "/a"} "n9"))))
