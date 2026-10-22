(ns elastickv.db-test
  (:require [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [elastickv.db :as db]
            [jepsen.control :as c]
            [jepsen.control.util :as cu])
  (:import (java.nio.file Files)
           (java.nio.file.attribute FileAttribute)))

(def ^:private awkward-dir "/mnt/elastickv test's dir")

(deftest migration-nodes-explicitly-allow-unauthenticated-test-helpers
  (doseq [enabled? [false true]]
    (let [started (atom nil)]
      (with-redefs [c/session (constantly :test-session)
                    c/disconnect (constantly nil)
                    c/exec (constantly nil)
                    cu/start-daemon! (fn [opts _bin & args]
                                       (reset! started {:opts opts :args (set args)}))]
        (#'db/start-node! {:nodes ["n1"]} "n1"
                         {:bootstrap-node "n1" :grpc-port 50051 :redis-port 6379
                          :data-dir "/tmp/test" :migration-enabled enabled?}))
      (is (= enabled? (contains? (:args @started) "--adminInsecureNoAuth")))
      (doseq [opcode ["IMPORT" "PROMOTE" "CLEANUP"]]
        (is (= (when enabled? "true")
               (get-in @started [:opts :env (str "ELASTICKV_ENABLE_MIGRATION_" opcode "_OPCODE")])))))))

(defn- bash-parses? [script]
  (zero? (:exit (sh/sh "bash" "-n" "-c" script))))

(deftest unmount-command-quotes-the-mount-path
  (let [cmd (#'db/unmount-filesystem-cmd awkward-dir)]
    (is (bash-parses? cmd))
    (is (str/includes? cmd "< '/proc/mounts'"))
    (is (str/includes? cmd "= '/mnt/elastickv\\040test'\\''s\\040dir'"))
    (is (str/includes? cmd "fusermount3 -uz '/mnt/elastickv test'\\''s dir'"))
    (is (str/includes? cmd "umount -l '/mnt/elastickv test'\\''s dir'"))
    (is (not (str/includes? cmd " /mnt/elastickv test's dir ")))))

(deftest proc-mounts-escape-matches-kernel-encoding
  (is (= "/mnt/plain" (#'db/proc-mounts-escape "/mnt/plain")))
  (is (= "/mnt/a\\040b\\011c\\012d\\134e" (#'db/proc-mounts-escape "/mnt/a b\tc\nd\\e"))))

(defn- with-fake-unmount-tools
  "Runs the unmount snippet for mount-dir against a synthetic mounts table
   with fusermount3/fusermount/umount replaced by scripts that log their
   arguments and exit with the given statuses. Returns the log lines, the
   snippet's stderr and its exit code."
  [mount-dir mounts-lines statuses]
  (let [dir  (Files/createTempDirectory "elastickv-db-test" (make-array FileAttribute 0))
        bin  (str dir "/bin")
        log  (str dir "/calls.log")
        tab  (str dir "/mounts")]
    (try
      (.mkdirs (io/file bin))
      (doseq [[tool status] statuses]
        (let [f (io/file bin tool)]
          (spit f (str "#!/bin/sh\nprintf '%s' \"" tool "\" >> '" log "'; for a in \"$@\"; do printf ' %s' \"$a\" >> '" log "'; done; printf '\\n' >> '" log "'; exit " status "\n"))
          (.setExecutable f true)))
      (spit tab (str (str/join "\n" mounts-lines) "\n"))
      (let [cmd (#'db/unmount-filesystem-cmd mount-dir tab)
            res (sh/sh "env" (str "PATH=" bin ":" (System/getenv "PATH")) "bash" "-c" cmd)]
        {:calls (if (.exists (io/file log)) (str/split-lines (slurp log)) [])
         :err   (:err res)
         :exit  (:exit res)})
      (finally
        (doseq [f (reverse (file-seq (io/file (str dir))))]
          (io/delete-file f true))))))

(def ^:private fuse-mounts-table
  ["elastickv /mnt/elastickv\\040test's\\040dir fuse.elastickv rw,nosuid,nodev,relatime,user_id=0,group_id=0 0 0"
   "/dev/sda1 /data ext4 rw,relatime 0 0"
   "tmpfs /run tmpfs rw 0 0"])

(deftest unmount-command-detaches-a-fuse-mount
  (let [{:keys [calls err exit]} (with-fake-unmount-tools awkward-dir fuse-mounts-table
                                   {"fusermount3" 0 "fusermount" 0 "umount" 0})]
    (is (zero? exit))
    (is (str/blank? err))
    (is (= ["fusermount3 -uz /mnt/elastickv test's dir"] calls))))

(deftest unmount-command-falls-back-to-lazy-umount-for-fuse-only
  (let [{:keys [calls exit]} (with-fake-unmount-tools awkward-dir fuse-mounts-table
                               {"fusermount3" 1 "fusermount" 1 "umount" 0})]
    (is (zero? exit))
    (is (= ["fusermount3 -uz /mnt/elastickv test's dir"
            "fusermount -uz /mnt/elastickv test's dir"
            "umount -l /mnt/elastickv test's dir"] calls))))

(deftest unmount-command-refuses-non-fuse-filesystems
  (let [{:keys [calls err exit]} (with-fake-unmount-tools "/data" fuse-mounts-table
                                   {"fusermount3" 1 "fusermount" 1 "umount" 0})]
    (is (zero? exit))
    (is (empty? calls))
    (is (str/includes? err "refusing to unmount non-FUSE filesystem (ext4) at /data"))))

(deftest unmount-command-ignores-unmounted-directories
  (let [{:keys [calls err exit]} (with-fake-unmount-tools "/mnt/nothing" fuse-mounts-table
                                   {"fusermount3" 0 "fusermount" 0 "umount" 0})]
    (is (zero? exit))
    (is (empty? calls))
    (is (str/blank? err))))

(deftest unmount-command-uses-the-topmost-mount
  ;; A FUSE mount stacked over a data volume at the same path: only the FUSE
  ;; layer (the last table entry) is detached.
  (let [{:keys [calls err]} (with-fake-unmount-tools "/data"
                              (conj fuse-mounts-table "elastickv /data fuse.elastickv rw 0 0")
                              {"fusermount3" 0 "fusermount" 0 "umount" 0})]
    (is (str/blank? err))
    (is (= ["fusermount3 -uz /data"] calls))))

(deftest wait-for-mount-command-quotes-and-times-out
  (let [cmd (#'db/wait-for-mount-cmd awkward-dir)]
    (is (bash-parses? cmd))
    (is (str/includes? cmd "mountpoint -q '/mnt/elastickv test'\\''s dir'"))
    (is (str/includes? cmd "exit 1"))))

(defn- mountpoint-available? []
  (zero? (:exit (sh/sh "bash" "-c" "command -v mountpoint >/dev/null"))))

(deftest wait-for-mount-command-succeeds-on-a-real-mountpoint
  (if (mountpoint-available?)
    (let [cmd (#'db/wait-for-mount-cmd "/")]
      (is (zero? (:exit (sh/sh "bash" "-c" cmd)))))
    (println "skipping wait-for-mount-command-succeeds-on-a-real-mountpoint: mountpoint not available")))

(deftest filesystem-mount-dir-resolution
  (is (nil? (#'db/filesystem-mount-dir nil "n1")))
  (is (= "/mnt/fs" (#'db/filesystem-mount-dir "/mnt/fs" "n1")))
  (is (= "/b" (#'db/filesystem-mount-dir {"n1" "/a" "n2" "/b"} "n2")))
  (is (nil? (#'db/filesystem-mount-dir {"n1" "/a"} "n9"))))
