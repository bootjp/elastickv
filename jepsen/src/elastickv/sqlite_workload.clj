(ns elastickv.sqlite-workload
  "SQLite register transactions on the replicated FUSE filesystem. Each key
   owns a database and one worker; final reads check every mounted replica.
   SQLite file locks are local to a mount, so this workload does not share a
   database between concurrent writers on different mounts."
  (:require [clojure.java.shell :as shell]
            [clojure.string :as str]
            [elastickv.cli :as cli]
            [elastickv.filesystem-workload :as fs]
            [jepsen.client :as client]
            [jepsen.independent :as independent])
  (:import (java.nio.file Files LinkOption Paths)
           (java.nio.file.attribute FileAttribute)))

(def sqlite-subdir "jepsen-sqlite")
(def ^:const max-retries 32)

(defn retryable-sqlite-error? [e]
  (let [msg (.getMessage e)]
    (boolean (some #(str/includes? msg %)
                   ["disk I/O error" "unable to open database"
                    "readonly database" "database is locked"
                    "Resource temporarily unavailable" "SQLite database missing"]))))

(defn with-sqlite-retries [f]
  (fs/with-retries max-retries retryable-sqlite-error? f))

(defn database-path [mount-dir k]
  (str mount-dir "/" sqlite-subdir "/reg-" k ".db"))

(defn sqlite!
  "Run one sqlite3 invocation. A failed write has an unknown outcome because
   COMMIT may have succeeded before the CLI or FUSE reported an error."
  [path sql]
  (let [{:keys [exit out err]} (shell/sh "sqlite3" "-batch" "-noheader"
                                         "-cmd" ".timeout 1000" path sql)]
    (when-not (zero? exit)
      (throw (ex-info (str "sqlite3 exited " exit ": " (str/trim err))
                      {:exit exit :out out :err err})))
    (str/trim out)))

(defn setup-databases! [mount-dir key-count]
  (let [dir (Paths/get (str mount-dir "/" sqlite-subdir) (make-array String 0))]
    (Files/createDirectories dir (make-array FileAttribute 0))
    (doseq [k (range key-count)]
      (let [path (database-path mount-dir k)]
        (with-sqlite-retries #(sqlite! path (str "PRAGMA journal_mode=PERSIST; "
                           "PRAGMA synchronous=FULL; "
                           "CREATE TABLE IF NOT EXISTS register "
                           "(id INTEGER PRIMARY KEY CHECK(id=1), value INTEGER NOT NULL); "
                           "INSERT OR REPLACE INTO register VALUES (1, -1);")))))))

(defn write-value! [path v]
  ;; One worker owns each database. Retrying this idempotent value after an
  ;; ambiguous I/O error cannot overwrite a newer operation on that key.
  (with-sqlite-retries
    #(sqlite! path (str "PRAGMA synchronous=FULL; BEGIN IMMEDIATE; "
                        "UPDATE register SET value=" (long v)
                        " WHERE id=1; COMMIT;"))))

(defn read-value [path]
  ;; sqlite3 opens a missing path by creating a new empty database. Detect
  ;; loss before invoking it so a final read never repairs the evidence.
  (when-not (Files/exists (Paths/get path (make-array String 0))
                          (make-array LinkOption 0))
    (throw (ex-info "SQLite database missing" {:path path})))
  (let [out (sqlite! path "SELECT value FROM register WHERE id=1;")]
    (when (str/blank? out)
      (throw (ex-info "SQLite register row missing" {:path path})))
    (Long/parseLong out)))

(defn require-sqlite-mount! [mount-dir require-fuse-mount]
  (when (or (and require-fuse-mount (not (fs/local-fuse-mount? mount-dir)))
            (not (Files/isDirectory
                   (Paths/get (str mount-dir "/" sqlite-subdir) (make-array String 0))
                   (make-array LinkOption 0))))
    (throw (ex-info (str fs/mount-unavailable-message ": " mount-dir) {}))))

(defrecord SQLiteClient [node->mount-dir key-count require-fuse-mount initialized node mount-dir]
  client/Client
  (open! [this _test node]
    (assoc this :node node :mount-dir (or (get node->mount-dir node)
                                         (throw (ex-info "no mount directory" {:node node})))))
  (setup! [_this _test]
    (when require-fuse-mount (fs/require-local-fuse-mount! mount-dir))
    (locking initialized
      (when-not @initialized
        (setup-databases! mount-dir key-count)
        (reset! initialized true))))
  (teardown! [_this _test])
  (close! [this _test] (assoc this :node nil :mount-dir nil))
  (invoke! [_this _test op]
    (let [[k v] (:value op)
          path (database-path mount-dir k)]
      (case (:f op)
        :write (try
                 (write-value! path v)
                 (assoc op :type :ok)
                 (catch Exception e
                   (assoc op :type :info :error (.getMessage e))))
        :read (try
                (require-sqlite-mount! mount-dir require-fuse-mount)
                (assoc op :type :ok :value
                       (independent/tuple k (with-sqlite-retries #(read-value path))))
                (catch Exception e
                  (if (and (:final op)
                           (or (not (Files/exists (Paths/get path (make-array String 0))
                                                  (make-array LinkOption 0)))
                               (str/includes? (.getMessage e) "row missing")))
                    (assoc op :type :ok :value (independent/tuple k :missing)
                           :error (.getMessage e))
                    (assoc op :type :fail :error (.getMessage e)))))))))

(defn elastickv-sqlite-test
  ([] (elastickv-sqlite-test {}))
  ([opts]
   (when-not (:local opts)
     (throw (ex-info "SQLite workload requires --local FUSE mounts" {})))
   (let [opts (assoc opts :threads-per-key 1)
         base (fs/elastickv-filesystem-test opts)
         key-count (or (:key-count opts) 10)]
     (assoc base
            :name "elastickv-sqlite-register"
            :client (->SQLiteClient (:node->mount-dir base) key-count
                                    (if (contains? opts :require-fuse-mount)
                                      (:require-fuse-mount opts) true)
                                    (atom false) nil nil)))))

(def sqlite-cli-opts
  (vec (remove #(= "--write-mode MODE" (second %)) fs/cli-opts)))

(defn prepare-sqlite-opts [options]
  (let [mount-dirs (:mount-dirs options)
        options (cli/parse-common-opts options mount-dirs)]
    (assoc options
           :node->mount-dir (fs/node->mount-dir (:nodes options) mount-dirs (:mount-dir options))
           :require-fuse-mount (not (:allow-non-fuse-mount options))
           :threads-per-key 1)))

(defn -main [& args]
  (cli/run-workload! args sqlite-cli-opts prepare-sqlite-opts elastickv-sqlite-test))
