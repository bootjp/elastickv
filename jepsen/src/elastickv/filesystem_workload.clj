(ns elastickv.filesystem-workload
  "Jepsen workload for the Elastickv FUSE filesystem.

   Every node mounts the same replicated filesystem. Each independent key is
   a regular file holding one fixed-width integer, modelled as a linearizable
   register:

   - :write stores the value either in place (`--write-mode overwrite`, a
     single pwrite of the whole 16-byte record at offset 0, which the backend
     applies as one chunk read-modify-write transaction) or by writing a
     temporary file and renaming it over the key (`--write-mode rename`, which
     exercises the namespace transaction and remove-while-open semantics).
   - :read opens the file and preads the record.

   Files are pre-created in setup! with the initial register value so the
   history never observes a size-0 intermediate state.

   The backend is optimistically concurrent: open/write/close on a file that
   another mount is writing can fail with EAGAIN (a write conflict), and a
   follower mount that lost its forwarded proposal may report EIO. Both are
   retried a bounded number of times (`--max-retries`); a write that exhausts
   its retries is recorded as indeterminate because an attempt may already
   have been committed. Reads additionally retry ENOENT, short and malformed
   records: FUSE splits open(2) into LOOKUP + OPEN, so a reader racing a
   rename on another mount can briefly miss the replaced inode. A main-phase
   read that exhausts those retries is still a plain :fail, because under
   write contention a bounded retry window cannot tell that race from loss.
   The final reads run once every writer has stopped, so there the same
   exhaustion is evidence: a final read that cannot observe its register is
   not a :fail (knossos treats a failed read as never having happened, which
   would let a lost acknowledged write pass) but completes :ok with a
   data-loss sentinel (:missing, :truncated or :corrupt) that the register
   model rejects, and a dedicated checker flags a final read that observed
   nothing at all. A read against a bare or dangling mount (the node was
   killed) fails without being counted as loss.

   In `--local` mode the Jepsen process talks to the mount points directly
   through java.nio (one mount directory per node). Without `--local` the
   client runs `dd`/`head`/`mv` on each node over SSH against the node's
   own mount, and the DB adapter starts every node with `--filesystemMount`."
  (:gen-class)
  (:require [clojure.string :as str]
            [elastickv.cli :as cli]
            [elastickv.db :as ekdb]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [independent :as independent]
                    [net :as net]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control :as control]
            [jepsen.db :as jdb]
            [jepsen.nemesis :as nemesis]
            [jepsen.nemesis.combined :as combined]
            [jepsen.os :as os]
            [jepsen.os.debian :as debian]
            [knossos.model :as model])
  (:import (java.io EOFException FileNotFoundException IOException RandomAccessFile)
           (java.nio.charset StandardCharsets)
           (java.nio.file AtomicMoveNotSupportedException
                          FileAlreadyExistsException
                          Files
                          LinkOption
                          NoSuchFileException
                          Path
                          Paths
                          StandardCopyOption)
           (java.util.concurrent ThreadLocalRandom)))

;; ---------------------------------------------------------------------------
;; Constants
;; ---------------------------------------------------------------------------

(def default-nodes ["n1" "n2" "n3" "n4" "n5"])

(def default-mount-dir
  "Mount point used on every node when --mount-dirs is not given."
  "/mnt/elastickv")

(def ^:const record-width
  "Every register file holds exactly this many bytes, so an in-place write is
   a single chunk read-modify-write and a read never observes a short file."
  16)

(def ^:const initial-value
  "Value every register starts with. Writes draw from (range), so -1 never
   collides with a generated write."
  -1)

(def workload-subdir
  "Directory (relative to the mount point) holding the register files."
  "jepsen")

(def write-modes #{:overwrite :rename})

(def ^:const default-max-retries
  "Attempts per operation before giving up on EAGAIN/EIO."
  8)

(def ^:const retry-backoff-ms 20)

(def retryable-messages
  "Exception messages the kernel produces for the backend's retryable errnos:
   EAGAIN for a write conflict and EIO for a conflict that crossed leader
   forwarding, plus the dd/head phrasing used by the SSH backend."
  ["Resource temporarily unavailable"
   "Input/output error"])

(defn error-text
  "Text an error is classified by. A jepsen.control non-zero exit carries the
   command line in its message (which may quote the very markers we look
   for), so for those only stderr and stdout are inspected."
  [^Throwable e]
  (let [data (ex-data e)]
    (if (contains? data :exit)
      (str (:err data) "\n" (:out data))
      (or (.getMessage e) ""))))

(defn retryable-error?
  [^Throwable e]
  (let [msg (error-text e)]
    (boolean (some #(str/includes? msg %) retryable-messages))))

(def ^:const short-record-message "short record")

(def ^:const mount-unavailable-message "mount unavailable")

(def data-loss-kinds
  "Sentinel register values a read completes with when the register itself is
   gone or damaged. None of them equals a written long, so the register model
   rejects the read instead of ignoring it."
  #{:missing :truncated :corrupt})

(defn data-loss-kind
  "Classifies a read error by what it says about the register: :missing for
   ENOENT, :truncated for a record shorter than record-width and :corrupt for
   an unparseable record. nil for everything else (EAGAIN, EIO, ENOTCONN, an
   unavailable mount, SSH failures), which only says the read could not be
   performed. Only a quiescent (final-phase) read may treat a non-nil kind as
   loss; while writers run, ENOENT can be the LOOKUP/OPEN race."
  [^Throwable e]
  (let [msg (error-text e)]
    (cond
      (str/includes? msg mount-unavailable-message)      nil
      (instance? NumberFormatException e)                :corrupt
      (instance? EOFException e)                         :truncated
      (str/includes? msg short-record-message)           :truncated
      (instance? NoSuchFileException e)                  :missing
      (str/includes? msg "No such file or directory")    :missing
      :else                                              nil)))

(defn read-retryable-error?
  "Reads retry the backend's retryable errnos and every data-loss-shaped
   error, so only a persistent loss is reported as one."
  [^Throwable e]
  (or (retryable-error? e)
      (some? (data-loss-kind e))))

(defn with-retries
  "Calls (f) up to max-retries times while it throws an error accepted by
   retry? (retryable-error? by default), sleeping retry-backoff-ms between
   attempts. Rethrows the last error."
  ([max-retries f]
   (with-retries max-retries retryable-error? f))
  ([max-retries retry? f]
   (loop [attempt 1]
     (let [result (try
                    {:value (f)}
                    (catch Exception e
                      (if (and (< attempt max-retries) (retry? e))
                        {:retry e}
                        (throw e))))]
       (if (contains? result :retry)
         (do (Thread/sleep (long retry-backoff-ms))
             (recur (inc attempt)))
         (:value result))))))

;; ---------------------------------------------------------------------------
;; Record encoding
;; ---------------------------------------------------------------------------

(defn encode-value
  "Left-pads v to record-width ASCII bytes so every record has the same size."
  ^bytes [v]
  (let [s (str v)]
    (when (> (count s) record-width)
      (throw (ex-info "value does not fit the register record" {:value v})))
    (.getBytes (str (apply str (repeat (- record-width (count s)) \space)) s)
               StandardCharsets/US_ASCII)))

(defn decode-value
  "Parses a record back into a long. Returns nil for an empty record (a file
   that exists but was never written) and throws on malformed content."
  [^bytes data]
  (let [s (str/trim (String. data StandardCharsets/US_ASCII))]
    (when-not (str/blank? s)
      (Long/parseLong s))))

;; ---------------------------------------------------------------------------
;; Paths
;; ---------------------------------------------------------------------------

(defn workload-dir
  "Directory holding the register files under a mount point."
  [mount-dir]
  (str mount-dir "/" workload-subdir))

(defn register-path
  "Path of the register file for key k under a mount point."
  [mount-dir k]
  (str (workload-dir mount-dir) "/reg-" k))

(defn temp-path
  "Unique temporary path next to the register file, used by rename writes."
  [mount-dir k]
  (str (register-path mount-dir k)
       ".tmp-"
       (Long/toHexString (.nextLong (ThreadLocalRandom/current)))))

(defn node->mount-dir
  "Maps nodes to their mount directories. mount-dirs (per node) wins over the
   single mount-dir applied to every node."
  [nodes mount-dirs mount-dir]
  (if (seq mount-dirs)
    (do (when (not= (count mount-dirs) (count nodes))
          (throw (ex-info "mount-dirs count must match node count"
                          {:nodes nodes :mount-dirs mount-dirs})))
        (zipmap nodes mount-dirs))
    (zipmap nodes (repeat (or mount-dir default-mount-dir)))))

(defn parse-write-mode
  [s]
  (let [mode (keyword (str/lower-case (str s)))]
    (when-not (contains? write-modes mode)
      (throw (IllegalArgumentException.
               (str "Unknown write mode: " s ". Expected one of "
                    (str/join ", " (map name (sort write-modes)))))))
    mode))

;; ---------------------------------------------------------------------------
;; Local (java.io) backend
;;
;; RandomAccessFile is used instead of FileChannel on purpose: FileChannel
;; translates an EAGAIN from the kernel into a 0-byte read or write, which
;; would silently turn a write conflict into a short record or an empty read.
;; RandomAccessFile surfaces it as an IOException ("Resource temporarily
;; unavailable") that the retry loop understands.
;; ---------------------------------------------------------------------------

(defn- ^Path nio-path [^String p]
  (Paths/get p (make-array String 0)))

(defn- require-existing!
  "Throws NoSuchFileException when path does not exist. RandomAccessFile's
   \"rw\" mode would otherwise create the file."
  [^String path]
  (when-not (Files/exists (nio-path path) (make-array LinkOption 0))
    (throw (NoSuchFileException. path))))

(defn- write-record!
  "Writes the whole record at offset 0 of an open file."
  [^RandomAccessFile raf ^bytes data]
  (.seek raf 0)
  (.write raf data))

(defn- read-record
  "Reads exactly record-width bytes at offset 0; throws EOFException when the
   file is shorter than a record."
  ^bytes [^RandomAccessFile raf]
  (let [buf (byte-array record-width)]
    (.seek raf 0)
    (.readFully raf buf)
    buf))

(defn local-overwrite!
  "In-place write: one write of the full record at offset 0, no truncate."
  [path v]
  (require-existing! path)
  (with-open [raf (RandomAccessFile. ^String path "rw")]
    (write-record! raf (encode-value v))))

(def ^:const temp-delete-attempts
  "Unlink attempts for a leftover temporary file; the unlink itself can hit
   the same directory-level write conflicts as the rename it cleans up after."
  3)

(defn- delete-quietly!
  "Best-effort removal of a temporary file; errors are ignored because the
   rename that preceded it may already have consumed the inode."
  [^String path]
  (try
    (with-retries temp-delete-attempts
      (fn [] (Files/deleteIfExists (nio-path path))))
    (catch Exception _)))

(defn temp-file?
  [^String name]
  (str/includes? name ".tmp-"))

(defn local-sweep-temp-files!
  "Removes temporary files left behind by rename writes whose cleanup also
   failed, so a reused workload directory starts clean and does not accrue
   inodes across runs."
  [mount-dir]
  (let [dir (java.io.File. ^String (workload-dir mount-dir))]
    (when (.isDirectory dir)
      (doseq [f (.listFiles dir)
              :when (temp-file? (.getName f))]
        (delete-quietly! (.getPath f))))))

(defn local-rename-write!
  "Atomic replace: write the record to a temporary file, then rename it over
   the register path. Any failure between creating the temporary file and
   renaming it (write, close or rename hitting EAGAIN/EIO) removes the
   temporary file so retries under directory contention do not leak one
   inode per attempt; an ambiguously successful rename has already consumed
   it, which delete-if-exists tolerates."
  [mount-dir k v]
  (let [tmp  (temp-path mount-dir k)
        dest (register-path mount-dir k)]
    (try
      (with-open [raf (RandomAccessFile. ^String tmp "rw")]
        (write-record! raf (encode-value v)))
      (Files/move (nio-path tmp) (nio-path dest)
                  (into-array StandardCopyOption
                              [StandardCopyOption/ATOMIC_MOVE
                               StandardCopyOption/REPLACE_EXISTING]))
      (catch Exception e
        (delete-quietly! tmp)
        (throw e)))))

(defn local-read
  "Reads the register value."
  [path]
  (with-open [raf (RandomAccessFile. ^String path "r")]
    (decode-value (read-record raf))))

(defn local-setup!
  "Creates the workload directory and pre-fills every register file."
  [mount-dir keys]
  (let [dir (nio-path (workload-dir mount-dir))]
    (try
      (Files/createDirectory dir (make-array java.nio.file.attribute.FileAttribute 0))
      (catch FileAlreadyExistsException _))
    (doseq [k keys]
      (with-open [raf (RandomAccessFile. ^String (register-path mount-dir k) "rw")]
        (write-record! raf (encode-value initial-value))))))

(defn local-fuse-mount?
  "True when mount-dir sits on a FUSE file store. Guards --local runs against
   a typo or a vanished mount, which would otherwise exercise the host's
   native filesystem and still report a valid Elastickv run."
  [mount-dir]
  (let [path (nio-path mount-dir)]
    (and (Files/isDirectory path (make-array LinkOption 0))
         (str/starts-with? (str (.type (Files/getFileStore path))) "fuse"))))

(defn require-local-fuse-mount!
  [mount-dir]
  (when-not (local-fuse-mount? mount-dir)
    (throw (ex-info (str "mount directory is not a FUSE mount: " mount-dir
                         " (pass --allow-non-fuse-mount to override)")
                    {:mount-dir mount-dir}))))

(defn local-mount-available?
  "True when reads may interpret an ENOENT as a lost register: the workload
   directory is reachable (a bare directory left by an unmount, or a dangling
   mount, has none) and, unless the FUSE check is disabled, the mount is
   still a FUSE file store."
  [mount-dir require-fuse-mount]
  (and (or (not require-fuse-mount) (local-fuse-mount? mount-dir))
       (Files/isDirectory (nio-path (workload-dir mount-dir)) (make-array LinkOption 0))))

(defn require-local-mount!
  [mount-dir require-fuse-mount]
  (when-not (local-mount-available? mount-dir require-fuse-mount)
    (throw (ex-info (str mount-unavailable-message ": " mount-dir)
                    {:mount-dir mount-dir}))))

;; ---------------------------------------------------------------------------
;; Remote (SSH) backend
;; ---------------------------------------------------------------------------

(defn- sh-quote [s]
  (str "'" (str/replace (str s) "'" "'\\''") "'"))

(defn remote-overwrite-cmd
  "Shell command performing the in-place record write on a node."
  [path v]
  (str "printf '%s' " (sh-quote (String. ^bytes (encode-value v) StandardCharsets/US_ASCII))
       " | dd of=" (sh-quote path) " bs=" record-width " conv=notrunc status=none"))

(defn remote-rename-cmd
  "Shell command performing the write-temp-then-rename replace on a node.
   A failed write or mv removes the temporary file before reporting the
   error."
  [mount-dir k v]
  (let [tmp (sh-quote (temp-path mount-dir k))]
    (str "{ printf '%s' " (sh-quote (String. ^bytes (encode-value v) StandardCharsets/US_ASCII))
         " > " tmp
         " && mv -f " tmp " " (sh-quote (register-path mount-dir k))
         "; } || { rm -f " tmp "; exit 1; }")))

(defn remote-read-cmd
  "Shell command reading one record. It refuses to run against an
   unavailable mount (a killed node leaves a bare or dangling directory, whose
   ENOENT must not pass for a lost register) and fails with a short-record
   message instead of returning a truncated number, mirroring the local
   backend's readFully. stat reports ENOENT on stderr."
  [mount-dir path]
  (let [m (sh-quote mount-dir)
        w (sh-quote (workload-dir mount-dir))
        q (sh-quote path)]
    (str "{ mountpoint -q " m " && [ -d " w " ]; } || { echo "
         (sh-quote (str mount-unavailable-message ": " mount-dir)) " >&2; exit 1; }; "
         "s=$(stat -c %s " q ") || exit 1; "
         "[ \"$s\" -ge " record-width " ] || { echo " (sh-quote short-record-message) " >&2; exit 1; }; "
         "exec head -c " record-width " " q)))

(defn remote-sweep-cmd
  "Shell command removing leftover temporary files from a node's workload
   directory."
  [mount-dir]
  (str "rm -f " (sh-quote (workload-dir mount-dir)) "/reg-*.tmp-* 2>/dev/null || true"))

(defn remote-setup-cmd
  [mount-dir keys]
  (str "mkdir -p " (sh-quote (workload-dir mount-dir))
       (apply str
              (for [k keys]
                (str " && printf '%s' "
                     (sh-quote (String. ^bytes (encode-value initial-value) StandardCharsets/US_ASCII))
                     " > " (sh-quote (register-path mount-dir k)))))))

(defn- remote-exec!
  "Runs a shell command on node over the test's SSH session. elastickv.db
   starts the server as root, so the FUSE mount belongs to root and is only
   reachable through su."
  [test node cmd]
  (control/with-node test node
    (control/su
      (control/exec :sh :-c cmd))))

;; ---------------------------------------------------------------------------
;; Jepsen client
;; ---------------------------------------------------------------------------

(defn- fail
  [op error]
  (assoc op :type :fail :error error))

(defn- indeterminate
  [op error]
  (assoc op :type :info :error error))

(defn- io-error-label [^Throwable e]
  (str (.. e getClass getSimpleName) ": " (.getMessage e)))

(defn pre-mutation-error?
  "True when an attempt failed before any byte could reach the register: the
   register (or temp) file could not be opened or does not exist. Everything
   else (a write, close, rename or SSH error) may have left the write applied."
  [^Throwable e]
  (or (instance? NoSuchFileException e)
      (instance? FileNotFoundException e)
      (instance? FileAlreadyExistsException e)))

(defn invoke-write!
  "Executes a write, retrying retryable errors. The outcome is classified
   over every attempt, not just the last exception: an op is :fail only when
   all attempts failed before touching the backend, because a single earlier
   write/close/rename error may already have been committed and a later
   EAGAIN at open (which RandomAccessFile reports as FileNotFoundException)
   must not downgrade it to a definite failure."
  [{:keys [local? mount-dir write-mode max-retries]} test node op k v]
  (let [maybe-applied? (volatile! false)
        attempt! (fn []
                   (try
                     (if local?
                       (case write-mode
                         :overwrite (local-overwrite! (register-path mount-dir k) v)
                         :rename    (local-rename-write! mount-dir k v))
                       (remote-exec! test node
                                     (case write-mode
                                       :overwrite (remote-overwrite-cmd (register-path mount-dir k) v)
                                       :rename    (remote-rename-cmd mount-dir k v))))
                     (catch Exception e
                       (when-not (pre-mutation-error? e)
                         (vreset! maybe-applied? true))
                       (throw e))))]
    (try
      (with-retries max-retries attempt!)
      (assoc op :type :ok)
      (catch Exception e
        (if (and (not @maybe-applied?) (pre-mutation-error? e))
          (fail op (io-error-label e))
          (indeterminate op (io-error-label e)))))))

(defn invoke-read!
  "Executes a read, retrying retryable and data-loss-shaped errors. Reads
   never mutate state, so an exhausted error is a :fail, with one exception:
   a final-phase read (:final, issued after every writer stopped) whose error
   says the register is gone completes :ok with its data-loss sentinel so the
   checker sees the loss. A main-phase read keeps :fail even then, because a
   bounded retry window cannot distinguish a lost register from the
   LOOKUP/OPEN race against a concurrent rename."
  [{:keys [local? mount-dir max-retries require-fuse-mount]} test node op k]
  (try
    (let [v (with-retries max-retries read-retryable-error?
              (fn []
                (if local?
                  (do (require-local-mount! mount-dir require-fuse-mount)
                      (local-read (register-path mount-dir k)))
                  (decode-value (.getBytes ^String (remote-exec! test node
                                                                   (remote-read-cmd mount-dir (register-path mount-dir k)))
                                           StandardCharsets/US_ASCII)))))]
      (assoc op :type :ok :value (independent/tuple k v)))
    (catch Exception e
      (if-let [kind (and (:final op) (data-loss-kind e))]
        (assoc op :type :ok :value (independent/tuple k kind) :error (io-error-label e))
        (fail op (io-error-label e))))))

(defrecord FilesystemClient [node->mount-dir write-mode key-count max-retries require-fuse-mount node mount-dir local?]
  client/Client
  (open! [this test node]
    (let [dir (or (get node->mount-dir node)
                  (throw (ex-info "no mount directory for node" {:node node})))]
      (assoc this
             :node      node
             :mount-dir dir
             :local?    (boolean (:local test)))))

  (setup! [this test]
    (let [keys (range key-count)]
      (when (and local? require-fuse-mount)
        (require-local-fuse-mount! mount-dir))
      (with-retries max-retries
        (fn []
          (if local?
            (local-setup! mount-dir keys)
            (remote-exec! test node (remote-setup-cmd mount-dir keys)))))))

  (teardown! [_this test]
    ;; Jepsen runs teardown! before setup! and again after the test, so the
    ;; sweep both clears a reused directory and tidies up at the end.
    (try
      (if local?
        (local-sweep-temp-files! mount-dir)
        (remote-exec! test node (remote-sweep-cmd mount-dir)))
      (catch Exception _)))

  (close! [this _test]
    (assoc this :node nil :mount-dir nil))

  (invoke! [this test op]
    (let [[k v] (:value op)]
      (case (:f op)
        :write (invoke-write! this test node op k v)
        :read  (invoke-read! this test node op k)))))

;; ---------------------------------------------------------------------------
;; Workload & Test builders
;; ---------------------------------------------------------------------------

(defn final-read-ops
  "One read per register, run after the main phase (and after the nemesis
   has healed the cluster) so a lost final write is observed even when no
   workload read happened to target that key afterwards."
  [key-count]
  (map (fn [k] {:f :read :value (independent/tuple k nil) :final true})
       (range key-count)))

(defn with-final-reads
  "Appends the recovery and final-read phases to the main generator.
   nemesis-final (the nemesis package's :final-generator) heals partitions and
   restarts killed nodes before the reads; it is nil in --local runs."
  [main-gen nemesis-final key-count]
  (gen/phases main-gen
              (when nemesis-final
                (gen/phases (gen/log "Healing cluster before final reads")
                            (gen/nemesis nemesis-final)
                            (gen/sleep 10)))
              (gen/log "Final reads")
              (gen/clients (final-read-ops key-count))))

(defn final-reads-checker
  "Per-key checker for the final-read phase. Knossos models a failed read as
   never having happened, so a lost register whose only read after the last
   write failed would otherwise pass: the key is invalid when its final read
   observed a data-loss sentinel and :unknown when it could not observe the
   register at all (or never ran)."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [finals     (->> history
                            (filter #(and (:final %) (not= :invoke (:type %))))
                            (map #(into {} %)))
            lost       (filter #(and (= :ok (:type %)) (contains? data-loss-kinds (:value %))) finals)
            unobserved (remove #(= :ok (:type %)) finals)]
        (cond
          (seq lost)       {:valid? false :lost (vec lost)}
          (seq unobserved) {:valid? :unknown :unobserved (vec unobserved)}
          (empty? finals)  {:valid? :unknown :error "no final read in history"}
          :else            {:valid? true})))))

(defn filesystem-register-checker
  "Per-key register checker. Knossos races its linear and WGL analyzers; the
   final-reads checker refuses to let a failed last read hide data loss."
  []
  (checker/compose
    {:linear      (checker/linearizable {:model (model/register initial-value)})
     :final-reads (final-reads-checker)
     :timeline    (timeline/html)}))

(defn register-key-generator
  "Per-key op stream: exactly max-writes writes (values 0..max-writes-1)
   mixed with at most as many reads, so --max-writes-per-key really bounds
   the writes a key receives and the key exhausts (handing its threads to the
   next key) once they are done. A limit over the mixed stream would instead
   cap reads and writes together and leave the write count to chance."
  [max-writes]
  (gen/mix [(map (fn [v] {:f :write :value v}) (range max-writes))
            (gen/limit max-writes (gen/repeat {:f :read}))]))

(defn filesystem-register-workload
  "Builds the linearizable-register workload. Each independent key is one
   file under <mount>/jepsen; threads-per-key concurrent workers hammer each
   active key."
  [opts]
  (let [key-count       (or (:key-count opts) 10)
        max-writes      (or (:max-writes-per-key opts) 100)
        threads-per-key (or (:threads-per-key opts) 2)
        write-mode      (or (:write-mode opts) :overwrite)
        max-retries     (max 1 (or (:max-retries opts) default-max-retries))
        require-fuse    (if (contains? opts :require-fuse-mount) (boolean (:require-fuse-mount opts)) true)
        node->dir       (or (:node->mount-dir opts)
                            (node->mount-dir default-nodes nil default-mount-dir))
        client          (->FilesystemClient node->dir write-mode key-count max-retries require-fuse nil nil false)]
    (when-not (contains? write-modes write-mode)
      (throw (ex-info "unknown write mode" {:write-mode write-mode})))
    {:client    client
     :generator (independent/concurrent-generator
                  threads-per-key
                  (range key-count)
                  (fn [_k] (register-key-generator max-writes)))
     :checker   (independent/checker
                  (filesystem-register-checker))}))

(defn elastickv-filesystem-test
  "Builds a Jepsen test map that drives the Elastickv FUSE filesystem with the
   linearizable register workload."
  ([] (elastickv-filesystem-test {}))
  ([opts]
   (let [nodes       (or (:nodes opts) default-nodes)
         mount-dir   (or (:mount-dir opts) default-mount-dir)
         node->dir   (or (:node->mount-dir opts)
                         (node->mount-dir nodes (:mount-dirs opts) mount-dir))
         local?      (:local opts)
         db          (if local?
                       jdb/noop
                       (ekdb/db {:grpc-port        (or (:grpc-port opts) 50051)
                                 :redis-port       (or (:redis-port opts) 6379)
                                 :filesystem-mount node->dir
                                 :raft-groups      (:raft-groups opts)
                                 :shard-ranges     (:shard-ranges opts)}))
         rate        (double (or (:rate opts) 5))
         time-limit  (or (:time-limit opts) 30)
         faults      (if local?
                       []
                       (cli/normalize-faults (or (:faults opts) [:partition :kill])))
         nemesis-p   (when-not local?
                       (combined/nemesis-package {:db       db
                                                  :faults   faults
                                                  :interval (or (:fault-interval opts) 40)}))
         nemesis-gen (if nemesis-p
                       (:generator nemesis-p)
                       (gen/once {:type :info :f :noop}))
         key-count   (or (:key-count opts) 10)
         workload    (filesystem-register-workload (assoc opts :node->mount-dir node->dir))
         main-gen    (->> (:generator workload)
                          (gen/nemesis nemesis-gen)
                          (gen/stagger (/ rate))
                          (gen/time-limit time-limit))]
     (merge workload
            {:name            (or (:name opts)
                                  (str "elastickv-filesystem-register-"
                                       (name (or (:write-mode opts) :overwrite))))
             :nodes           nodes
             :db              db
             :local           (boolean local?)
             :write-mode      (or (:write-mode opts) :overwrite)
             :node->mount-dir node->dir
             :os              (if local? os/noop debian/os)
             :net             (if local? net/noop net/iptables)
             :ssh             (merge {:username                 "vagrant"
                                      :private-key-path         "/home/vagrant/.ssh/id_rsa"
                                      :strict-host-key-checking false}
                                     (when local? {:dummy true})
                                     (:ssh opts))
             :remote          control/ssh
             :nemesis         (if nemesis-p
                                (:nemesis nemesis-p)
                                nemesis/noop)
             ;; Final reads live inside :generator (gen/phases) rather than
             ;; :final-generator, which Jepsen 0.3.x cannot serialize for
             ;; combined nemesis generators.
             :final-generator nil
             :concurrency     (or (:concurrency opts) 10)
             :generator       (with-final-reads main-gen
                                                (when nemesis-p (:final-generator nemesis-p))
                                                key-count)}))))

;; ---------------------------------------------------------------------------
;; CLI
;; ---------------------------------------------------------------------------

(def filesystem-cli-opts
  "Filesystem-specific CLI options, appended to common opts."
  [[nil "--mount-dirs DIRS" "Comma separated mount directories (one per node, in node order)."
    :default nil
    :parse-fn (fn [s]
                (->> (str/split s #",")
                     (remove str/blank?)
                     vec))]
   [nil "--mount-dir DIR" "Mount directory (applied to all nodes)."
    :default default-mount-dir]
   [nil "--write-mode MODE" "How writes replace a register: overwrite (pwrite in place) or rename (temp file + rename)."
    :default :overwrite
    :parse-fn parse-write-mode]
   [nil "--redis-port PORT" "Redis port."
    :default 6379
    :parse-fn #(Integer/parseInt %)]
   [nil "--threads-per-key N" "Concurrent threads per register file."
    :default 2
    :parse-fn #(Integer/parseInt %)]
   [nil "--max-retries N" "Attempts per operation before an EAGAIN/EIO is reported."
    :default default-max-retries
    :parse-fn #(Integer/parseInt %)]
   [nil "--allow-non-fuse-mount" "Skip the check that every --mount-dirs entry is a FUSE mount (local runs only)."
    :default false]])

(defn- prepare-filesystem-opts
  "Transform parsed CLI options into the map expected by elastickv-filesystem-test."
  [options]
  (let [mount-dirs (:mount-dirs options)
        options    (cli/parse-common-opts options mount-dirs)
        node->dir  (node->mount-dir (:nodes options) mount-dirs (:mount-dir options))]
    (assoc options
      :node->mount-dir node->dir
      :mount-dirs      mount-dirs
      :write-mode      (:write-mode options)
      :redis-port      (:redis-port options)
      :threads-per-key (:threads-per-key options)
      :max-retries     (:max-retries options)
      :require-fuse-mount (not (:allow-non-fuse-mount options)))))

(defn -main
  [& args]
  (cli/run-workload! args
                     (into cli/common-cli-opts filesystem-cli-opts)
                     prepare-filesystem-opts
                     elastickv-filesystem-test))
