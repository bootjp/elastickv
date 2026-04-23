(ns elastickv.jepsen-test
  (:gen-class)
  (:require [elastickv.redis-workload :as redis-workload]
            [elastickv.redis-zset-safety-workload :as zset-safety-workload]
            [elastickv.dynamodb-workload :as dynamodb-workload]
            [elastickv.s3-workload :as s3-workload]
            [jepsen.cli :as cli]))

(defn elastickv-test []
  (redis-workload/elastickv-redis-test {}))

(defn elastickv-dynamodb-test []
  (dynamodb-workload/elastickv-dynamodb-test {}))

(defn elastickv-s3-test []
  (s3-workload/elastickv-s3-test {}))

(defn elastickv-zset-safety-test []
  (zset-safety-workload/elastickv-zset-safety-test {}))

(def ^:private test-fns
  "Map of user-facing test names to their constructor fns. The first
  positional CLI arg selects which workload runs; if absent or unknown,
  we default to `elastickv-test` for backward compatibility with
  pre-existing invocations."
  {"elastickv-test"             elastickv-test
   "elastickv-zset-safety-test" elastickv-zset-safety-test
   "elastickv-dynamodb-test"    elastickv-dynamodb-test
   "elastickv-s3-test"          elastickv-s3-test})

(defn -main
  "Dispatch to a named workload. Usage:

    lein run -m elastickv.jepsen-test <test-name> [jepsen-subcmd] [jepsen-opts ...]

  Supported <test-name>s: elastickv-test, elastickv-zset-safety-test,
  elastickv-dynamodb-test, elastickv-s3-test. When the first positional
  arg is not a known test name, we default to `elastickv-test` for
  backward compatibility and forward ALL args to jepsen.cli/run!.

  The jepsen subcommand (`test` or `analyze`) is auto-prepended when
  missing, so `lein run elastickv-zset-safety-test --nodes n1,n2` works
  without the user repeating `test`."
  [& args]
  (let [[head & tail] args
        [selected-fn remaining-args] (if-let [f (get test-fns head)]
                                       [f tail]
                                       [elastickv-test args])
        ;; jepsen.cli/run! requires a subcommand ("test" or "analyze")
        ;; as the first arg. Insert "test" only when the user clearly
        ;; did NOT supply a subcommand:
        ;;   - remaining-args is empty, OR
        ;;   - the first token is an option (starts with "-")
        ;; If the first token looks like a subcommand (any non-option
        ;; word, e.g. "test", "analyze", "serve", or a future jepsen.cli
        ;; subcommand we don't hard-code), leave it alone and let
        ;; jepsen.cli/run! handle it (including producing a better
        ;; error message for unknown subcommands than we could here).
        [next-head & _] remaining-args
        prepend-test? (or (empty? remaining-args)
                          (and (string? next-head)
                               (.startsWith ^String next-head "-")))
        final-args (if prepend-test?
                     (cons "test" remaining-args)
                     remaining-args)]
    (cli/run! (cli/single-test-cmd {:test-fn selected-fn})
              final-args)))
