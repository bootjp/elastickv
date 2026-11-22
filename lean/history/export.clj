;; Convert the line-oriented Jepsen EDN using the repository's Clojure dependencies.
(require '[clojure.data.json :as json] '[clojure.edn :as edn] '[clojure.java.io :as io])
(with-open [reader (io/reader *in*)]
  (doseq [line (line-seq reader)]
    (println (json/write-str (edn/read-string {:readers {'elastickv.db.ElastickvDB identity}} line)))))
