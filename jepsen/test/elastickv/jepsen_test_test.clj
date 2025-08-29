(ns elastickv.jepsen-test-test
  (:require [clojure.test :refer :all]
            [elastickv.jepsen-test :as jt]))

(deftest builds-test-spec
  (is (map? (jt/elastickv-test))))
