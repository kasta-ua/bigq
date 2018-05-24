(ns bigq.utils
  (:require [jsonista.core :as json]))


(def json-mapper (json/object-mapper {:decode-key-fn true}))


(defn decode-json [v]
  (json/read-value v json-mapper))

