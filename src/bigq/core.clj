(ns bigq.core
  (:import [java.time Instant])
  (:require [clojure.string :as str]

            [bigq.auth :as auth]
            [bigq.request :as req]))


(def bq-queries "https://www.googleapis.com/bigquery/v2/projects/%s/queries")
(def bq-results "https://www.googleapis.com/bigquery/v2/projects/%s/queries/%s")


(defn field->pair [field {:keys [v]}]
  [(keyword (:name field))
   (when-not (and (nil? v) (= "NULLABLE" (:mode field)))
     (case (:type field)
       "STRING"    v
       "BYTES"     v
       "FLOAT"     (Double/parseDouble v)
       "FLOAT64"   (Double/parseDouble v)
       "INTEGER"   (Long/parseLong v 10)
       "INT64"     (Long/parseLong v 10)
       "BOOLEAN"   (= v "TRUE")
       "BOOL"      (= v "TRUE")
       "TIMESTAMP" v
       v))])


(defn extract-data [job]
  (let [fields (-> job :schema :fields)]
    (->> (:rows job)
         (map #(into {} (map field->pair fields (:f %)))))))


(defn param [-name -type -value]
  {:name           -name
   :parameterType  {:type (str/upper-case (name -type))}
   :parameterValue {:value -value}})


(defn query [auth-path opts]
  "Returns a query result, given a path to authentication data (json with
   private key and stuff Google gives you after making a service account) and a
   query itself.

   `opts` should be in format `{:query \"query-string\"}`, and you can supply
   other options, see documentation here:

   https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query#request-body"

  (let [token (auth/path->token auth-path)
        data  (req/job {:url-start   bq-queries
                        :url-results bq-results
                        :project-id  (:project_id token)
                        :token       token
                        :data (merge
                                {:useLegacySql false
                                 :timeoutMs    10}
                                opts)})]

    (extract-data data)))
