(ns bigq.core
  (:import [java.time Instant])
  (:require [clojure.string :as str]
            [clj-http.client :as http]
            [taoensso.timbre :as log]

            [bigq.auth :as auth]
            [bigq.utils :as utils]))


(defn url-queries [project-id]
  (assert project-id)
  (format "https://www.googleapis.com/bigquery/v2/projects/%s/queries"
    project-id))


(defn url-results [project-id job-id]
  (assert (and project-id job-id))
  (format "https://www.googleapis.com/bigquery/v2/projects/%s/queries/%s"
    project-id job-id))


(defn- make-req [token method url params]
  (log/debug "making request to" url)
  (let [data (cond->
                 {:method       method
                  :url          url
                  :content-type :json
                  :accept       :json
                  :headers      {"Authorization" (str "Bearer " (:access_token token))}}

               (= method :post)
               (assoc :form-params params)

               (= method :get)
               (assoc :query-params params))

        req (http/request data)]

    (utils/decode-json (:body req))))


(def job-id #(-> % :jobReference :jobId))
(def job-complete? :jobComplete)


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


(defn job->data [job]
  (let [fields (-> job :schema :fields)]
    (->> (:rows job)
         (map #(into {} (map field->pair fields (:f %)))))))


(defn query [auth-path query]
  "Returns a query result, given a path to authentication data (json with
   private key and stuff Google gives you after making a service account) and a
   query itself.

   Query should be in format `{:query \"query-string\"}`, and you can supply
   other options, see documentation here:

   https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query"

  (let [auth-data (auth/read-path auth-path)
        jwt       (auth/create-jwt auth-data)
        token     (auth/jwt->token! jwt)
        job       (make-req token :post
                    (url-queries (:project_id auth-data))
                    (merge
                      {:useLegacySql false
                       :timeoutMs    10}
                      query))
        id        (job-id job)]

    (loop [job job]
      (log/debug "loop" job)

      (if (job-complete? job)
        (job->data job)

        (do
          (Thread/sleep 1000)
          (recur (make-req token :get
                   (url-results (:project_id auth-data) id)
                   {:timeoutMs 10})))))))
