(ns bigq.core
  (:require [clojure.string :as str]

            [cheshire.core :as json]
            [clj-http.client :as http]
            [taoensso.timbre :as log]

            [bigq.auth :as auth]))


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
                 {:method method
                  :url    url
                  :content-type :json
                  :accept       :json
                  :headers      {"Authorization" (str "Bearer " (:access_token token))}}

               (= method :post)
               (assoc :form-params params)

               (= method :get)
               (assoc :query-params params))

        req (http/request data)]

    (json/parse-string (:body req) true)))


(def job-id #(-> % :jobReference :jobId))
(def job-complete? :jobComplete)


(defn job->data [job]
  (let [fields (->> (-> job :schema :fields)
                    (mapv #(keyword (:name %))))
        rows   (->> (:rows job)
                    (map #(map :v (:f %))))]
    (map #(into {} (map vector fields %)) rows)))


(defn query [auth-path query]
  "Returns a query result, given a path to authentication data (json with
   private key and stuff Google gives you after making a service account) and a
   query itself.

   Query should be in format `{:query \"query-string\"}`, and you can supply
   other options, see documentation here:

   https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query"

  (let [token (auth/path->token auth-path)
        job   (make-req token :post
                (url-queries (:project_id auth-data))
                (merge
                  {:useLegacySql false
                   :timeoutMs    10}
                  query))
        id    (job-id job)]

    (loop [job job]
      (log/debug "loop" job)

      (if (job-complete? job)
        (job->data job)

        (do
          (Thread/sleep 1000)
          (recur (make-req token :get
                   (url-results (:project_id auth-data) id)
                   {:timeoutMs 10})))))))
