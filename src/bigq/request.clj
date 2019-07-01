(ns bigq.request
  (:require [clj-http.client :as http]
            [taoensso.timbre :as log]

            [bigq.utils :as utils]))


(def job-id #(-> % :jobReference :jobId))
(def job-complete? :jobComplete)


(defn fmt-url-start
  "Returns url to a starting point of a job.

  `tmpl` is like `https://www.googleapis.com/bigquery/v2/projects/%s/queries`"
  [tmpl project-id]
  (assert project-id)
  (format tmpl project-id))


(defn fmt-url-results
  "Returns url to results of a job.

  `tmpl` is like `https://www.googleapis.com/bigquery/v2/projects/%s/queries/%s`"
  [tmpl project-id job-id]
  (assert (and project-id job-id))
  (format tmpl project-id job-id))


(defn make-req [token method url params]
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


(defn job [{:keys [url-start url-results project-id token data sleep]
            :or   {sleep 1000}}]
  (let [url-s (fmt-url-start url-start project-id)
        job   (make-req token :post url-s data)
        id    (job-id job)
        url-r (fmt-url-results url-results project-id id)]
    (loop [job job]
      (log/debug "loop" job)

      (if (job-complete? job)
        job
        (do
          (Thread/sleep sleep)
          (recur (make-req token :get url-r {:timeoutMs 10})))))))
