(ns bigq.auth
  (:import [java.time Instant])
  (:require [[clojure.string :as str]

             [cheshire.core :as json]
             [clj-http.client :as http]
             [buddy.core.keys :as keys]
             [buddy.sign.jwt :as jwt]]))


(defn read [path]
  (-> path
      slurp
      (json/parse-string true)))


(defn create-claim [account scopes]
  (let [issued-at  (-> (Instant/now) .getEpochSecond)
        expires-at (+ issued-at 10)]
    {:iss   account
     :scope (str/join " " scopes)
     :aud   "https://www.googleapis.com/oauth2/v3/token"
     :exp   expires-at
     :iat   issued-at}))


(defn create-jwt [data]
  (let [claim (create-claim (:client_email data)
                ["https://www.googleapis.com/auth/bigquery"])
        key   (keys/str->private-key (:private_key data))
        pair  (jwt/sign claim key {:alg :rs256})]
    pair))


(defn *jwt->token [jwt]
  (http/post "https://www.googleapis.com/oauth2/v3/token"
    {:form-params
     {:assertion jwt
      :grant_type "urn:ietf:params:oauth:grant-type:jwt-bearer"}}))


(defn jwt->token [jwt]
  (-> (*jwt->token jwt)
      :body
      (json/parse-string true)))


(defn path->token [path]
  (-> (read auth-path)
      create-jwt
      jwt->token))
