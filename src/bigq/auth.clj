(ns bigq.auth
  (:import [java.time Instant])
  (:require [clojure.string :as str]
            [clj-http.client :as http]
            [buddy.core.keys :as keys]
            [buddy.sign.jwt :as jwt]

            [bigq.utils :as utils]))


(defn read-path
  "Pass something slurpable: a path, or a file, or a resource"
  [path]
  (-> path
      slurp
      utils/decode-json))


(defn create-claim [account scopes]
  (let [issued-at  (-> (Instant/now) .getEpochSecond)
        expires-at (+ issued-at 10)]
    {:iss   account
     :scope (str/join " " scopes)
     :aud   "https://www.googleapis.com/oauth2/v3/token"
     :exp   expires-at
     :iat   issued-at}))


(defn create-jwt [data scopes]
  (let [claim (create-claim (:client_email data) scopes)
        key   (keys/str->private-key (:private_key data))
        pair  (jwt/sign claim key {:alg :rs256})]
    pair))


(defn *jwt->token! [jwt]
  (http/post "https://www.googleapis.com/oauth2/v3/token"
    {:form-params
     {:assertion jwt
      :grant_type "urn:ietf:params:oauth:grant-type:jwt-bearer"}}))


(defn jwt->token! [jwt]
  (-> (*jwt->token! jwt)
      :body
      utils/decode-json))


(defn path->token
  "Pass something slurpable and list of scopes from:

  https://developers.google.com/identity/protocols/googlescopes"
  [path scopes]
  (let [data (read-path path)]
    (-> data
        (create-jwt scopes)
        (jwt->token!)
        (assoc :project_id (:project_id data)))))
