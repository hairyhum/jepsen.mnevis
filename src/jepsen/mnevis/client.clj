(ns jepsen.mnevis.client
  (:require [clj-http.client :as http-client])
  (:use [slingshot.slingshot :only [try+]]))

(defn connect
  "Ping a host"
  [host]
  (try+
    (http-client/get (str host "/nokey"))
    (catch [:status 404] _ nil)))

(defn r
  "Read the key from mnevis_register host"
  [host key]
  (try+
    (:val (:body (http-client/get (str host "/" key) {:accept :json :as :json :throw-exceptions false})))
    (catch [:status 404] _ nil)))

(defn w
  "Write a value for key into mnevis_register host"
  [host key val]
  (do (http-client/post (str host "/" key) {:form-params {:val val}
                                            :content-type :json})
      true))

(defn cas
  "Compare-and-set operation on mnevis_register host"
  [host, key, oldval, newval]
  (try+
    (do (http-client/put (str host "/" key) {:form-params {:old_val oldval :new_val newval}
                                             :content-type :json})
        true)
    (catch [:status 400] _ nil)))