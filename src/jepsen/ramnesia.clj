(ns jepsen.ramnesia
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [cli :as cli]
                    [control :as c]
                    [db :as db]
                    [tests :as tests]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))

(defn node-name
  "Erlang nodename for ramnesia_register app"
  [node]
  (str "ramnesia_listener@" node))

(defn initial-cluster
  "Constructs an initial nodes list for a test"
  [test]
  (->> (:nodes test)
       (map (fn [node]
              (node-name node)))
       (str/join ",")))

(defn db
  "Ramnesia DB."
  [_]
  (reify db/DB
    (setup! [_ test node]
      (info node "installing ramnesia")
      (c/su
        (let [url "https://s3-eu-west-1.amazonaws.com/rabbitmq-share/ramnesia_register_release-1.tar.gz"]
          (cu/install-archive! url dir))
        (c/exec
          :echo (str "[{ramnesia, [{initial_nodes, [" (initial-cluster test) "]}]}].")
            :> "releases/1/sys.config")
        (c/exec "bin/ramnesia_register_release" "start")))

    (teardown! [_ test node]
      (info node "tearing down ramnesia")
      (c/exec "bin/ramnesia_register_release" "stop"))))

(defn ramnesia-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         opts
         {:name "ramnesia"
          :os   debian/os
          :db   (db "nope")}))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (cli/single-test-cmd {:test-fn ramnesia-test})
            args))