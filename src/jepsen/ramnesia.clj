(ns jepsen.ramnesia
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [client :as client]
                    [control :as c]
                    [db :as db]
                    [tests :as tests]
                    [generator :as gen]]
            [jepsen.ramnesia.client :as ramnesia]
            [jepsen.control.util :as cu]
            [knossos.model :as model]
            [jepsen.os.debian :as debian]))

(def dir "/opt/ramnesia")

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (let [host (str "http://" node ":8080")]
      (ramnesia/connect host)
      (assoc this :conn host)))

  (setup! [this test])

  (invoke! [this test op]
    (case (:f op)
        :read (assoc op :type :ok, :value (ramnesia/r conn "foo"))
        :write (do (ramnesia/w conn "foo" (:value op))
                   (assoc op :type, :ok))
        :cas (let [[old new] (:value op)]
               (assoc op :type (if (ramnesia/cas conn "foo" old new)
                                 :ok
                                 :fail)))))

  (teardown! [this test])

  (close! [_ test]))

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
            ">" "releases/1/sys.config")
        (c/cd dir
          (c/exec "bin/ramnesia_register_release" "start"))
        (Thread/sleep 10000)))

    (teardown! [_ test node]
      (info node "tearing down ramnesia")
      (c/cd dir
        (c/exec "bin/ramnesia_register_release" "stop"))
      (c/su (c/exec :rm :-rf dir)))))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn ramnesia-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         opts
         {:name "ramnesia"
          :os   debian/os
          :db   (db "nope")
          :client (Client. nil)
          :model      (model/cas-register)
          :checker    (checker/linearizable)
          :generator  (->> (gen/mix [r w cas])
                           (gen/stagger 1)
                           (gen/nemesis nil)
                           (gen/time-limit 15))}))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (cli/single-test-cmd {:test-fn ramnesia-test})
            args))