(ns jepsen.mnevis
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [client :as client]
                    [control :as c]
                    [db :as db]
                    [tests :as tests]
                    [nemesis :as nemesis]
                    [generator :as gen]]
            [jepsen.mnevis.client :as mnevis]
            [jepsen.control.util :as cu]
            [knossos.model :as model]
            [jepsen.os.debian :as debian]))

(def dir "/opt/mnevis")

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (let [host (str "http://" node ":8080")]
      (mnevis/connect host)
      (assoc this :conn host)))

  (setup! [this test])

  (invoke! [this test op]
    (case (:f op)
        :read (assoc op :type :ok, :value (mnevis/r conn "foo"))
        :write (do (mnevis/w conn "foo" (:value op))
                   (assoc op :type, :ok))
        :cas (let [[old new] (:value op)]
               (assoc op :type (if (mnevis/cas conn "foo" old new)
                                 :ok
                                 :fail)))))

  (teardown! [this test])

  (close! [_ test]))

(defn node-name
  "Erlang nodename for mnevis_register app"
  [node]
  (str "mnevis_listener@" node))

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
      (info node "installing mnevis")
      (c/su
        (let [url "https://s3-eu-west-1.amazonaws.com/rabbitmq-share/mnevis_register_release-1.tar.gz"]
          (cu/install-archive! url dir))
        (c/cd dir
          (c/exec
            :echo (str "[{mnevis, [{initial_nodes, [" (initial-cluster test) "]}]}].")
            :| :tee  "releases/1/sys.config")
          (c/exec "bin/mnevis_register_release" "start"))
        (Thread/sleep 10000)))

    (teardown! [_ test node]
      (info node "tearing down mnevis")
      (c/cd dir
        (c/exec "bin/mnevis_register_release" "stop"))
      (c/su (c/exec :rm :-rf dir)))
    db/LogFiles
    (log-files [_ test node]
      [(str dir "/log/erlang.log.1")])))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn mnevis-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         opts
         {:name "mnevis"
          :os   debian/os
          :db   (db "nope")
          :client (Client. nil)
          :nemesis    (nemesis/partition-random-halves)
          :model      (model/cas-register)
          :checker    (checker/linearizable)
          :generator (->> (gen/mix [r w cas])
                          (gen/stagger 1)
                          (gen/nemesis
                            (gen/seq (cycle [(gen/sleep 5)
                                             {:type :info, :f :start}
                                             (gen/sleep 5)
                                             {:type :info, :f :stop}])))
                          (gen/time-limit 30))}))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (cli/single-test-cmd {:test-fn mnevis-test})
            args))