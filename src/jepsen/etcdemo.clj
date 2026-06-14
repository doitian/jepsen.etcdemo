(ns jepsen.etcdemo
  (:require
   [clojure.tools.logging :refer :all]
   [clojure.string :as str]
   [jepsen
    [cli :as cli]
    [control :as c]
    [db :as db]
    [tests :as tests]]
   [jepsen.control.util :as cu]
   [jepsen.os.debian :as debian]))

(def dir "/opt/etcd")

(defn db
  "Etcd DB for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (info node "installing etcd" version)
      (debian/install ["ca-certificates"])
      (c/su
       (let [url (str "https://storage.googleapis.com/etcd/" version
                      "/etcd-" version "-linux-amd64.tar.gz")]
         (cu/install-archive! url dir))))

    (teardown! [_ test node]
      (info node "tearing down etcd"))))

(defn etcd-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         opts
         {:name "etcd"
          :os debian/os
          :db (db "v3.1.5")
          :pure-generators true}))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn etcd-test}) (cli/serve-cmd)) args))
