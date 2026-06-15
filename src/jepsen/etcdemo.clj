(ns jepsen.etcdemo
  (:require
   [clojure.tools.logging :refer :all]
   [clojure.string :as str]
   [verschlimmbesserung.core :as v]
   [slingshot.slingshot :refer [try+]]
   [jepsen
    [checker :as checker]
    [cli :as cli]
    [client :as client]
    [control :as c]
    [db :as db]
    [generator :as gen]
    [nemesis :as nemesis]
    [tests :as tests]]
   [jepsen.control.util :as cu]
   [jepsen.control.retry :as retry]
   [jepsen.control.sshj :as sshj]
   [jepsen.os.debian :as debian]
   [jepsen.checker.timeline :as timeline]
   [knossos.model :as model]))

(def dir "/opt/etcd")
(def binary "etcd")
(def logfile (str dir "/etcd.log"))
(def pidfile (str dir "/etcd.pid"))

(defn parse-long-nil
  "Parses a string to a Long. Passes through `nil`."
  [s]
  (when s (parse-long s)))

(def sftp-remote
  "Like jepsen.control's default sshj remote, but without the shell-out SCP
  wrapper. The SCP wrapper invokes the system `scp` binary, which can't use the
  test's :ssh password and prompts interactively when no key is present. sshj's
  own SFTP up/download honors username+password auth, so log-file downloads work
  with root:root."
  (-> (sshj/->SSHJRemote sshj/concurrency-limit nil nil nil)
      retry/remote))

(defn node-url
  "An HTTP URL for connecting to a node on a particular port."
  [node port]
  (str "http://" node ":" port))

(defn peer-url
  "The HTTP URL for other peers to talk to a node."
  [node]
  (node-url node 2380))

(defn client-url
  "The HTTP URL clients use to talk to a node."
  [node]
  (node-url node 2379))

(defn initial-cluster
  "Constructs an initial cluster string for a test, like
  \"foo=foo:2380,bar=bar:2380,...\""
  [test]
  (->> (:nodes test)
       (map (fn [node] (str node "=" (peer-url node))))
       (str/join ",")))

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
         (cu/install-archive! url dir))

       (cu/start-daemon!
        {:logfile logfile
         :pidfile pidfile
         :chdir dir}
        binary
        :--log-output :stderr
        :--name (name node)
        :--listen-peer-urls (peer-url node)
        :--listen-client-urls (client-url node)
        :--advertise-client-urls (client-url node)
        :--initial-cluster-state :new
        :--initial-advertise-peer-urls (peer-url node)
        :--initial-cluster (initial-cluster test))

       (Thread/sleep 10000)))

    (teardown! [_ test node]
      (info node "tearing down etcd")

      (cu/stop-daemon! binary pidfile)
      (c/su (c/exec :rm :-rf dir)))

    db/LogFiles
    (log-files [_ test node]
      [logfile])))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defrecord Client [conn]
  client/Client
  (open! [this test node] (assoc this :conn (v/connect (client-url node) {:timeout 5000})))
  (setup! [this test])
  (invoke! [this test op]
    (try+
     (case (:f op)
       :read (assoc op
                    :type :ok,
                    :value (-> conn
                               (v/get "foo" {:quorum? true})
                               parse-long-nil))
       :write (do (v/reset! conn "foo" (:value op))
                  (assoc op :type :ok))
       :cas (let [[old new] (:value op)]
              (assoc op :type (if (v/cas! conn "foo" old new) :ok :fail))))

     (catch java.net.SocketTimeoutException ex
       (assoc op :type (if (= :read (:f op)) :fail :info) :error :timeout))
     (catch [:errorCode 100] ex
       (assoc op :type :fail, :error :not-found))))

  (teardown! [this test])
  (close! [_ test]))

(defn etcd-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         opts
         {:name "etcd"
          :os debian/os
          :db (db "v3.1.5")
          :client (Client. nil)
          :nemesis (nemesis/partition-random-halves)
          :checker (checker/compose
                    {:perf (checker/perf)
                     :linear (checker/linearizable
                              {:model (model/cas-register)
                               :algorithm :linear})
                     :timeline (timeline/html)})
          :generator (->> (gen/mix [r w cas])
                          (gen/stagger 1)
                          (gen/nemesis (cycle [(gen/sleep 5)
                                               {:type :info, :f :start}
                                               (gen/sleep 5)
                                               {:type :info, :f :stop}]))
                          (gen/time-limit (:time-limit opts)))
          :remote sftp-remote
          :pure-generators true}))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn etcd-test}) (cli/serve-cmd)) args))
