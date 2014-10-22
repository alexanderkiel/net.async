(ns user
  (:import [java.nio.channels SelectionKey])
  (:require [clojure.core.async :as async :refer (<!! <! >! go go-loop close!)]
            [clojure.tools.logging :as logging]
            [net.async.tcp :refer :all]
            [criterium.core :refer (bench)]
            [net.async.nio :as nio]))

(defn echo-server []
  (let [acceptor (accept (event-loop) {:port 8899})]
    (loop []
      (when-let [server (<!! (:accept-chan acceptor))]
        (logging/info "connect from:" server)
        (go-loop []
          (when-let [msg (<! (:read-chan server))]
            (when (keyword? msg)
              (logging/info (name msg)))

            (when-not (keyword? msg)
              (logging/info "received msg:" (String. msg))
              (>! (:write-chan server) (.getBytes (str "ECHO/" (String. msg)))))

            (when-not (= :closed msg)
              (recur))))
        (logging/info "disconnect of:" server)
        (recur)))))

(defn echo-client
  ([] echo-client {:host "127.0.0.1" :port 8899})
  ([dest]
   (let [{:keys [read-chan write-chan] :as client} (connect (event-loop) dest)]
     (go-loop [write? true]
       (when write?
         (let [msg (str (rand-int 100000))]
           (logging/info "send" msg)
           (>! write-chan (.getBytes msg))))

       (let [msg (<! read-chan)]
         (if (keyword? msg)
           (logging/info (name msg))
           (logging/info "received" (String. msg)))

         (if (= :closed msg)
           (logging/info "closed")
           (do (Thread/sleep (rand-int 3000))
               (recur (not (keyword? msg)))))))
     client)))

(comment

  (echo-server)
  (def client (echo-client {:host "127.0.0.1" :port 8898}))
  (close! (:write-chan client))

  (bench (select-opts-1 {:state :connected :read-bufs []}))

  (compile 'user)

  )
