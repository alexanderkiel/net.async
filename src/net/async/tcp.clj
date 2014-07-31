(ns net.async.tcp
  (:require
    [clojure.set :as set]
    [clojure.string :as string]
    [clojure.tools.logging :as logging]
    [clojure.core.async :as async :refer [>!! <!! chan close! go <! >! thread timeout alts!]]
    [net.async.nio :as nio])
  (:import
    [java.nio  ByteBuffer]
    [java.io   IOException]
    [java.net  InetSocketAddress StandardSocketOptions ConnectException SocketAddress InetAddress]
    [java.nio.channels ServerSocketChannel SocketChannel]))

(set! *warn-on-reflection* true)

;; UTILITY

(defn inet-addr [{:keys [host port]}]
  (if host
    (InetSocketAddress. (InetAddress/getByName host) ^int port)
    (InetSocketAddress. port)))

(defn buf [size]
  (ByteBuffer/allocate size))

(defn buf-array [& xs]
  (into-array ByteBuffer xs))

(defn in-thread [^String name f & args]
  (doto
    (Thread. #(try
                (apply f args)
                (catch Throwable e
                  (logging/error e "thread died")))
             name)
    (.start)))

(defn now [] (System/currentTimeMillis))

;; SOCKET CALLBACKS

(defn on-read-ready [socket-ref]
  (let [{:keys [read-bufs read-chan read-size-fn]} @socket-ref
        [size-buf body-buf] read-bufs]
    (case (count read-bufs)
      1 (let [size (read-size-fn size-buf)]
          (if (zero? size)
            (nio/rewind! size-buf) ;; heartbeat
            (do
              (logging/trace "Allocate read body buffer of size" size)
              (swap! socket-ref assoc :read-bufs (buf-array size-buf (buf size))))))
      2 (do
          (swap! socket-ref dissoc :read-bufs)
          (logging/trace "Done reading on socket" (:id @socket-ref)
                         "Park until payload is taken from read-chan.")
          (go
            (>! read-chan (nio/array body-buf))
            (logging/trace "Done putting payload into read-chan."
                           "Ready to read from socket again.")
            (nio/rewind! size-buf)
            (swap! socket-ref assoc :read-bufs (buf-array size-buf)))))))

(defn write-bufs! [size-buf payload]
  (when payload
    (if (empty? payload)
      (buf-array (doto size-buf (nio/put-int! 0 0))) ;; heartbeat
      (buf-array (doto size-buf (nio/put-int! 0 (count payload)))
                 (ByteBuffer/wrap payload)))))

(defn fill-write-bufs! [size-buf socket-ref]
  (let [{:keys [write-chan heartbeat-period]} @socket-ref
        timeout (some-> heartbeat-period timeout)]
    (go
      (let [[val port] (alts! (if timeout [write-chan timeout] [write-chan]))]
        (condp = port
          write-chan
          (if val
            (swap! socket-ref assoc :write-bufs (write-bufs! size-buf val))
            (swap! socket-ref assoc :state :closed))

          timeout
          (swap! socket-ref assoc :write-bufs (write-bufs! size-buf [])))))))

(defn on-write-done [socket-ref]
  (logging/trace "Done writing on socket" (:id @socket-ref))
  (let [[size-buf] (:write-bufs @socket-ref)]
    (nio/rewind! size-buf)
    (swap! socket-ref dissoc :write-bufs)
    (fill-write-bufs! size-buf socket-ref)))

(defn alloc-size-buf [socket]
  (let [buf ((:size-buf-alloc-fn socket))]
    (logging/trace "Allocated size buffer of size" (nio/capacity buf)
                   "and order" (nio/order buf))
    buf))

(defn new-socket [opts]
  (let [socket-ref
        (atom (merge {:id         (str "/" (+ 1000 (rand-int 8999)))
                      :read-chan  (chan)
                      :write-chan (chan)
                      :reconnect-period 1000
                      :heartbeat-period 5000
                      :heartbeat-timeout (some-> (:heartbeat-period opts 5000)
                                                 (* 4))
                      :size-buf-alloc-fn #(buf 4)
                      :read-size-fn #(nio/get-int % 0)}
                     opts))]
    (fill-write-bufs! (alloc-size-buf @socket-ref) socket-ref)
    socket-ref))

(defn client-socket [socket-ref]
  (select-keys @socket-ref [:read-chan :write-chan]))

(defn on-connected [socket-ref]
  (swap! socket-ref assoc
         :state :connected
         :read-bufs (buf-array (alloc-size-buf @socket-ref))
         :last-read (atom (now)))
  (async/put! (:read-chan @socket-ref) :connected))

(defn on-accepted [socket-ref net-chan]
  (swap! socket-ref assoc :state :not-accepting)
  (let [opts (merge
               { :net-chan net-chan
                 :read-chan  ((:read-chan-fn @socket-ref))
                 :write-chan ((:write-chan-fn @socket-ref)) }
               (select-keys @socket-ref [:heartbeat-period :heartbeat-timeout]))
        new-socket-ref (new-socket opts)]
    (logging/debug "Accepted new socket" (:id @new-socket-ref))
    (on-connected new-socket-ref)
    (go
      (>! (:accept-chan @socket-ref) (client-socket new-socket-ref))
      (swap! socket-ref assoc :state :accepting))
    new-socket-ref))

(defn on-conn-dropped [socket-ref]
  (let [{:keys [addr read-chan write-bufs reconnect-period]} @socket-ref]
    (swap! socket-ref dissoc :read-bufs)
    (doseq [buf write-bufs] (nio/rewind! buf))
    (if addr
      (do
        (swap! socket-ref assoc :state :disconnected)
        (go
          (>! read-chan :disconnected)
          (<! (timeout reconnect-period))
          (swap! socket-ref assoc
                 :state :connecting)))
      (swap! socket-ref assoc :state :closed))))

(defn on-conn-closed [socket-ref]
  (let [{:keys [read-chan write-chan]} @socket-ref]
    (go
      (>! read-chan :closed)
      (close! read-chan)
      (close! write-chan))))

;; EVENT LOOP

(defn add-watches [{:keys [sockets selector running?]}]
  (add-watch running? :wakeup (fn [_ _ _ _] (nio/wakeup! selector)))
  (add-watch sockets :wakeup
             (fn [_ _ old new]
               (doseq [socket (set/difference old new)]
                 (remove-watch socket :wakeup))
               (doseq [socket (set/difference new old)]
                 (add-watch socket :wakeup (fn [_ _ _ _]
                                             (nio/wakeup! selector))))
               (nio/wakeup! selector))))

(defn select-opts [socket]
  (case (:state socket)
    :accepting nio/op-accept
    :connecting nio/op-connect
    :connected (cond-> 0
                 (:read-bufs socket) (bit-or nio/op-read)
                 (:write-bufs socket) (bit-or nio/op-write))
    0))

(defn exhausted? [bufs]
  (zero? (.remaining ^ByteBuffer (last bufs))))

(defn close-net-chan! [socket-ref]
  (let [socket @socket-ref]
    (when-let [net-chan (:net-chan socket)]
    (try
      (nio/close! net-chan)
      (catch IOException e
        (logging/debug "Error while closing network channel" (:id socket)
                       (.getMessage e))))))
  (swap! socket-ref dissoc :net-chan))

(defn detect-connecting! [sockets]
  (doseq [socket-ref @sockets
          :let [{:keys [state net-chan addr id]} @socket-ref]
          :when (and (= :connecting state) (nil? net-chan))]
    (logging/debug "Connecting socket" id)
    (let [net-chan (doto (SocketChannel/open) (.configureBlocking false))]
      (swap! socket-ref assoc :net-chan net-chan)
      (.connect net-chan addr))))

(defn detect-dead! [sockets]
  (doseq [socket-ref @sockets
          :let [{:keys [state id]} @socket-ref]
          :when (= state :closed)]
    (logging/debug "Deleting socket" id)
    (swap! sockets disj socket-ref)
    (close-net-chan! socket-ref)
    (on-conn-closed socket-ref)))

(defn detect-stuck! [sockets]
  (doseq [socket-ref @sockets
          :let [{:keys [state last-read id heartbeat-timeout]} @socket-ref]]
    (when (and last-read
               heartbeat-timeout
               (= state :connected)
               (< (+ @last-read heartbeat-timeout) (now)))
      (logging/debug "Socket stuck" id)
      (close-net-chan! socket-ref)
      (on-conn-dropped socket-ref))))

(defn register-net-chans! [selector sockets]
  (doseq [socket-ref @sockets
          :let [{:keys [net-chan] :as socket} @socket-ref]
          :when net-chan
          :let [opts (select-opts socket)]
          :when (pos? opts)]
    (nio/register! net-chan selector opts socket-ref)))

(defn event-loop-impl [{:keys [sockets started? running? selector] :as env}]
  (add-watches env)
  (deliver started? true)

  (loop []
    (detect-connecting! sockets)
    (detect-stuck! sockets)
    (detect-dead! sockets)
    (register-net-chans! selector sockets)

    (nio/select! selector 1000)

    (doseq [key (nio/selected-keys selector)
            :let [socket-ref (nio/attachment key)
                  {:keys [net-chan read-bufs write-bufs last-read id addr]} @socket-ref]]
      (try
        (when (and (nio/readable? key) net-chan read-bufs)
          (let [read (nio/read! net-chan read-bufs 0 (count read-bufs))]
            (reset! last-read (now))
            (if (= -1 read)
              (throw (IOException. "End of stream reached."))
              (when (exhausted? read-bufs)
                (on-read-ready socket-ref)))))

        (when (and (nio/writeable? key) net-chan write-bufs)
          (nio/write! net-chan write-bufs 0 (count write-bufs))
          (when (exhausted? write-bufs)
            (on-write-done socket-ref)))

        (when (and (nio/connectable? key) net-chan)
          (nio/finish-connect! net-chan)
          (logging/debug "Connected to" (str addr) "in" id)
          (on-connected socket-ref))

        (when (nio/acceptable? key)
          (let [new-net-chan (nio/accept! net-chan)]
            (nio/set-non-blocking! new-net-chan)
            (swap! sockets conj (on-accepted socket-ref new-net-chan))))

        (catch ConnectException e
          (logging/warn "Cannot connect to" (str addr) "in" id (.getMessage e))
          (close-net-chan! socket-ref)
          (on-conn-dropped socket-ref))

        (catch IOException e
          (logging/warn "I/O error in" id (.getMessage e))
          (close-net-chan! socket-ref)
          (on-conn-dropped socket-ref))))

    (nio/clear-selected-keys! selector)
    (when @running?
      (recur)))
  (doseq [socket-ref @sockets]
    (close-net-chan! socket-ref)))

(defn event-loop []
  (let [selector (nio/open-selector)
        env      { :selector selector
                   :started? (promise)
                   :running? (atom true)
                   :sockets  (atom #{}) }
        thread   (in-thread "net.async.tcp/event-loop" event-loop-impl env)]
    @(:started? env)
    (assoc env :thread thread)))

(defn shutdown! [event-loop]
  (reset! (:running? event-loop) false))

;; CLIENT INTERFACE

(defn connect
  "env                  :: result of (event-loop)
  to                    :: map of {:host <String> :port <int>}
  Possible opts are:
   - :read-chan         :: <chan> to populate with reads from socket, defaults to (chan)
   - :write-chan        :: <chan> to schedule writes to socket, defaults to (chan)
   - :heartbeat-period  :: Heartbeat interval, ms. Defaults to 5000. Set to nil to disable heartbeats.
   - :heartbeat-timeout :: When to consider connection stale. Default value is (* 4 heartbeat-period)
   - :size-buf-alloc-fn :: fn allocating the size buf
   - :read-size-fn      :: fn reading the size from the size-buf"
  [env to & {:as opts}]
  (let [addr (inet-addr to)
        socket-ref (new-socket (merge opts
                                 {:addr  addr
                                  :state :connecting}))]
    (swap! (:sockets env) conj socket-ref)
    (client-socket socket-ref)))

(defn accept
  "env                  :: result of (event-loop)
  at                    :: map of {:host <String> :port <int>}, :host defaults to wildcard address
  Possible opts are:
   - :accept-chan       :: <chan> to populate with accepted client sockets, defaults to (chan)
  Following options will be translated to client sockets created by accept:
   - :read-chan-fn      :: no-args fn returning <chan>, will be used to create :read-chan for accepted sockets
   - :write-chan-fn     :: no-args fn returning <chan>, will be used to create :write-chan for accepted sockets
   - :heartbeat-period  :: Heartbeat interval, ms. Default value is 5000. Set to nil to disable heartbeats.
   - :heartbeat-timeout :: When to consider connection stale. Default value is (* 4 heartbeat-period)"
  [env at & {:as opts}]
  (let [addr (inet-addr at)
        net-chan   (doto (ServerSocketChannel/open)
                         (.setOption StandardSocketOptions/SO_REUSEADDR true)
                         (.bind ^SocketAddress addr)
                         (.configureBlocking false))
        socket-ref (atom (merge
                           { :net-chan      net-chan
                             :state         :accepting
                             :accept-chan   (chan)
                             :read-chan-fn  #(chan)
                             :write-chan-fn #(chan) }
                           opts))]
    (swap! (:sockets env) conj socket-ref)
    { :accept-chan (:accept-chan @socket-ref) }))
