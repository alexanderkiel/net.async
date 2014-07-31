(ns net.async.nio
  (:import [java.nio.channels SocketChannel SelectionKey ServerSocketChannel
            SelectableChannel Selector]
           [java.nio ByteBuffer ByteOrder]
           [java.io Closeable]))

(set! *warn-on-reflection* true)

;; CLOSEABLE

(defn close! [closeable]
  (.close ^Closeable closeable))

;; SELECTOR

(defn open-selector []
  (Selector/open))

(defn select!
  ([selector]
   (.select ^Selector selector))
  ([selector timeout]
   (.select ^Selector selector timeout)))

(defn selected-keys [selector]
  (.selectedKeys ^Selector selector))

(defn clear-selected-keys! [selector]
  (.clear (.selectedKeys ^Selector selector)))

(defn wakeup! [selector]
  (.wakeup ^Selector selector))

;; SELECTABLE CHANNEL

(defn set-non-blocking! [channel]
  (.configureBlocking ^SelectableChannel channel false))

(defn register! [net-chan selector opts socket-ref]
  (.register ^SelectableChannel net-chan selector opts socket-ref))

;; SOCKET CHANNEL

(defn read! [channel dsts offset length]
  (.read ^SocketChannel channel dsts offset length))

(defn write! [channel srcs offset length]
  (.write ^SocketChannel channel srcs offset length))

(defn finish-connect! [channel]
  (.finishConnect ^SocketChannel channel))

;; SERVER SOCKET CHANNEL

(defn accept! [channel]
  (.accept ^ServerSocketChannel channel))

;; BUFFER

(defn get-int [buf idx]
  (.getInt ^ByteBuffer buf idx))

(defn put-int! [buf idx value]
  (.putInt ^ByteBuffer buf idx value))

(defn rewind! [buf]
  (.rewind ^ByteBuffer buf))

(defn array [buf]
  (.array ^ByteBuffer buf))

(defn capacity [buf]
  (.capacity ^ByteBuffer buf))

(defn order [buf]
  (condp = (.order ^ByteBuffer buf)
    ByteOrder/BIG_ENDIAN :big-endian
    ByteOrder/LITTLE_ENDIAN :little-endian))

(defn set-byte-order!
  "Sets the byte order of the buffer which can be :big-endian or
  :little-endian."
  [buf order]
  (.order ^ByteBuffer buf
          (case order
            :big-endian ByteOrder/BIG_ENDIAN
            :little-endian ByteOrder/LITTLE_ENDIAN))
  nil)

;; SELECTION KEY

(def ^:const op-read SelectionKey/OP_READ)
(def ^:const op-write SelectionKey/OP_WRITE)
(def ^:const op-connect SelectionKey/OP_CONNECT)
(def ^:const op-accept SelectionKey/OP_ACCEPT)

(defn valid? [key]
  (.isValid ^SelectionKey key))

(defn readable? [key]
  (.isReadable ^SelectionKey key))

(defn writeable? [key]
  (.isWritable ^SelectionKey key))

(defn connectable? [key]
  (.isConnectable ^SelectionKey key))

(defn acceptable? [key]
  (.isAcceptable ^SelectionKey key))

(defn attachment [key]
  (.attachment ^SelectionKey key))
