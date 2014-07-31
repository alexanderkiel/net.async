(defproject net.async/async "0.1.0"
  :description "Network communications with clojure.core.async interface"
  :license {:name     "Eclipse Public License - v 1.0"
            :url      "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo
            :comments "same as Clojure"}
  :dependencies [
    [org.clojure/clojure "1.6.0"]
    [org.clojure/tools.logging "0.2.6"]
    [org.clojure/core.async "0.1.303.0-886421-alpha"]
  ]
  :profiles {
    :dev {
      :source-paths ["dev"]
      :resource-dirs ["logs"]
      :dependencies [[criterium "0.4.3"]]
      :jvm-opts [
        "-Djava.util.logging.config.file=logs/logging.properties"
      ]
    }
  }
)
