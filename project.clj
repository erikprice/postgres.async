(defproject alaisi/postgres.async "0.3.0-SNAPSHOT"
  :description "Asynchronous PostgreSQL Clojure client"
  :url "http://github.com/alaisi/postgres.async"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :scm {:name "git"
        :url "http://github.com/alaisi/postgres.async.git"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [com.github.alaisi.pgasync/postgres-async-driver "0.3"]]
  :global-vars {*warn-on-reflection* true}
  :target-path "target/%s")
