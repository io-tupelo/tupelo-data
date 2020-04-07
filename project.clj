(defproject tupelo-data
  "20.03.01-SNAPSHOT"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :min-lein-version "2.9.1"

  :global-vars {*warn-on-reflection* false}

  :deploy-repositories [["clojars" "http://beta.clojars.org/repo/"]]
  :dependencies
  [
   [org.clojure/clojure "1.10.2-alpha1"]
   [org.clojure/clojurescript "1.10.597"]
   [org.clojure/data.avl "0.1.0"]
   [prismatic/schema "1.1.12"]
   [tupelo "0.9.201"]
   ]

  :plugins [[com.jakemccrary/lein-test-refresh "0.24.1"]]

  :test-refresh {:quiet true ; true => suppress printing namespaces when testing
                 }

  :source-paths ["src/clj" "src/cljc"]
  :test-paths ["test/clj" "test/cljc"]
  :target-path "target/%s"

  ; need to add the compliled assets to the :clean-targets
  :clean-targets ^{:protect false} ["resources/public/js/compiled"
                                    "out"
                                    :target-path]

  :jvm-opts ["-Xms500m" "-Xmx2g"]
  )

