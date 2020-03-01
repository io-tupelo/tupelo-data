(defproject tupelo-data
  "20.03.01-SNAPSHOT"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :min-lein-version "2.9.1"

  :global-vars {*warn-on-reflection* false}

  :excludes [org.clojure/clojure
             org.clojure/clojurescript]

  :dependencies
  [ 
                                       [org.clojure/clojure "1.10.1"]
                                       [org.clojure/clojurescript "1.10.597"]
   [prismatic/schema "1.1.12"] 
   ]

  :plugins [ [com.jakemccrary/lein-test-refresh "0.24.1"]]

  :test-refresh {:quiet true ; true => suppress printing namespaces when testing
                 }

  
  :source-paths ["src/clj" "src/cljc"]
  :test-paths ["test/clj" "test/cljc"]
  :target-path "target/%s"

  ; need to add the compliled assets to the :clean-targets
  :clean-targets ^{:protect false} ["resources/public/js/compiled"
                                    "out"
                                    :target-path]

  :deploy-repositories {"snapshots"    :clojars
                        "releases"     :clojars
                        :sign-releases false}

  :jvm-opts ["-Xms500m" "-Xmx2g" ]
  )
















