;   Copyright (c) Alan Thompson. All rights reserved.
;   The use and distribution terms for this software are covered by the Eclipse Public License 1.0
;   (http://opensource.org/licenses/eclipse-1.0.php) which can be found in the file epl-v10.html at
;   the root of this distribution.  By using this software in any fashion, you are agreeing to be
;   bound by the terms of this license.  You must not remove this notice, or any other, from this
;   software.
(ns tupelo.graph
  "Effortless data access."
  ;---------------------------------------------------------------------------------------------------
  ;   https://code.thheller.com/blog/shadow-cljs/2019/10/12/clojurescript-macros.html
  ;   http://blog.fikesfarm.com/posts/2015-12-18-clojurescript-macro-tower-and-loop.html
  #?(:cljs (:require-macros
             [tupelo.core]
             [tupelo.graph]
             ))
  (:require
    [tupelo.core :as t :refer [spy spyx spyxx spyx-pretty with-spy-indent spyq spydiv ->true
                               grab glue map-entry indexed only only2 xfirst xsecond xthird xlast xrest not-empty? map-plain?
                               it-> cond-it-> forv vals->map fetch-in let-spy sym->kw with-map-vals vals->map
                               keep-if drop-if append prepend ->sym ->kw kw->sym validate dissoc-in
                               ]]
    [tupelo.data.index :as index]
    [tupelo.schema :as tsk]
    [clojure.set :as set]
    [clojure.walk :as walk]
    [schema.core :as s]
    ) )

#?(:cljs (enable-console-print!))


;-----------------------------------------------------------------------------
(def ^:no-doc nid-count-base 1000)
(def ^:no-doc nid-counter (atom nid-count-base))
(def ^:no-doc rid-count-base 2000)
(def ^:no-doc rid-counter (atom rid-count-base))

(defn ^:no-doc id-reset
  "Reset the eid-count to its initial value (for testing)"
  []
  (reset! nid-counter nid-count-base)
  (reset! rid-counter rid-count-base))

(s/defn ^:no-doc new-nid :- s/Int
  "Returns the next integer EID"
  [] (let [result (swap! nid-counter inc)]
       result))

(s/defn ^:no-doc new-rid :- s/Int
  "Returns the next integer RID"
  [] (let [result (swap! rid-counter inc)]
       result))

;-----------------------------------------------------------------------------
(def ^:dynamic ^:no-doc *grf* nil)
(defmacro with-grf ; #todo swap names?
  [grf-arg & forms]
  `(binding [*grf* (atom ~grf-arg)]
     ~@forms))

(s/defn new-grf :- tsk/KeyMap
  "Returns a new, empty graph db."
  [] (into (sorted-map)
       {:nodes (sorted-map)
        :rels  (sorted-map)
        }))

(s/defn ^:no-doc new-grf-reset []
  (id-reset)
  (new-grf))

(s/defn new-node :- tsk/KeyMap
  ([] (new-node nil))
  ([tag] (let [nid (new-nid)]
           {:nid   nid
            :tag   tag
            :props {}
            :rels  {:in  []
                    :out []}})))

(s/defn add-node
  "Add a node to the graph"
  ([ctx :- tsk/KeyMap] (add-node *grf* ctx))
  ([grf   ; atom
    ctx :- tsk/KeyMap]
   (assert (= clojure.lang.Atom (type grf)))
   (swap! grf (fn [it]
                (let [tag  (t/get-or-nil ctx :tag)
                      node (new-node tag)
                      nid  (grab :nid node)]
                  (assoc-in it [:nodes nid] node))))))















