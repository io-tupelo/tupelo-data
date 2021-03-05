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
    [tupelo.schema :as tsk]
    [tupelo.set :as set]
    [schema.core :as s]
    ))

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
  [ctx]
  {:nid   (new-nid)
   :tag   (t/get-or-nil ctx :tag)
   :props (dissoc ctx :tag)
   :rels  {:rid-out  []
           :to []}})

(s/defn new-rel :- tsk/KeyMap
  [ctx]
  (let [nid-from (grab :rid-out ctx)
        nid-to   (grab :to ctx)]
    {:rid     (new-rid)
     :tag     (grab :tag ctx)
     :rid-out nid-from
     :to      nid-to
     :props   (dissoc ctx :tag :rid-out :to) }))

(s/defn add-node
  "Add a node to the graph"
  ([ctx :- tsk/KeyMap] (add-node *grf* ctx))
  ([grf   ; atom
    ctx :- tsk/KeyMap]
   (assert (= clojure.lang.Atom (type grf)))
   (let [node (new-node ctx)
         nid  (grab :nid node)]
     (swap! grf (fn [it]
                  (assoc-in it [:nodes nid] node)))
     nid)))

(s/defn add-rel
  "Add a relationship to the graph"
  ([ctx :- tsk/KeyMap] (add-rel *grf* ctx))
  ([grf   ; atom
    ctx :- tsk/KeyMap]
   (assert (= clojure.lang.Atom (type grf)))
   (let [rel (new-rel ctx)
         rid (grab :rid rel)
         nid-from (grab :rid-out ctx)
         nid-to   (grab :to ctx)
         ]
     (swap! grf (fn [state]
                  (it-> state
                    (assoc-in it [:rels rid] rel)
                    (update-in it [:nodes nid-from :rels :rid-out] append rid)
                    (update-in it [:nodes nid-to :rels :to] append rid)
                    ) ))
     rid)))

(s/defn nid->node :- tsk/KeyMap
  [nid :- s/Int]
  (fetch-in @*grf* [:nodes nid]))

(s/defn rid->rel
  [rid]
  (fetch-in @*grf* [:rels rid]))

(declare nid->bush  rid->bush)

(s/defn ^:no-doc nid->bush-impl
  [ctx]
  (with-spy-indent
    (spy :-----------------------------)
    (with-map-vals ctx [nid depth nids-used rids-used]
      (spyx [nid depth nids-used rids-used])
      (if (t/contains-elem? nids-used nid)
        {:nid nid}
        (let-spy
          [depth-new      (dec depth)
           node           (nid->node nid)
           rids-from      (set (fetch-in node [:rels :rid-out]))
           rids-loop      (apply set/remove rids-from rids-used)
           nids-used-new  (set/add nids-used nid)
           rids-used-new  (apply set/add rids-used rids-from)
           ctx-base       {:depth     depth-new
                           :nids-used nids-used-new
                           :rids-used rids-used-new}

           rels-from-bush (forv [rid rids-loop]
                            (with-spy-indent
                              (spy :-----------------------------)
                              (let-spy
                                [rel      (rid->rel rid)
                                 nid-kid  (grab :to rel)
                                 kid-node (nid->bush-impl (glue ctx-base {:nid nid-kid}))
                                 result   (it-> rel
                                            (dissoc it :rid-out)
                                            (glue it {:to kid-node}))]
                                result)))
           node-bush      (assoc-in node [:rels :rid-out] rels-from-bush)]
          node-bush
          )))))

(def default-bush-depth 9) ; #todo kludge for convenience
(s/defn nid->bush
  ([nid :- s/Int] (nid->bush nid default-bush-depth))
  ([nid :- s/Int
    depth :- s/Int]
   (nid->bush-impl {:nid       nid
                    :depth     depth
                    :nids-used #{}
                    :rids-used #{}})))














