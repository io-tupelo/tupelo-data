;   Copyright (c) Alan Thompson. All rights reserved.
;   The use and distribution terms for this software are covered by the Eclipse Public License 1.0
;   (http://opensource.org/licenses/eclipse-1.0.php) which can be found in the file epl-v10.html at
;   the root of this distribution.  By using this software in any fashion, you are agreeing to be
;   bound by the terms of this license.  You must not remove this notice, or any other, from this
;   software.
(ns       ; ^:test-refresh/focus
  tst.tupelo.data
  #?(:clj (:refer-clojure :exclude [load ->VecNode]))
  #?(:clj (:require
            [tupelo.test :refer [define-fixture deftest dotest dotest-focus is isnt is= isnt= is-set= is-nonblank= testing throws?]]
            [tupelo.core :as t :refer [spy spyx spyxx spy-pretty spyx-pretty unlazy let-spy
                                       only only2 forv glue grab nl
                                       ]]
            [tupelo.data :as td :refer [with-tdb new-tdb eid-count-reset lookup query-triples boolean->binary search-triple
                                        *tdb*
                                        ]]
            [tupelo.misc :as misc]
            [clojure.set :as set]
            [schema.core :as s]
            [tupelo.data.index :as index]
            [tupelo.string :as ts]
            [tupelo.tag :as tv]
            ))
  #?(:cljs (:require
             [tupelo.test-cljs :refer [define-fixture deftest dotest is isnt is= isnt= is-set= is-nonblank= testing throws?]
              :include-macros true]
             [tupelo.core :as t :refer [spy spyx spyxx] :include-macros true]
             ))
  )

; #todo fix for cljs

#?(:cljs (enable-console-print!))

#?(:clj
 (do

   (dotest
     (let [ss123 (t/it-> (index/empty-index)
                   (conj it [1 :a])
                   (conj it [3 :a])
                   (conj it [2 :a]))
           ss13  (disj ss123 [2 :a])]
       (is= ss123 #{[1 :a] [2 :a] [3 :a]})
       (is= (vec ss123) [[1 :a] [2 :a] [3 :a]])
       (is= ss13 #{[1 :a] [3 :a]}))

     (is (map? (td/wrap-eid 3)))
     (is (map? {:a 1}))
     (isnt (record? (td/wrap-idx 3)))
     (isnt (record? {:a 1}))

     ; Leaf and entity-id records sort separately in the index. Eid sorts first since the type name
     ; `tupelo.data.Eid` sorts before `tupelo.data.Leaf`
     (is= (td/wrap-eid 5) {:eid 5})
     (throws?  (td/wrap-eid "hello"))
     (is= (td/wrap-idx 5) {:idx 5})
     (throws?  (td/wrap-idx "hello"))

     (let [idx      (-> (index/empty-index)

                      (index/add-entry [1 3])
                      (index/add-entry [1 (td/wrap-eid 3)])
                      (index/add-entry [1 1])
                      (index/add-entry [1 (td/wrap-eid 1)])
                      (index/add-entry [1 2])
                      (index/add-entry [1 (td/wrap-eid 2)])

                      (index/add-entry [0 3])
                      (index/add-entry [0 (td/wrap-eid 3)])
                      (index/add-entry [0 1])
                      (index/add-entry [0 (td/wrap-eid 1)])
                      (index/add-entry [0 2])
                      (index/add-entry [0 (td/wrap-eid 2)]))

           expected [[0 {:eid 1}]
                     [0 {:eid 2}]
                     [0 {:eid 3}]
                     [0  1]
                     [0  2]
                     [0  3]
                     [1 {:eid 1}]
                     [1 {:eid 2}]
                     [1 {:eid 3}]
                     [1 1]
                     [1 2]
                     [1 3]]]
       (is= (vec idx) expected))
     )

   (dotest
     (td/with-tdb (td/new-tdb)
       (td/eid-count-reset)
       (is= (deref *tdb*)
         {:eid-type {} :idx-eav #{} :idx-vae #{} :idx-ave #{}})
       (let [edn-val  {:a 1}
             root-eid (td/add-edn edn-val)]
         (is= root-eid
           (td/wrap-eid 1001)
           (tv/new :eid 1001))
         (is= (unlazy (deref *tdb*))
           {:eid-type {{:eid 1001} :map},
            :idx-ave  #{[:a 1 {:eid 1001}]},
            :idx-eav  #{[{:eid 1001} :a 1]},
            :idx-vae  #{[1 :a {:eid 1001}]}})
         (is= edn-val (td/eid->edn root-eid)) )))

   (dotest
     (with-tdb (new-tdb)
       (eid-count-reset)
       (let [edn-val  {:a 1 :b 2}
             root-eid (td/add-edn edn-val)]
         (is= (td/wrap-eid 1001) root-eid)
         (is= (unlazy (deref *tdb*))
           {:eid-type {{:eid 1001} :map},
            :idx-ave  #{[:a 1 {:eid 1001}]
                        [:b 2 {:eid 1001}]},
            :idx-eav  #{[{:eid 1001} :a 1]
                        [{:eid 1001} :b 2]},
            :idx-vae  #{[1 :a {:eid 1001}]
                        [2 :b {:eid 1001}]}} )
         (is= edn-val (td/eid->edn root-eid)) )))

   (dotest
     (with-tdb (new-tdb)
       (eid-count-reset)
       (let [edn-val  {:a 1 :b 2 :c {:d 4}}
             root-eid (td/add-edn edn-val)]
         (is= (td/wrap-eid 1001) root-eid)
         (is= (unlazy (deref *tdb*))
           {:eid-type {{:eid 1001} :map, {:eid 1002} :map},
            :idx-ave  #{[:a 1 {:eid 1001}]
                        [:b 2 {:eid 1001}]
                        [:c {:eid 1002} {:eid 1001}]
                        [:d 4 {:eid 1002}]},
            :idx-eav  #{[{:eid 1001} :a 1]
                        [{:eid 1001} :b 2]
                        [{:eid 1001} :c {:eid 1002}]
                        [{:eid 1002} :d 4]},
            :idx-vae  #{[{:eid 1002} :c {:eid 1001}]
                        [1 :a {:eid 1001}]
                        [2 :b {:eid 1001}]
                        [4 :d {:eid 1002}]}})
         (is= edn-val (td/eid->edn root-eid)))) )

   (dotest
     (with-tdb (new-tdb)
       (eid-count-reset)
       (let [edn-val  [1 2 3]
             root-eid (td/add-edn edn-val)]
         (is= (unlazy (deref *tdb*))
           {:eid-type {{:eid 1001} :array},
            :idx-ave  #{[{:idx 0} 1 {:eid 1001}]
                        [{:idx 1} 2 {:eid 1001}]
                        [{:idx 2} 3 {:eid 1001}]},
            :idx-eav  #{[{:eid 1001} {:idx 0} 1]
                        [{:eid 1001} {:idx 1} 2]
                        [{:eid 1001} {:idx 2} 3]},
            :idx-vae  #{[1 {:idx 0} {:eid 1001}]
                        [2 {:idx 1} {:eid 1001}]
                        [3 {:idx 2} {:eid 1001}]}})
         (is= edn-val (td/eid->edn root-eid)))))

   (dotest
     (with-tdb (new-tdb)
       (eid-count-reset)
       (let [edn-val  {:a 1 :b 2 :c [10 11 12]}
             root-eid (td/add-edn edn-val)]
         (is= (unlazy (deref *tdb*))
           {:eid-type {{:eid 1001} :map, {:eid 1002} :array},
            :idx-ave #{[{:idx 0} 10 {:eid 1002}]
                       [{:idx 1} 11 {:eid 1002}]
                        [{:idx 2} 12 {:eid 1002}]
                       [:a 1 {:eid 1001}]
                       [:b 2 {:eid 1001}]
                        [:c {:eid 1002} {:eid 1001}]},
            :idx-eav #{[{:eid 1001} :a 1]
                       [{:eid 1001} :b 2]
                       [{:eid 1001} :c {:eid 1002}]
                        [{:eid 1002} {:idx 0} 10]
                       [{:eid 1002} {:idx 1} 11]
                        [{:eid 1002} {:idx 2} 12]},
            :idx-vae #{[{:eid 1002} :c {:eid 1001}]
                       [1 :a {:eid 1001}]
                       [2 :b {:eid 1001}]
                        [10 {:idx 0} {:eid 1002}]
                       [11 {:idx 1} {:eid 1002}]
                        [12 {:idx 2} {:eid 1002}]}} )
         (is= edn-val (td/eid->edn root-eid))
         (is= (td/eid->edn (td/wrap-eid 1002)) [10 11 12]))))

   (dotest-focus
     (with-tdb (new-tdb)
       (eid-count-reset)
       (let [data [{:a 1}
                   {:a 2}
                   {:a 3}
                   {:b 1}
                   {:b 2}
                   {:b 3}
                   {:c 1}
                   {:c 2}
                   {:c 3}]]
         (doseq [m data]
           (td/add-edn m))
         (is= (unlazy @*tdb*) ; #todo error: missing :array
           {:eid-type
            {{:eid 1001} :map,
             {:eid 1002} :map,
             {:eid 1003} :map,
             {:eid 1004} :map,
             {:eid 1005} :map,
             {:eid 1006} :map,
             {:eid 1007} :map,
             {:eid 1008} :map,
             {:eid 1009} :map},
            :idx-ave
            #{[:a 1 {:eid 1001}] [:a 2 {:eid 1002}] [:a 3 {:eid 1003}]
              [:b 1 {:eid 1004}] [:b 2 {:eid 1005}] [:b 3 {:eid 1006}]
              [:c 1 {:eid 1007}] [:c 2 {:eid 1008}] [:c 3 {:eid 1009}]},
            :idx-eav
            #{[{:eid 1001} :a 1] [{:eid 1002} :a 2] [{:eid 1003} :a 3]
              [{:eid 1004} :b 1] [{:eid 1005} :b 2] [{:eid 1006} :b 3]
              [{:eid 1007} :c 1] [{:eid 1008} :c 2] [{:eid 1009} :c 3]},
            :idx-vae
            #{[1 :a {:eid 1001}] [1 :b {:eid 1004}] [1 :c {:eid 1007}]
              [2 :a {:eid 1002}] [2 :b {:eid 1005}] [2 :c {:eid 1008}]
              [3 :a {:eid 1003}] [3 :b {:eid 1006}] [3 :c {:eid 1009}]}})
         ;---------------------------------------------------------------------------------------------------
         (is= (lookup [(td/wrap-eid 1003) nil nil])
           #{[{:eid 1003} :a 3]})
         (is= (lookup [nil :b nil])
           #{[{:eid 1004} :b 1]
             [{:eid 1005} :b 2]
             [{:eid 1006} :b 3]})
         (is= (lookup [nil nil 3])
           #{[{:eid 1003} :a 3]
             [{:eid 1006} :b 3]
             [{:eid 1009} :c 3]}))
       ;---------------------------------------------------------------------------------------------------
       (is= (lookup [nil :a 3])
         #{[{:eid 1003} :a 3]})
       (is= (lookup [(td/wrap-eid 1009) nil 3])
         #{[{:eid 1009} :c 3]})
       (is= (lookup [(td/wrap-eid 1005) :b nil])
         #{[{:eid 1005} :b 2]})
       ))

   ;(dotest
   ;  (is= (td/->SearchParam-impl (quote :a))
   ;    '(tupelo.data/->SearchParam-fn (quote :a)))
   ;  (is= (td/->SearchParam-impl (quote b))
   ;    '(tupelo.data/->SearchParam-fn (quote b)))
   ;
   ;  (let [spa (td/->SearchParam :a)
   ;        spb (td/->SearchParam b)]
   ;    (is (td/wrapped-param? spa))
   ;    (is (td/wrapped-param? spb))
   ;    (is= spa {:param :a})
   ;    (is= spb {:param :b}))
   ;
   ;  (with-tdb (new-tdb)
   ;    (eid-count-reset)
   ;    (let [edn-val  {:a 1
   ;                    :b 2}
   ;          root-eid (td/add-edn edn-val)]
   ;      (is= edn-val (td/eid->edn root-eid))
   ;      (is= (unlazy (deref *tdb*))
   ;        {:eid-type {{:eid 1001} :map},
   ;         :idx-ave  #{[{:attr :a} {:leaf 1} {:eid 1001}]
   ;                     [{:attr :b} {:leaf 2} {:eid 1001}]},
   ;         :idx-eav  #{[{:eid 1001} {:attr :a} {:leaf 1}]
   ;                     [{:eid 1001} {:attr :b} {:leaf 2}]},
   ;         :idx-vae  #{[{:leaf 1} {:attr :a} {:eid 1001}]
   ;                     [{:leaf 2} {:attr :b} {:eid 1001}]}}))
   ;
   ;    (let [search-spec [[{:param :x} {:attr :a} {:leaf 1}]]]
   ;      (is= (query-triples search-spec)
   ;        [{{:param :x} {:eid 1001}}]))
   ;
   ;    (let [search-spec [[{:param :x} {:attr :a} {:param :y}]]]
   ;      (is= (query-triples search-spec) [{{:param :x} {:eid 1001},
   ;                                         {:param :y} {:leaf 1}}]))
   ;
   ;    (let [search-spec [[(td/->SearchParam :x) (td/->SearchParam :y) {:leaf 1}]]]
   ;      (is= (query-triples search-spec) [{{:param :x} {:eid 1001},
   ;                                         {:param :y} {:attr :a}}]))))
   ;
   ;(dotest
   ;  (with-tdb (new-tdb)
   ;    (eid-count-reset)
   ;    (let [edn-val  {:a 1
   ;                    :b 1}
   ;          root-eid (td/add-edn edn-val)]
   ;      (is= edn-val (td/eid->edn root-eid))
   ;      (is= (deref *tdb*)
   ;        {:eid-type {{:eid 1001} :map},
   ;         :idx-ave  #{[{:attr :a} {:leaf 1} {:eid 1001}]
   ;                     [{:attr :b} {:leaf 1} {:eid 1001}]},
   ;         :idx-eav  #{[{:eid 1001} {:attr :a} {:leaf 1}]
   ;                     [{:eid 1001} {:attr :b} {:leaf 1}]},
   ;         :idx-vae  #{[{:leaf 1} {:attr :a} {:eid 1001}]
   ;                     [{:leaf 1} {:attr :b} {:eid 1001}]}}))
   ;
   ;    (let [search-spec [[(td/->SearchParam :x) (td/wrap-attr :a) (td/wrap-leaf 1)]]]
   ;      (is= (query-triples search-spec) [{{:param :x} {:eid 1001}}]))
   ;
   ;    (let [search-spec [[(td/->SearchParam :x) (td/wrap-attr :b) (td/wrap-leaf 1)]]]
   ;      (is= (query-triples search-spec) [{{:param :x} {:eid 1001}}]))
   ;
   ;    (let [search-spec [[(td/->SearchParam :x) (td/->SearchParam :y) (td/wrap-leaf 1)]]]
   ;      (is= (query-triples search-spec)
   ;        [{{:param :x} {:eid 1001},
   ;          {:param :y} {:attr :a}}
   ;
   ;         {{:param :x} {:eid 1001},
   ;          {:param :y} {:attr :b}}]))))
   ;
   ;(dotest
   ;  (with-tdb (new-tdb)
   ;    (eid-count-reset)
   ;    (let [edn-val  {:a {:b 2}}
   ;          root-eid (td/add-edn edn-val)]
   ;      (is= (deref *tdb*)
   ;        {:eid-type {{:eid 1001} :map, {:eid 1002} :map},
   ;         :idx-ave  #{[{:attr :a} {:eid 1002} {:eid 1001}]
   ;                     [{:attr :b} {:leaf 2} {:eid 1002}]},
   ;         :idx-eav  #{[{:eid 1001} {:attr :a} {:eid 1002}]
   ;                     [{:eid 1002} {:attr :b} {:leaf 2}]},
   ;         :idx-vae  #{[{:eid 1002} {:attr :a} {:eid 1001}]
   ;                     [{:leaf 2} {:attr :b} {:eid 1002}]}})
   ;      ; (prn :-----------------------------------------------------------------------------)
   ;      ; compound search
   ;      (is= (query-triples [[{:param :x} {:attr :a} {:param :y}]
   ;                           [{:param :y} {:attr :b} (td/wrap-leaf 2)]])
   ;        [{{:param :x} {:eid 1001},
   ;          {:param :y} {:eid 1002}}])
   ;      ; (prn :-----------------------------------------------------------------------------)
   ;      ; failing search
   ;      (is= [] (query-triples [[{:param :x} {:attr :a} {:param :y}]
   ;                              [{:param :y} {:attr :b} (td/wrap-leaf 99)]]))
   ;      ; (prn :-----------------------------------------------------------------------------)
   ;      ; wildcard search - match all
   ;      (is= (query-triples [[{:param :x} {:param :y} {:param :z}]])
   ;        [{{:param :x} {:eid 1001},
   ;          {:param :y} {:attr :a},
   ;          {:param :z} {:eid 1002}}
   ;         {{:param :x} {:eid 1002},
   ;          {:param :y} {:attr :b},
   ;          {:param :z} {:leaf 2}}]))))
   ;
   ;(dotest
   ;  (is= 1 (boolean->binary true))
   ;  (is= 0 (boolean->binary false))
   ;  (throws? (boolean->binary))
   ;  (throws? (boolean->binary 234)))
   ;
   ;(dotest
   ;  (is= (td/search-triple-impl (quote [:a :b :c]))
   ;    '(tupelo.data/search-triple-fn (quote [:a :b :c])))
   ;  (is= (td/search-triple-impl (quote [a b c]))
   ;    '(tupelo.data/search-triple-fn (quote [a b c])))
   ;
   ;  (is= (td/search-triple x y z)
   ;    [(td/->SearchParam x)
   ;     (td/->SearchParam y)
   ;     (td/->SearchParam z)]
   ;    [{:param :x} {:param :y} {:param :z}])
   ;  (is= (td/search-triple 123 :color "Joey")
   ;    [(td/wrap-eid 123) (td/wrap-attr :color) (td/wrap-leaf "Joey")]))
   ;
   ;(dotest
   ;  (with-tdb (new-tdb)
   ;    (eid-count-reset)
   ;    (let [edn-val     {:a {:b 2}}
   ;          root-eid    (td/add-edn edn-val)
   ;          search-spec [(search-triple x :a y)
   ;                       (search-triple y :b 2)]]
   ;      (is= (query-triples search-spec)
   ;        (quote [{{:param :x} {:eid 1001}
   ;                 {:param :y} {:eid 1002}}]))))
   ;
   ;  (with-tdb (new-tdb)
   ;    (eid-count-reset)
   ;    (let [edn-val     {:a {:b 2}}
   ;          root-eid    (td/add-edn edn-val)
   ;          search-spec [(search-triple y :b 2) (search-triple x :a y)]]
   ;      (is= (query-triples search-spec)
   ;        [{{:param :x} {:eid 1001}
   ;          {:param :y} {:eid 1002}}])))
   ;
   ;  (with-tdb (new-tdb)
   ;    (eid-count-reset)
   ;    (let [edn-val     {:a {:b 2}}
   ;          root-eid    (td/add-edn edn-val)
   ;          search-spec [(search-triple x :a y) (search-triple y :b 99)]]
   ;      (is= [] (query-triples search-spec))))
   ;
   ;  (with-tdb (new-tdb)
   ;    (eid-count-reset)
   ;    (let [edn-val     {:a {:b 2}}
   ;          root-eid    (td/add-edn edn-val)
   ;          search-spec [(search-triple y :b 99) (search-triple x :a y)]]
   ;      (is= [] (query-triples search-spec))))
   ;  )
   ;
   ;(dotest
   ;  (binding [td/*all-triples*   (atom []) ; receives output!
   ;            td/*autosyms-seen* (atom #{})]
   ;    (td/query-maps->triples (quote
   ;                              [{:eid x :map y}
   ;                               {:eid y :a a}]))
   ;    (is= (deref td/*all-triples*)
   ;      [[{:param :x} {:attr :map} {:param :y}]
   ;       [{:param :y} {:attr :a} {:param :a}]]))
   ;
   ;  (binding [td/*all-triples*   (atom []) ; receives output!
   ;            td/*autosyms-seen* (atom #{})]
   ;    (td/query-maps->triples (quote [{:eid ? :map y}]))
   ;    (is= (deref td/*all-triples*)
   ;      [[{:param :eid} {:attr :map} {:param :y}]]))
   ;
   ;  (binding [td/*all-triples*   (atom []) ; receives output!
   ;            td/*autosyms-seen* (atom #{})]
   ;    (td/query-maps->triples (quote [{:eid ? :map y}
   ;                                    {:eid y :a a}]))
   ;    (is= (deref td/*all-triples*)
   ;      [[{:param :eid} {:attr :map} {:param :y}]
   ;       [{:param :y} {:attr :a} {:param :a}]]))
   ;
   ;  (binding [td/*all-triples*   (atom []) ; receives output!
   ;            td/*autosyms-seen* (atom #{})]
   ;    (throws? (td/query-maps->triples
   ;               (quote [{:eid ? :map y}
   ;                       {:eid ? :a a}])))))
   ;
   ;(dotest
   ;  (is (td/param-tmp-eid? {:param :tmp-eid-99999}))
   ;  (is (td/tmp-attr-kw? :tmp-attr-99999)))
   ;
   ;(def edn-val  (glue (sorted-map)
   ;                {:num     5
   ;                 :map     {:a 1 :b 2}
   ;                 :hashmap {:a 21 :b 22}
   ;                 :vec     [5 6 7]
   ;                 ;  :set #{3 4}  ; #todo add sets
   ;                 :str     "hello"
   ;                 :kw      :nothing}) )
   ;
   ;
   ;(dotest
   ;  (td/with-tdb (td/new-tdb)
   ;    (td/eid-count-reset)
   ;    (let [ root-hid (td/add-edn edn-val)]
   ;      ; (spyx-pretty (grab :idx-eav (deref *tdb*)))
   ;      (is= edn-val (td/eid->edn root-hid))
   ;
   ;      (when true
   ;        (let [eids-match (td/index-find-leaf 1) ; only 1 match
   ;              entity-edn (td/eid->edn (only eids-match))]
   ;          (is= entity-edn {:a 1, :b 2}))
   ;        (is= (query-triples [(search-triple e :num v)])
   ;          [{{:param :e} {:eid 1001},
   ;            {:param :v} {:leaf 5}}])
   ;        (is= (query-triples [(search-triple e a "hello")])
   ;          [{{:param :e} {:eid 1001},
   ;            {:param :a} {:attr :str}}])
   ;        (is= (query-triples [(search-triple e a 7)])
   ;          [{{:param :e} {:eid 1004},
   ;            {:param :a} {:attr 2}}]))
   ;
   ;      (is= {:param :x}
   ;        (td/->SearchParam-fn (quote x))
   ;        (td/->SearchParam-fn :x))
   ;
   ;      (is= (td/query-maps->wrapped-impl (quote [{:eid x :map y}
   ;                                                {:eid y :a a}]))
   ;        '(tupelo.data/query-maps->wrapped-fn (quote [{:eid x, :map y}
   ;                                                     {:eid y, :a a}])))
   ;      ))
   ;
   ;  (dotest
   ;    (td/with-tdb (td/new-tdb)
   ;      (td/eid-count-reset)
   ;
   ;      (is= (td/query-maps->wrapped [{:eid x :map y}
   ;                                    {:eid y :a a}])
   ;        [{{:param :x} {:eid 1001},
   ;          {:param :y} {:eid 1003},
   ;          {:param :a} {:leaf 1}}])
   ;      (is= edn-val (td/eid->edn {:eid 1001}))
   ;      (is= (td/eid->edn {:eid 1003}) {:a 1 :b 2})
   ;      (comment ; #todo API:  output should look like
   ;        {:x 1001 :y 1002 :a 1})
   ;
   ;      (let [r1 (only (td/query-triples [(search-triple e i 7)]))
   ;            r2 (only (td/index-find-leaf 7))]
   ;        (is= r1 {{:param :e} {:eid 1004}
   ;                 {:param :i} {:attr 2}})
   ;        (is= (td/eid->edn {:eid 1004}) [5 6 7])
   ;        (is= r2 {:eid 1004})
   ;        (is= (td/eid->edn r2) [5 6 7]))
   ;
   ;      ))
   ;  (dotest
   ;    (td/with-tdb (td/new-tdb)
   ;      (td/eid-count-reset)
   ;      (when false
   ;        (nl)
   ;        (spyx-pretty
   ;          (td/query-maps->wrapped-impl (quote [{:eid x :map {:a a}}]))))
   ;
   ;      (is= (td/query-maps->wrapped [{:eid x :map {:a a}}])
   ;        [{{:param :x} {:eid 1001},
   ;          {:param :a} {:leaf 1}}])
   ;      (is= :x (td/unwrap-param {:param :x}))
   ;      (is= 1234 (td/unwrap-eid {:eid 1234}))
   ;      (is= :color (td/unwrap-attr {:attr :color}))
   ;      (is= 42 (td/unwrap-leaf {:leaf 42}))
   ;
   ;      (is= (td/query-maps->wrapped [{:map {:a a}}])
   ;        [{{:param :a} {:leaf 1}}])
   ;      (is= (td/query-maps->wrapped [{:hashmap {:a a}}])
   ;        [{{:param :a} {:leaf 21}}])
   ;
   ;      ))
   ;  (dotest
   ;    (td/with-tdb (td/new-tdb)
   ;      (td/eid-count-reset)
   ;
   ;      (binding [td/*autosyms-seen* (atom #{})]
   ;        (is= (symbol "a") (td/autosym-resolve :a (quote ?)))
   ;        (throws? (td/autosym-resolve :a (quote ?)))) ;attempted duplicate throws
   ;
   ;      (throws? (td/exclude-reserved-identifiers {:a {:tmp-eid-123 :x}}))
   ;      (throws? (td/exclude-reserved-identifiers (quote {:a {:x [1 2 3 tmp-eid-123 4 5 6]}})))
   ;      (throws? (td/query-maps->wrapped-impl (quote [{:map {:a tmp-eid-123}}])))
   ;
   ;      (is= (td/query-maps->wrapped [{:map {:a ?}}])
   ;        [{{:param :a} {:leaf 1}}])
   ;
   ;      (is= (td/unwrap-query-results [{{:param :a} {:leaf 1}}])
   ;        [{:a 1}])
   ;      (is= (td/unwrap-query-results [{{:param :x} {:eid 1001},
   ;                                      {:param :y} {:eid 1002},
   ;                                      {:param :a} {:leaf 1}}])
   ;        [{:x 1001, :y 1002, :a 1}])
   ;      (is= (td/unwrap-query-results [{{:param :e} {:eid 1003}
   ;                                      {:param :i} {:attr 2}}])
   ;        [{:e 1003, :i 2}])
   ;
   ;      (is= (td/query-maps [{:map     {:a a1}
   ;                            :hashmap {:a a2}}])
   ;        [{:a1 1
   ;          :a2 21}])
   ;
   ;      (is-set= (td/query-maps [{kk {:a ?}}]) ; Awesome!  Found both solutions!
   ;        [{:kk :map, :a 1}
   ;         {:kk :hashmap, :a 21}])
   ;
   ;      (is= (only (td/query-maps [{:num ?}])) {:num 5})
   ;      (is= (only (td/query-maps [{:eid ? :num ?}])) {:eid 1001, :num 5})
   ;      (is= (only (td/query-maps [{:eid ? :num num}])) {:eid 1001, :num 5})
   ;
   ;      )))
   ;
   ;; #todo need to convert all from compile-time macros to runtime functions
   ;(dotest
   ;  (td/eid-count-reset)
   ;  (td/with-tdb (td/new-tdb)
   ;    ; (td/eid-count-reset)
   ;    (let [hospital             {:hospital "Hans Jopkins"
   ;                                :staff    {
   ;                                           10 {:first-name "John"
   ;                                               :last-name  "Doe"
   ;                                               :salary     40000
   ;                                               :position   :resident}
   ;                                           11 {:first-name "Joey"
   ;                                               :last-name  "Buttafucco"
   ;                                               :salary     42000
   ;                                               :position   :resident}
   ;                                           20 {:first-name "Jane"
   ;                                               :last-name  "Deer"
   ;                                               :salary     100000
   ;                                               :position   :attending}
   ;                                           21 {:first-name "Dear"
   ;                                               :last-name  "Jane"
   ;                                               :salary     102000
   ;                                               :position   :attending}
   ;                                           30 {:first-name "Sam"
   ;                                               :last-name  "Waterman"
   ;                                               :salary     0
   ;                                               :position   :volunteer}
   ;                                           31 {:first-name "Sammy"
   ;                                               :last-name  "Davis"
   ;                                               :salary     0
   ;                                               :position   :volunteer}
   ;                                           }}
   ;          root-hid             (td/add-edn hospital)
   ;          nm-sal-all           (td/query-maps [{:first-name ? :salary ?}])
   ;          nm-sal-attending     (td/query-maps [{:first-name ? :salary ? :position :attending}])
   ;          nm-sal-resident      (td/query-maps [{:first-name ? :salary ? :position :resident}])
   ;          nm-sal-volunteer     (td/query-maps [{:first-name ? :salary ? :position :volunteer}])
   ;          avg-fn               (fn [vals]
   ;                                 (let [n      (count vals)
   ;                                       total  (reduce + 0 vals)
   ;                                       result (/ total n)]
   ;                                   result))
   ;          salary-avg-attending (avg-fn (mapv :salary nm-sal-attending))
   ;          salary-avg-resident  (avg-fn (mapv :salary nm-sal-resident))
   ;          salary-avg-volunteer (avg-fn (mapv :salary nm-sal-volunteer))
   ;          ]
   ;      (is= nm-sal-all [{:first-name "Joey", :salary 42000}
   ;                       {:first-name "Dear", :salary 102000}
   ;                       {:first-name "Jane", :salary 100000}
   ;                       {:first-name "John", :salary 40000}
   ;                       {:first-name "Sam", :salary 0}
   ;                       {:first-name "Sammy", :salary 0}])
   ;      (is= nm-sal-attending
   ;        [{:first-name "Dear", :salary 102000}
   ;         {:first-name "Jane", :salary 100000}])
   ;
   ;      (is= salary-avg-attending 101000)
   ;      (is= salary-avg-resident 41000)
   ;      (is= salary-avg-volunteer 0))))
   ;
   ;(dotest
   ;  (td/with-tdb (td/new-tdb)
   ;    (td/eid-count-reset)
   ;    (let [edn-val  {:aa [1 2 3]
   ;                    :bb [2 3 4]
   ;                    :cc [3 4 5 6]}
   ;          root-eid (td/add-edn edn-val)]
   ;      (comment
   ;        (spyx-pretty (unlazy (deref td/*tdb*)))
   ;        {:eid-type
   ;         {{:eid 1009} :map,
   ;          {:eid 1010} :array,
   ;          {:eid 1011} :array,
   ;          {:eid 1012} :array},
   ;         :idx-ave
   ;         #{[{:attr :aa} {:eid 1010} {:eid 1009}]
   ;           [{:attr :bb} {:eid 1011} {:eid 1009}]
   ;           [{:attr :cc} {:eid 1012} {:eid 1009}]
   ;           [{:attr 0} {:leaf 1} {:eid 1010}] [{:attr 0} {:leaf 2} {:eid 1011}]
   ;           [{:attr 0} {:leaf 3} {:eid 1012}] [{:attr 1} {:leaf 2} {:eid 1010}]
   ;           [{:attr 1} {:leaf 3} {:eid 1011}] [{:attr 1} {:leaf 4} {:eid 1012}]
   ;           [{:attr 2} {:leaf 3} {:eid 1010}] [{:attr 2} {:leaf 4} {:eid 1011}]
   ;           [{:attr 2} {:leaf 5} {:eid 1012}]
   ;           [{:attr 3} {:leaf 6} {:eid 1012}]},
   ;         :idx-eav
   ;         #{[{:eid 1009} {:attr :aa} {:eid 1010}]
   ;           [{:eid 1009} {:attr :bb} {:eid 1011}]
   ;           [{:eid 1009} {:attr :cc} {:eid 1012}]
   ;           [{:eid 1010} {:attr 0} {:leaf 1}] [{:eid 1010} {:attr 1} {:leaf 2}]
   ;           [{:eid 1010} {:attr 2} {:leaf 3}] [{:eid 1011} {:attr 0} {:leaf 2}]
   ;           [{:eid 1011} {:attr 1} {:leaf 3}] [{:eid 1011} {:attr 2} {:leaf 4}]
   ;           [{:eid 1012} {:attr 0} {:leaf 3}] [{:eid 1012} {:attr 1} {:leaf 4}]
   ;           [{:eid 1012} {:attr 2} {:leaf 5}]
   ;           [{:eid 1012} {:attr 3} {:leaf 6}]},
   ;         :idx-vae
   ;         #{[{:eid 1010} {:attr :aa} {:eid 1009}]
   ;           [{:eid 1011} {:attr :bb} {:eid 1009}]
   ;           [{:eid 1012} {:attr :cc} {:eid 1009}]
   ;           [{:leaf 1} {:attr 0} {:eid 1010}] [{:leaf 2} {:attr 0} {:eid 1011}]
   ;           [{:leaf 2} {:attr 1} {:eid 1010}] [{:leaf 3} {:attr 0} {:eid 1012}]
   ;           [{:leaf 3} {:attr 1} {:eid 1011}] [{:leaf 3} {:attr 2} {:eid 1010}]
   ;           [{:leaf 4} {:attr 1} {:eid 1012}] [{:leaf 4} {:attr 2} {:eid 1011}]
   ;           [{:leaf 5} {:attr 2} {:eid 1012}] [{:leaf 6} {:attr 3} {:eid 1012}]}})
   ;      (let [found (td/query-triples [(td/search-triple ? 1 2)])]
   ;        (is= (td/eid->edn root-eid) {:aa [1 2 3], :bb [2 3 4], :cc [3 4 5 6]})
   ;        (is= (td/eid->edn (val (t/only2 found))) [1 2 3]))
   ;      (let [found    (td/query-triples [(td/search-triple eid idx 3)])
   ;            entities (t/it-> found
   ;                       (mapv #(grab {:param :eid} %) it)
   ;                       (mapv td/eid->edn it))]
   ;        (is-set= found
   ;          [{{:param :eid} {:eid 1004}, {:param :idx} {:attr 0}}
   ;           {{:param :eid} {:eid 1003}, {:param :idx} {:attr 1}}
   ;           {{:param :eid} {:eid 1002}, {:param :idx} {:attr 2}}])
   ;        (is-set= entities [[1 2 3] [2 3 4] [3 4 5 6]]))
   ;
   ;      (is= (td/eid->edn (val (t/only2 (td/query-triples [(td/search-triple eid 2 3)])))) [1 2 3])
   ;      (is= (td/eid->edn (val (t/only2 (td/query-triples [(td/search-triple eid 1 3)])))) [2 3 4])
   ;      (is= (td/eid->edn (val (t/only2 (td/query-triples [(td/search-triple eid 0 3)])))) [3 4 5 6])
   ;      )))
   ;
   ;
   ;(dotest
   ;  (td/with-tdb (td/new-tdb)
   ;    (let [edn-val  {:a 1 :b 2}
   ;          root-eid (td/add-edn edn-val)]
   ;      (is= edn-val (td/eid->edn root-eid))))
   ;
   ;  (td/with-tdb (td/new-tdb)
   ;    (let [edn-val  [1 2 3]
   ;          root-eid (td/add-edn edn-val)]
   ;      (is= edn-val (td/eid->edn root-eid))))
   ;
   ;  (td/with-tdb (td/new-tdb)
   ;    (let [edn-val  {:val "hello"}
   ;          root-eid (td/add-edn edn-val)]
   ;      (is= edn-val (td/eid->edn root-eid))))
   ;
   ;  (td/with-tdb (td/new-tdb)
   ;    (let [data-val {:a [{:b 2}
   ;                        {:c 3}
   ;                        {:d 4}]
   ;                    :e {:f 6}
   ;                    :g :green
   ;                    :h "hotel"
   ;                    :i 1}
   ;          root-eid (td/add-edn data-val)]
   ;      (is= data-val (td/eid->edn root-eid)))))
   ;
   ;(dotest
   ;  (td/with-tdb (td/new-tdb)
   ;    (let [edn-val    #{1 2 3}
   ;          root-hid   (td/add-edn edn-val)
   ;          ; >> (spyx-pretty (deref td/*tdb*))
   ;          edn-result (td/eid->edn root-hid)]
   ;      (is (set? edn-result)) ; ***** Sets are coerced to vectors! *****
   ;      (is-set= [1 2 3] edn-result)))
   ;  (td/with-tdb (td/new-tdb)
   ;    (let [edn-val  #{:a 1 :b 2}
   ;          root-hid (td/add-edn edn-val)]
   ;      (is= edn-val (td/eid->edn root-hid))))
   ;  (td/with-tdb (td/new-tdb)
   ;    (let [edn-val  {:a 1 :b #{1 2 3}}
   ;          root-hid (td/add-edn edn-val)]
   ;      (is= edn-val (td/eid->edn root-hid)))))
   ;
   ;(dotest
   ;  (td/with-tdb (td/new-tdb)
   ;    (let [data     {:a [{:b 2}
   ;                        {:c 3}
   ;                        {:d 4}]
   ;                    :e {:f 6}
   ;                    :g :green
   ;                    :h "hotel"
   ;                    :i 1}
   ;          root-hid (td/add-edn data)]
   ;      (let [found (td/query-maps [{:a ?}])]
   ;        (is= (td/eid->edn (td/wrap-eid (val (only2 found))))
   ;          [{:b 2} {:c 3} {:d 4}]))
   ;      (let [found (td/query-maps [{:a e1}
   ;                                  {:eid e1 0 val}])]
   ;        (is= (td/eid->edn (td/wrap-eid (:val (only found))))
   ;          {:b 2}))
   ;      (let [found (td/query-triples [(td/search-triple e1 :a e2)
   ;                                     (td/search-triple e2 2 e3)])]
   ;        (is= (td/eid->edn (grab {:param :e3} (only found))) {:d 4}))
   ;
   ;      (let [found (td/query-triples [(td/search-triple e1 a1 e2)
   ;                                     (td/search-triple e2 a2 e3)
   ;                                     (td/search-triple e3 a3 4)])]
   ;        (is= data (td/eid->edn (grab {:param :e1} (only found))))))))
   ;
   ;(dotest
   ;  (td/with-tdb (td/new-tdb)
   ;    (td/eid-count-reset)
   ;    (let [data          [{:a 1 :b :first}
   ;                         {:a 2 :b :second}
   ;                         {:a 3 :b :third}
   ;                         {:a 4 :b "fourth"}
   ;                         {:a 5 :b "fifth"}
   ;                         {:a 1 :b 101}
   ;                         {:a 1 :b 102}]
   ;          root-hid      (td/add-edn data)
   ;          found         (td/query-maps [{:eid ? a1 1}])
   ;          eids          (mapv #(grab :eid %) found)
   ;          one-leaf-maps (mapv #(td/eid->edn (td/wrap-eid %)) eids)]
   ;      (is-set= found [{:eid 1008, :a1 :a} {:eid 1007, :a1 :a} {:eid 1002, :a1 :a}])
   ;      (is-set= one-leaf-maps [{:a 1, :b :first}
   ;                              {:a 1, :b 101}
   ;                              {:a 1, :b 102}])))
   ;
   ;  (td/with-tdb (td/new-tdb)
   ;    (let [data     [{:a 1 :x :first}
   ;                    {:a 2 :x :second}
   ;                    {:a 3 :x :third}
   ;                    {:b 1 :x 101}
   ;                    {:b 2 :x 102}
   ;                    {:c 1 :x 301}
   ;                    {:c 2 :x 302}]
   ;          root-hid (td/add-edn data)
   ;          found    (td/query-maps [{:eid ? :a 1}])]
   ;      (is= (td/eid->edn (only found))
   ;        {:a 1 :x :first}))))
   ;
   ;(dotest
   ;  (td/with-tdb (td/new-tdb)
   ;    (let [data     [{:a 1 :b 1 :c 1}
   ;                    {:a 1 :b 2 :c 2}
   ;                    {:a 1 :b 1 :c 3}
   ;                    {:a 2 :b 2 :c 4}
   ;                    {:a 2 :b 1 :c 5}
   ;                    {:a 2 :b 2 :c 6}]
   ;          root-hid (td/add-edn data)]
   ;
   ;      (let [found (td/query-maps [{:eid ? :a 1}])]
   ;        (is-set= (mapv td/eid->edn found)
   ;          [{:a 1, :b 1, :c 1}
   ;           {:a 1, :b 2, :c 2}
   ;           {:a 1, :b 1, :c 3}]))
   ;      (let [found (td/query-maps [{:eid ? :a 2}])]
   ;        (is-set= (mapv td/eid->edn found)
   ;          [{:a 2, :b 2, :c 4}
   ;           {:a 2, :b 1, :c 5}
   ;           {:a 2, :b 2, :c 6}]))
   ;      (let [found (td/query-maps [{:eid ? :b 1}])]
   ;        (is-set= (mapv td/eid->edn found)
   ;          [{:a 1, :b 1, :c 1}
   ;           {:a 1, :b 1, :c 3}
   ;           {:a 2, :b 1, :c 5}]))
   ;      (let [found (td/query-maps [{:eid ? :c 6}])]
   ;        (is-set= (mapv td/eid->edn found)
   ;          [{:a 2, :b 2, :c 6}]))
   ;
   ;      (let [found (td/query-maps [{:eid ? :a 1 :b 2}])]
   ;        (is-set= (mapv td/eid->edn found)
   ;          [{:a 1, :b 2, :c 2}]))
   ;      (let [found (td/query-maps [{:eid ? :a 1 :b 1}])]
   ;        (is-set= (mapv td/eid->edn found)
   ;          [{:a 1, :b 1, :c 1}
   ;           {:a 1, :b 1, :c 3}])))))
   ;
   ;(dotest
   ;  (is= (td/seq->idx-map [:a :b :c]) {0 :a, 1 :b, 2 :c})
   ;
   ;  (td/with-tdb (td/new-tdb)
   ;    (td/eid-count-reset)
   ;    (let [data     {:a [{:id [2 22] :color :red}
   ;                        {:id [3 33] :color :yellow}
   ;                        {:id [4 44] :color :blue}]}
   ;          root-eid (td/add-edn data)]
   ;      ; (spyx-pretty (deref td/*tdb*))
   ;      ;  #todo ***** don't have any way to wildcard test this yet *****
   ;      ; (spyx-pretty (td/query-maps [{:a [{:color cc}]}]))
   ;      ;  => [{:tmp-attr-34956 0, :cc :red}
   ;      ;      {:tmp-attr-34956 1, :cc :yellow}
   ;      ;      {:tmp-attr-34956 2, :cc :blue}]
   ;      ))
   ;  (td/with-tdb (td/new-tdb)
   ;    (td/eid-count-reset)
   ;    (let [data     {:a [{:id [2 22] :color :red}
   ;                        {:id [3 33] :color :yellow}
   ;                        {:id [4 44] :color :blue}]}
   ;          root-eid (td/add-edn data)]
   ;      ;(spyx-pretty (td/query-maps [{:a [{:color :red}
   ;      ;                                  {:color :blue}]}]))
   ;      ;  => [{:tmp-attr-38069 0, :tmp-attr-38070 2}]
   ;      )))
   ;
   ;(dotest
   ;  (td/with-tdb (td/new-tdb)
   ;    (td/eid-count-reset)
   ;    (let [data     {:a [{:id 2 :color :red}
   ;                        {:id 3 :color :yellow}
   ;                        {:id 4 :color :blue}
   ;                        {:id 5 :color :pink}
   ;                        {:id 6 :color :white}]
   ;                    :b {:c [{:ident 2 :flower :rose}
   ;                            {:ident 3 :flower :daisy}
   ;                            {:ident 4 :flower :tulip}
   ;                            ]}}
   ;          root-hid (td/add-edn data)]
   ;      ;(spyx-pretty (td/query-maps [{:eid ? :a [{:id ?}]}]))  =>
   ;      ;  [{:eid 1001, :tmp-attr-42122 0, :id 2}
   ;      ;   {:eid 1001, :tmp-attr-42122 3, :id 5}
   ;      ;   {:eid 1001, :tmp-attr-42122 4, :id 6}
   ;      ;   {:eid 1001, :tmp-attr-42122 1, :id 3}
   ;      ;   {:eid 1001, :tmp-attr-42122 2, :id 4}]
   ;      ;(spyx-pretty (td/query-maps [{:b {:eid ? :c [{:ident ?}]}}])) =>
   ;      ;    [{:eid 1008, :tmp-attr-43452 1, :ident 3}
   ;      ;     {:eid 1008, :tmp-attr-43452 2, :ident 4}
   ;      ;     {:eid 1008, :tmp-attr-43452 0, :ident 2}]
   ;      )))
   ;
   ;(dotest
   ;  (td/with-tdb (td/new-tdb)
   ;    (td/eid-count-reset)
   ;    (let [data     {:a [{:id 2 :color :red}
   ;                        {:id 3 :color :yellow}
   ;                        {:id 4 :color :blue}]}
   ;          root-hid (td/add-edn data)]
   ;      (is= (td/eid->edn (only (td/query-maps [{:eid ? :color :red}])))
   ;        {:color :red, :id 2})
   ;      (is= (td/eid->edn (only (td/query-maps [{:eid ? :id 4}])))
   ;        {:color :blue, :id 4}))))
   ;
   ;(dotest
   ;  (td/with-tdb (td/new-tdb)
   ;    (td/eid-count-reset)
   ;    (let [data     {:a [{:id 2 :color :red}
   ;                        {:id 3 :color :yellow}
   ;                        {:id 4 :color :blue}
   ;                        {:id 5 :color :pink}
   ;                        {:id 6 :color :white}]
   ;                    :b {:c [{:ident 2 :flower :rose}
   ;                            {:ident 3 :flower :daisy}
   ;                            {:ident 4 :flower :tulip}
   ;                            ]}}
   ;          root-hid (td/add-edn data)]
   ;      (is= (td/eid->edn (only (td/query-maps [{:eid ?, :id 2}])))
   ;        {:color :red, :id 2})
   ;      (is= (td/eid->edn (only (td/query-maps [{:eid ?, :ident 2}])))
   ;        {:flower :rose, :ident 2})
   ;      (is= (td/eid->edn (only (td/query-maps [{:eid ?, :flower :rose}])))
   ;        {:flower :rose, :ident 2})
   ;      (is= (td/eid->edn (only (td/query-maps [{:eid ?, :color :pink}])))
   ;        {:color :pink, :id 5}))))
   ;
   ;(dotest
   ;  (td/with-tdb (td/new-tdb)
   ;    (let [skynet-widgets   [{:basic-info   {:producer-code "Cyberdyne"}
   ;                             :widgets      [{:widget-code      "Model-101"
   ;                                             :widget-type-code "t800"}
   ;                                            {:widget-code      "Model-102"
   ;                                             :widget-type-code "t800"}
   ;                                            {:widget-code      "Model-201"
   ;                                             :widget-type-code "t1000"}]
   ;                             :widget-types [{:widget-type-code "t800"
   ;                                             :description      "Resistance Infiltrator"}
   ;                                            {:widget-type-code "t1000"
   ;                                             :description      "Mimetic polyalloy"}]}
   ;                            {:basic-info   {:producer-code "ACME"}
   ;                             :widgets      [{:widget-code      "Dynamite"
   ;                                             :widget-type-code "c40"}]
   ;                             :widget-types [{:widget-type-code "c40"
   ;                                             :description      "Boom!"}]}]
   ;          root-eid         (td/add-edn skynet-widgets)
   ;          search-results   (td/query-maps [{:basic-info   {:producer-code ?}
   ;                                            :widgets      [{:widget-code      ?
   ;                                                            :widget-type-code wtc}]
   ;                                            :widget-types [{:widget-type-code wtc
   ;                                                            :description      ?}]}])
   ;          results-filtered (t/walk-with-parents search-results
   ;                             {:enter (fn [parents item]
   ;                                       (t/cond-it-> item
   ;                                         (map? item) (t/drop-if (fn [k v] (td/tmp-attr-kw? k))
   ;                                                       item)))})]
   ;      (is-set= results-filtered
   ;        [{:producer-code "ACME",
   ;          :widget-code   "Dynamite",
   ;          :description   "Boom!",
   ;          :wtc           "c40"}
   ;         {:producer-code "Cyberdyne",
   ;          :widget-code   "Model-101",
   ;          :description   "Resistance Infiltrator",
   ;          :wtc           "t800"}
   ;         {:producer-code "Cyberdyne",
   ;          :widget-code   "Model-201",
   ;          :description   "Mimetic polyalloy",
   ;          :wtc           "t1000"}
   ;         {:producer-code "Cyberdyne",
   ;          :widget-code   "Model-102",
   ;          :description   "Resistance Infiltrator",
   ;          :wtc           "t800"}])
   ;      (let [results-normalized (mapv (fn [result-map]
   ;                                       [(grab :producer-code result-map)
   ;                                        (grab :widget-code result-map)
   ;                                        (grab :description result-map)])
   ;                                 results-filtered)
   ;            normalized-desired [["Cyberdyne" "Model-101" "Resistance Infiltrator"]
   ;                                ["Cyberdyne" "Model-102" "Resistance Infiltrator"]
   ;                                ["Cyberdyne" "Model-201" "Mimetic polyalloy"]
   ;                                ["ACME" "Dynamite" "Boom!"]]]
   ;        (is-set= results-normalized normalized-desired)))))
   ;
   ;(dotest
   ;  (td/with-tdb (td/new-tdb)
   ;    (let [person   {:name "jimmy"
   ;                    :preferred-address
   ;                          {:address1 "123 street ave"
   ;                           :address2 "apt 2"
   ;                           :city     "Townville"
   ;                           :state    "IN"
   ;                           :zip      "46203"}
   ;                    :other-addresses
   ;                          [{:address1 "432 street ave"
   ;                            :address2 "apt 7"
   ;                            :city     "Cityvillage"
   ;                            :state    "New York"
   ;                            :zip      "12345"}
   ;                           {:address1 "534 street ave"
   ;                            :address2 "apt 5"
   ;                            :city     "Township"
   ;                            :state    "IN"
   ;                            :zip      "46203"}]}
   ;
   ;          root-eid (td/add-edn person)
   ;          ]
   ;      (is-set= (distinct (td/query-maps [{:zip ?}]))
   ;        [{:zip "12345"}
   ;         {:zip "46203"}])
   ;
   ;      (is-set= (distinct (td/query-maps [{:zip ? :city ?}]))
   ;        [{:zip "46203", :city "Township"}
   ;         {:zip "46203", :city "Townville"}
   ;         {:zip "12345", :city "Cityvillage"}]))))
   ;
   ;(dotest
   ;  (td/with-tdb (td/new-tdb)
   ;    (let [people           [{:name      "jimmy"
   ;                             :addresses [{:address1  "123 street ave"
   ;                                          :address2  "apt 2"
   ;                                          :city      "Townville"
   ;                                          :state     "IN"
   ;                                          :zip       "46203"
   ;                                          :preferred true}
   ;                                         {:address1  "534 street ave",
   ;                                          :address2  "apt 5",
   ;                                          :city      "Township",
   ;                                          :state     "IN",
   ;                                          :zip       "46203"
   ;                                          :preferred false}
   ;                                         {:address1  "543 Other St",
   ;                                          :address2  "apt 50",
   ;                                          :city      "Town",
   ;                                          :state     "CA",
   ;                                          :zip       "86753"
   ;                                          :preferred false}]}
   ;                            {:name      "joel"
   ;                             :addresses [{:address1  "2026 park ave"
   ;                                          :address2  "apt 200"
   ;                                          :city      "Town"
   ;                                          :state     "CA"
   ;                                          :zip       "86753"
   ;                                          :preferred true}]}]
   ;
   ;          root-eid         (td/add-edn people)
   ;          results          (td/query-maps [{:name ? :addresses [{:address1 ? :zip "86753"}]}])
   ;          results-filtered (t/walk-with-parents results
   ;                             {:enter (fn [parents item]
   ;                                       (t/cond-it-> item
   ;                                         (map? item) (t/drop-if (fn [k v] (td/tmp-attr-kw? k))
   ;                                                       item)))}) ]
   ;      (is-set= results-filtered
   ;        [{:name "jimmy", :address1 "543 Other St"}
   ;         {:name "joel", :address1 "2026 park ave"}]) )))
   ;
   ;(dotest ; -focus
   ;  (td/eid-count-reset)
   ;  (td/with-tdb (td/new-tdb)
   ;    (let [data      {:people
   ;                             [{:name "jimmy" :id 1}
   ;                              ;{:name "joel" :id 2}
   ;                              ;{:name "tim" :id 3}
   ;                              ]
   ;                     :addrs
   ;                             {1 [{; :addr  "123 street ave"
   ;                                  ; :address2  "apt 2"
   ;                                  ;:city      "Townville"
   ;                                  ;:state     "IN"
   ;                                  :zip  "46203"
   ;                                  :pref true}
   ;                                 {; :addr  "534 street ave",
   ;                                  ; :address2  "apt 5",
   ;                                  ;:city      "Township",
   ;                                  ;:state     "IN",
   ;                                  :zip  "46203"
   ;                                  :pref false}]
   ;                              ;2 [{; :addr  "2026 park ave"
   ;                              ;    ; :address2  "apt 200"
   ;                              ;    ;:city      "Town"
   ;                              ;    ;:state     "CA"
   ;                              ;    :zip  "86753"
   ;                              ;    :pref true}]
   ;                              ;3 [{; :addr  "1448 street st"
   ;                              ;    ; :address2  "apt 1"
   ;                              ;    ;:city      "City"
   ;                              ;    ;:state     "WA"
   ;                              ;    :zip  "92456"
   ;                              ;    :pref true}]
   ;                              }
   ;                     :visits {1 [{:date "12-31-1900" :geo-loc {:zip "46203"}}]
   ;                              ;2 [{:date "1-1-1970" :geo-loc {:zip "12345"}}
   ;                              ;   {:date "1-1-1970" :geo-loc {:zip "86753"}}]
   ;                              ;3 [{:date "4-4-4444" :geo-loc {:zip "54221"}}
   ;                              ;   {:date "4-4-4444" :geo-loc {:zip "92456"}}]
   ;                              }}
   ;
   ;          root-eid  (td/add-edn data)
   ;          >>        (spyx-pretty td/*tdb*)
   ;          ;results-1 (td/query-maps [{:people [{:name ? :id id}]}])
   ;
   ;          ;>> (do (newline) (spyx-pretty results-1))
   ;          ;results-2 (td/query-maps [{:addrs {id [{:zip ? :pref true}]}}])
   ;
   ;          results-3 (td/query-triples [; #todo debug this
   ;                                       ;(search-triple eid-pers :name name)
   ;                                       (search-triple eid-pers :id id)
   ;
   ;                                       (search-triple eid-addrs id eid-addr-deets)
   ;                                       (search-triple eid-addr-deets idx-deet eid-addr-deet)
   ;                                       (search-triple eid-addr-deet :zip zip)
   ;                                       (search-triple eid-addr-deet :pref true)
   ;                                       ])
   ;          >> (do (newline) (spyx-pretty results-3))
   ;
   ;          ;results-filtered (t/walk-with-parents results
   ;          ;                   {:enter (fn [parents item]
   ;          ;                             (t/cond-it-> item
   ;          ;                               (map? item) (t/drop-if (fn [k v] (td/tmp-attr-kw? k))
   ;          ;                                             item)))})
   ;          ]
   ;      )))



 ))

