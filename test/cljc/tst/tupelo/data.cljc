;   Copyright (c) Alan Thompson. All rights reserved.
;   The use and distribution terms for this software are covered by the Eclipse Public License 1.0
;   (http://opensource.org/licenses/eclipse-1.0.php) which can be found in the file epl-v10.html at
;   the root of this distribution.  By using this software in any fashion, you are agreeing to be
;   bound by the terms of this license.  You must not remove this notice, or any other, from this
;   software.
(ns tst.tupelo.data
  ;---------------------------------------------------------------------------------------------------
  ;   https://code.thheller.com/blog/shadow-cljs/2019/10/12/clojurescript-macros.html
  ;   http://blog.fikesfarm.com/posts/2015-12-18-clojurescript-macro-tower-and-loop.html
  #?(:cljs (:require-macros
             [tupelo.core]
             [tupelo.data]
             [tupelo.testy]
             ; #todo finish dotest-focus for tupelo.testy
             ; #todo finish tupelo.testy => tupelo.test
             ))
  (:require
    [clojure.test] ; sometimes this is required - not sure why
    [clojure.string :as str]
    [schema.core :as s]
    [tupelo.testy :refer [deftest testing is dotest dotest-focus isnt is= isnt= is-set= is-nonblank=
                          throws? throws-not? define-fixture]]
    [tupelo.core :as t :refer [spy spyx spyxx spy-pretty spyx-pretty spyq unlazy let-spy with-spy-indent
                               only only2 forv glue grab nl keep-if drop-if ->sym xfirst xsecond xthird not-nil?
                               it-> fetch-in with-map-vals map-plain? append prepend
                               ]]
    [tupelo.data :as td :refer [with-tdb new-tdb eid-count-reset lookup match-triples match-triples->tagged search-triple
                                *tdb* ->Eid Eid? ->Idx Idx? ->Prim Prim? ->Param Param?
                                ]]
    [tupelo.data.index :as index]
    [tupelo.tag :as tt :refer [IVal ITag ITagMap ->tagmap <tag <val]]
    )
  #?(:clj (:import [tupelo.data Eid Idx Prim Param]))
  )


#?(:cljs (enable-console-print!))

; #todo  need example of transform non-prim keys for map/set into canonical DS

(def dashes :---------------------------------------------------------------------------------------------------)
;-----------------------------------------------------------------------------
(comment  ; #todo be able to process this data & delete unwise users
  (def age-of-wisdom 30)
  (def customers
    [{:customer-id 1
      :plants      [{:plant-id  1
                     :employees [{:name "Alice" :age 35 :sex "F"}
                                 {:name "Bob" :age 25 :sex "M"}]}
                    {:plant-id  2
                     :employees []}]}
     {:customer-id 2}]))

;-----------------------------------------------------------------------------
(def skynet-widgets [{:basic-info   {:producer-code "Cyberdyne"}
                      :widgets      [{:widget-code      "Model-101"
                                      :widget-type-code "t800"}
                                     {:widget-code      "Model-102"
                                      :widget-type-code "t800"}
                                     {:widget-code      "Model-201"
                                      :widget-type-code "t1000"}]
                      :widget-types [{:widget-type-code "t800"
                                      :description      "Resistance Infiltrator"}
                                     {:widget-type-code "t1000"
                                      :description      "Mimetic polyalloy"}]}
                     {:basic-info   {:producer-code "ACME"}
                      :widgets      [{:widget-code      "Dynamite"
                                      :widget-type-code "c40"}]
                      :widget-types [{:widget-type-code "c40"
                                      :description      "Boom!"}]}])

(def users-and-accesses {:people [{:name "jimmy" :id 1}
                                  {:name "joel" :id 2}
                                  {:name "tim" :id 3}]
                         :addrs  {1 [{:addr     "123 street ave"
                                      :address2 "apt 2"
                                      :city     "Townville"
                                      :state    "IN"
                                      :zip      "11201"
                                      :pref     true}
                                     {:addr     "534 street ave",
                                      :address2 "apt 5",
                                      :city     "Township",
                                      :state    "IN",
                                      :zip      "00666"
                                      :pref     false}]
                                  2 [{:addr     "2026 park ave"
                                      :address2 "apt 200"
                                      :city     "Town"
                                      :state    "CA"
                                      :zip      "11753"
                                      :pref     true}]
                                  3 [{:addr     "1448 street st"
                                      :address2 "apt 1"
                                      :city     "City"
                                      :state    "WA"
                                      :zip      "11456"
                                      :pref     true}]}
                         :visits {1 [{:date "12-25-1900" :geo-loc {:zip "11201"}}
                                     {:date "12-31-1900" :geo-loc {:zip "00666"}}]
                                  2 [{:date "1-1-1970" :geo-loc {:zip "12345"}}
                                     {:date "2-1-1970" :geo-loc {:zip "11753"}}]
                                  3 [{:date "4-4-4444" :geo-loc {:zip "54221"}}
                                     {:date "5-4-4444" :geo-loc {:zip "11456"}}]}})

;-----------------------------------------------------------------------------
(dotest
  (is= true (and true))
  (is= [true] (cons true []))
  (is= true (every? t/truthy? (cons true []))))

(dotest
  (let [eid5 (->Eid 5)]
    (is (satisfies? ITagMap eid5))

    (is= 5 (<val eid5))
    (is= (type eid5) tupelo.data.Eid)
    (is= (instance? tupelo.data.Eid eid5) true)
    (is (Eid? eid5))
    (is= (td/walk-compact eid5) {:eid 5})
    (is= eid5 (td/coerce->Eid 5))
    (is= eid5 (td/coerce->Eid eid5))
    (is= eid5 (td/coerce->Eid {:eid 5}))
    (throws? (td/coerce->Eid {:idx 5}))
    (throws? (td/coerce->Eid :a)))
  (let [idx5 (->Idx 5)]
    (is= 5 (<val idx5))
    (is= (type idx5) tupelo.data.Idx)
    (is= (instance? tupelo.data.Idx idx5) true)
    (is (Idx? idx5))
    (is= (td/walk-compact idx5) {:idx 5})
    (is= idx5 (td/coerce->Idx 5))
    (is= idx5 (td/coerce->Idx idx5))
    (is= idx5 (td/coerce->Idx {:idx 5}))
    (throws? (td/coerce->Idx {:eid 5}))
    (throws? (td/coerce->Idx :a)))
  (let [prim5 (->Prim 5)]
    (is= 5 (<val prim5))
    (is= (type prim5) tupelo.data.Prim)
    (is= (instance? tupelo.data.Prim prim5) true)
    (is (Prim? prim5))
    (is= (td/walk-compact prim5) {:prim 5})
    (is= prim5 (td/coerce->Prim 5))
    (is= prim5 (td/coerce->Prim prim5))
    (is= prim5 (td/coerce->Prim {:prim 5}))
    (is= (->Prim :a) (td/coerce->Prim :a))
    (is= (->Prim "abc") (td/coerce->Prim "abc"))
    (throws? (td/coerce->Prim {:idx 5})))
  (let [param5 (->Param 5)]
    (is= 5 (<val param5))
    (is= (type param5) tupelo.data.Param)
    (is= (instance? tupelo.data.Param param5) true)
    (is (Param? param5))
    (is= (td/walk-compact param5) {:param 5})))

;-----------------------------------------------------------------------------
(dotest
  (let [arr-1 (vec (range 3))
        im    (td/array->tagidx-map arr-1)
        arr-2 (td/tagidx-map->array im)]
    (is= (td/walk-compact im)
      {{:idx 0} 0
       {:idx 1} 1
       {:idx 2} 2})
    (is= arr-1 arr-2))
  (let [ok  {(->Idx 0) 0
             (->Idx 1) 1
             (->Idx 2) 2}
        bad {(->Idx 0) 0
             ; missing idx=1
             (->Idx 2) 2}]
    (is= (td/tagidx-map->array ok)
      [0 1 2])
    (throws? (td/tagidx-map->array bad))))

;-----------------------------------------------------------------------------

(dotest
  (let [ss123 (t/it-> (index/empty-index)
                (conj it [1 :a])
                (conj it [3 :a])
                (conj it [2 :a]))
        ss13  (disj ss123 [2 :a])]
    (is= ss123 #{[1 :a] [2 :a] [3 :a]})
    (is= (vec ss123) [[1 :a] [2 :a] [3 :a]])
    (is= ss13 #{[1 :a] [3 :a]}))

  (is (map? (->Eid 3)))
  (is (map? {:a 1}))
  (is (record? (->Idx 3)))
  (isnt (record? {:a 1}))

  ; Leaf and entity-id records sort separately in the index. Eid sorts first since the type name
  ; `tupelo.data.Eid` sorts before `tupelo.data.Leaf`

  (let [idx (-> (index/empty-index)

              (index/add-entry [1 3])
              (index/add-entry [1 (->Eid 3)])
              (index/add-entry [1 1])
              (index/add-entry [1 (->Eid 1)])
              (index/add-entry [1 2])
              (index/add-entry [1 (->Eid 2)])

              (index/add-entry [0 3])
              (index/add-entry [0 (->Eid 3)])
              (index/add-entry [0 1])
              (index/add-entry [0 (->Eid 1)])
              (index/add-entry [0 2])
              (index/add-entry [0 (->Eid 2)]))]
    (is= (td/walk-compact (vec idx))
      [[0 1]
       [0 2]
       [0 3]
       [0 {:eid 1}]
       [0 {:eid 2}]
       [0 {:eid 3}]
       [1 1]
       [1 2]
       [1 3]
       [1 {:eid 1}]
       [1 {:eid 2}]
       [1 {:eid 3}]])))

(dotest
  (td/with-tdb (td/new-tdb)
    (td/eid-count-reset)
    (is= (deref *tdb*)
      {:eid-type {} :eid-watchers {}
       :idx-eav  #{} :idx-vea #{} :idx-ave #{}})
    ;(newline)
    ;(spyx-pretty :dry-run
    ;  (td/with-entity-watchers-impl  (quote [
    ;                                         (spyx :add-entity-edn *tdb-deltas*)
    ;                                         (<val (add-entity-edn-impl {:a 1})) ]) ) )
    ;(newline)
    (let [edn-val  {:a 1}
          root-eid (td/add-entity-edn edn-val)
          ]
      (is= root-eid 1001)
      (is= (td/walk-compact (deref *tdb*))
        {:eid-type {{:eid 1001} :map}, :eid-watchers {}
         :idx-ave  #{[{:prim :a} {:prim 1} {:eid 1001}]},
         :idx-eav  #{[{:eid 1001} {:prim :a} {:prim 1}]},
         :idx-vea  #{[{:prim 1} {:eid 1001} {:prim :a}]}})
      (is= edn-val (td/eid->edn root-eid))
      )
    ))

(dotest
  (with-tdb (new-tdb)
    (eid-count-reset)
    (let [edn-val  {:a 1 :b 2}
          root-eid (td/add-entity-edn edn-val)]
      (is= 1001 root-eid)
      (is= (td/walk-compact (deref *tdb*))
        {:eid-type {{:eid 1001} :map}, :eid-watchers {}
         :idx-ave  #{[{:prim :a} {:prim 1} {:eid 1001}]
                     [{:prim :b} {:prim 2} {:eid 1001}]},
         :idx-eav  #{[{:eid 1001} {:prim :a} {:prim 1}]
                     [{:eid 1001} {:prim :b} {:prim 2}]},
         :idx-vea  #{[{:prim 1} {:eid 1001} {:prim :a}]
                     [{:prim 2} {:eid 1001} {:prim :b}]}})
      (is= edn-val (td/eid->edn root-eid)))))

(dotest
  (with-tdb (new-tdb)
    (eid-count-reset)
    (let [edn-val  {:a 1 :b 2 :c {:d 4}}
          root-eid (td/add-entity-edn edn-val)]
      (is= 1001 root-eid)
      (is= (td/walk-compact (deref *tdb*))
        {:eid-type     {{:eid 1001} :map, {:eid 1002} :map},
         :eid-watchers {}
         :idx-ave      #{[{:prim :a} {:prim 1} {:eid 1001}]
                         [{:prim :b} {:prim 2} {:eid 1001}]
                         [{:prim :c} {:eid 1002} {:eid 1001}]
                         [{:prim :d} {:prim 4} {:eid 1002}]},
         :idx-eav      #{[{:eid 1001} {:prim :a} {:prim 1}]
                         [{:eid 1001} {:prim :b} {:prim 2}]
                         [{:eid 1001} {:prim :c} {:eid 1002}]
                         [{:eid 1002} {:prim :d} {:prim 4}]},
         :idx-vea      #{[{:eid 1002} {:eid 1001} {:prim :c}]
                         [{:prim 1} {:eid 1001} {:prim :a}]
                         [{:prim 2} {:eid 1001} {:prim :b}]
                         [{:prim 4} {:eid 1002} {:prim :d}]}})
      (is= edn-val (td/eid->edn root-eid)))))

(dotest
  (with-tdb (new-tdb)
    (eid-count-reset)
    (let [edn-val  [1 2 3]
          root-eid (td/add-entity-edn edn-val)]
      (is= (td/walk-compact (deref *tdb*))
        {:eid-type     {{:eid 1001} :array},
         :eid-watchers {}
         :idx-ave      #{[{:idx 0} {:prim 1} {:eid 1001}] [{:idx 1} {:prim 2} {:eid 1001}]
                         [{:idx 2} {:prim 3} {:eid 1001}]},
         :idx-eav      #{[{:eid 1001} {:idx 0} {:prim 1}]
                         [{:eid 1001} {:idx 1} {:prim 2}]
                         [{:eid 1001} {:idx 2} {:prim 3}]},
         :idx-vea      #{[{:prim 1} {:eid 1001} {:idx 0}] [{:prim 2} {:eid 1001} {:idx 1}]
                         [{:prim 3} {:eid 1001} {:idx 2}]}})
      (is= edn-val (td/eid->edn root-eid)))))

(dotest
  (with-tdb (new-tdb)
    (eid-count-reset)
    (let [edn-val  {:a 1 :b 2 :c [10 11 12]}
          root-eid (td/add-entity-edn edn-val)]
      (is= (td/walk-compact (deref *tdb*))
        {:eid-type     {{:eid 1001} :map, {:eid 1002} :array},
         :eid-watchers {}
         :idx-ave      #{[{:idx 0} {:prim 10} {:eid 1002}] [{:idx 1} {:prim 11} {:eid 1002}]
                         [{:idx 2} {:prim 12} {:eid 1002}] [{:prim :a} {:prim 1} {:eid 1001}]
                         [{:prim :b} {:prim 2} {:eid 1001}]
                         [{:prim :c} {:eid 1002} {:eid 1001}]},
         :idx-eav      #{[{:eid 1001} {:prim :a} {:prim 1}]
                         [{:eid 1001} {:prim :b} {:prim 2}]
                         [{:eid 1001} {:prim :c} {:eid 1002}]
                         [{:eid 1002} {:idx 0} {:prim 10}]
                         [{:eid 1002} {:idx 1} {:prim 11}]
                         [{:eid 1002} {:idx 2} {:prim 12}]},
         :idx-vea      #{[{:eid 1002} {:eid 1001} {:prim :c}]
                         [{:prim 1} {:eid 1001} {:prim :a}]
                         [{:prim 2} {:eid 1001} {:prim :b}] [{:prim 10} {:eid 1002} {:idx 0}]
                         [{:prim 11} {:eid 1002} {:idx 1}] [{:prim 12} {:eid 1002} {:idx 2}]}})
      (is= edn-val (td/eid->edn root-eid))
      (is= (td/eid->edn 1002) [10 11 12]))))

(dotest
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
        (td/add-entity-edn m))
      (is= (td/walk-compact @*tdb*) ; #todo error: missing :array
        {:eid-watchers {}
         :eid-type     {{:eid 1001} :map,
                        {:eid 1002} :map,
                        {:eid 1003} :map,
                        {:eid 1004} :map,
                        {:eid 1005} :map,
                        {:eid 1006} :map,
                        {:eid 1007} :map,
                        {:eid 1008} :map,
                        {:eid 1009} :map},
         :idx-ave      #{[{:prim :a} {:prim 1} {:eid 1001}]
                         [{:prim :a} {:prim 2} {:eid 1002}]
                         [{:prim :a} {:prim 3} {:eid 1003}]
                         [{:prim :b} {:prim 1} {:eid 1004}]
                         [{:prim :b} {:prim 2} {:eid 1005}]
                         [{:prim :b} {:prim 3} {:eid 1006}]
                         [{:prim :c} {:prim 1} {:eid 1007}]
                         [{:prim :c} {:prim 2} {:eid 1008}]
                         [{:prim :c} {:prim 3} {:eid 1009}]},
         :idx-eav      #{[{:eid 1001} {:prim :a} {:prim 1}]
                         [{:eid 1002} {:prim :a} {:prim 2}]
                         [{:eid 1003} {:prim :a} {:prim 3}]
                         [{:eid 1004} {:prim :b} {:prim 1}]
                         [{:eid 1005} {:prim :b} {:prim 2}]
                         [{:eid 1006} {:prim :b} {:prim 3}]
                         [{:eid 1007} {:prim :c} {:prim 1}]
                         [{:eid 1008} {:prim :c} {:prim 2}]
                         [{:eid 1009} {:prim :c} {:prim 3}]},
         :idx-vea      #{[{:prim 1} {:eid 1001} {:prim :a}]
                         [{:prim 1} {:eid 1004} {:prim :b}]
                         [{:prim 1} {:eid 1007} {:prim :c}]
                         [{:prim 2} {:eid 1002} {:prim :a}]
                         [{:prim 2} {:eid 1005} {:prim :b}]
                         [{:prim 2} {:eid 1008} {:prim :c}]
                         [{:prim 3} {:eid 1003} {:prim :a}]
                         [{:prim 3} {:eid 1006} {:prim :b}]
                         [{:prim 3} {:eid 1009} {:prim :c}]}})
      ;---------------------------------------------------------------------------------------------------
      (is= (td/walk-compact (lookup [(->Eid 1003) nil nil]))
        [[{:eid 1003} {:prim :a} {:prim 3}]])
      (is= (td/walk-compact (lookup [nil (->Prim :b) nil]))
        [[{:eid 1004} {:prim :b} {:prim 1}]
         [{:eid 1005} {:prim :b} {:prim 2}]
         [{:eid 1006} {:prim :b} {:prim 3}]])
      (is= (td/walk-compact (lookup [nil nil (->Prim 3)]))
        [[{:eid 1003} {:prim :a} {:prim 3}]
         [{:eid 1006} {:prim :b} {:prim 3}]
         [{:eid 1009} {:prim :c} {:prim 3}]])
      (is= (td/walk-compact (lookup [nil nil nil]))
        [[{:eid 1001} {:prim :a} {:prim 1}]
         [{:eid 1002} {:prim :a} {:prim 2}]
         [{:eid 1003} {:prim :a} {:prim 3}]
         [{:eid 1004} {:prim :b} {:prim 1}]
         [{:eid 1005} {:prim :b} {:prim 2}]
         [{:eid 1006} {:prim :b} {:prim 3}]
         [{:eid 1007} {:prim :c} {:prim 1}]
         [{:eid 1008} {:prim :c} {:prim 2}]
         [{:eid 1009} {:prim :c} {:prim 3}]])

      ;---------------------------------------------------------------------------------------------------
      (is= (td/walk-compact (lookup [nil (->Prim :a) (->Prim 3)]))
        [[{:eid 1003} {:prim :a} {:prim 3}]])
      (is= (td/walk-compact (lookup [(->Eid 1009) nil (->Prim 3)]))
        [[{:eid 1009} {:prim :c} {:prim 3}]])
      (is= (td/walk-compact (lookup [(->Eid 1005) (->Prim :b) nil]))
        [[{:eid 1005} {:prim :b} {:prim 2}]]))))

(dotest
  (with-tdb (new-tdb)
    (eid-count-reset)
    (let [edn-val  {:a 1 :b 2}
          root-eid (td/add-entity-edn edn-val)]
      (is= edn-val (td/eid->edn root-eid))
      (is= (td/walk-compact (deref *tdb*))
        {:eid-type {{:eid 1001} :map}, :eid-watchers {}
         :idx-ave  #{[{:prim :a} {:prim 1} {:eid 1001}]
                     [{:prim :b} {:prim 2} {:eid 1001}]},
         :idx-eav  #{[{:eid 1001} {:prim :a} {:prim 1}]
                     [{:eid 1001} {:prim :b} {:prim 2}]},
         :idx-vea  #{[{:prim 1} {:eid 1001} {:prim :a}]
                     [{:prim 2} {:eid 1001} {:prim :b}]}}))

    (let [triple-specs [[{:param :e} {:param :a} {:param :v}]]]
      (let [qpred (fn [env]
                    (with-map-vals env [v]
                      (odd? v)))]
        (is= (td/walk-compact (match-triples->tagged triple-specs))
          [{{:param :e} {:eid 1001},
            {:param :a} {:prim :a},
            {:param :v} {:prim 1}}
           {{:param :e} {:eid 1001},
            {:param :a} {:prim :b},
            {:param :v} {:prim 2}}])
        (is= (td/walk-compact (td/match-triples triple-specs))
          [{:e 1001, :a :a, :v 1}
           {:e 1001, :a :b, :v 2}])
        (is= (td/walk-compact (td/match-triples+pred triple-specs qpred))
          [{{:param :e} {:eid 1001},
            {:param :a} {:prim :a},
            {:param :v} {:prim 1}}])))
    ))


(dotest
  (with-tdb (new-tdb)
    (eid-count-reset)
    (let [edn-val  {:a 1 :b 2}
          root-eid (td/add-entity-edn edn-val)]
      (is= edn-val (td/eid->edn root-eid))
      (is= (td/walk-compact (deref *tdb*))
        {:eid-type {{:eid 1001} :map}, :eid-watchers {}
         :idx-ave  #{[{:prim :a} {:prim 1} {:eid 1001}]
                     [{:prim :b} {:prim 2} {:eid 1001}]},
         :idx-eav  #{[{:eid 1001} {:prim :a} {:prim 1}]
                     [{:eid 1001} {:prim :b} {:prim 2}]},
         :idx-vea  #{[{:prim 1} {:eid 1001} {:prim :a}]
                     [{:prim 2} {:eid 1001} {:prim :b}]}}))
    (let [triple-specs [[(->Param :x) (->Prim :a) (->Prim 1)]]]
      (let [qpred (fn [env]
                    (with-map-vals env [x]
                      (odd? x)))]
        (is= (td/walk-compact (match-triples->tagged triple-specs))
          [{{:param :x} {:eid 1001}}])
        (is= (td/walk-compact (td/match-triples+pred triple-specs qpred))
          [{{:param :x} {:eid 1001}}])))))

(dotest
  (with-tdb (new-tdb)
    (eid-count-reset)
    (let [edn-val  {:a 1
                    :b 1}
          root-eid (td/add-entity-edn edn-val)]
      (is= edn-val (td/eid->edn root-eid))
      (is= (td/walk-compact (deref *tdb*))
        {:eid-type {{:eid 1001} :map}, :eid-watchers {}
         :idx-ave  #{[{:prim :a} {:prim 1} {:eid 1001}]
                     [{:prim :b} {:prim 1} {:eid 1001}]},
         :idx-eav  #{[{:eid 1001} {:prim :a} {:prim 1}]
                     [{:eid 1001} {:prim :b} {:prim 1}]},
         :idx-vea  #{[{:prim 1} {:eid 1001} {:prim :a}]
                     [{:prim 1} {:eid 1001} {:prim :b}]}}))

    (let [search-spec [[(->Param :x) (->Prim :a) (->Prim 1)]]]
      (is= (td/walk-compact (match-triples->tagged search-spec))
        [{{:param :x} {:eid 1001}}])
      (is= (td/walk-compact (match-triples search-spec))
        [{:x 1001}]))

    (let [search-spec [[(->Param :x) (->Prim :b) (->Prim 1)]]]
      (is= (td/walk-compact (match-triples->tagged search-spec))
        [{{:param :x} {:eid 1001}}])
      (is= (td/walk-compact (match-triples search-spec))
        [{:x 1001}]))

    (let [search-spec [[{:param :x} (->Param :y) (->Prim 1)]]] ; both ways work
      (is= (td/walk-compact (match-triples->tagged search-spec))
        [{{:param :x} {:eid 1001}, {:param :y} {:prim :a}}
         {{:param :x} {:eid 1001}, {:param :y} {:prim :b}}])
      (is= (td/walk-compact (match-triples search-spec))
        [{:x 1001, :y :a}
         {:x 1001, :y :b}]))))

(dotest
  (with-tdb (new-tdb)
    (eid-count-reset)
    (let [edn-val  {:a {:b 2}}
          root-eid (td/add-entity-edn edn-val)]
      (is= (td/walk-compact (deref *tdb*))
        {:eid-type {{:eid 1001} :map, {:eid 1002} :map}, :eid-watchers {}
         :idx-ave  #{[{:prim :a} {:eid 1002} {:eid 1001}]
                     [{:prim :b} {:prim 2} {:eid 1002}]},
         :idx-eav  #{[{:eid 1001} {:prim :a} {:eid 1002}]
                     [{:eid 1002} {:prim :b} {:prim 2}]},
         :idx-vea  #{[{:eid 1002} {:eid 1001} {:prim :a}]
                     [{:prim 2} {:eid 1002} {:prim :b}]}})
      ; (prn :-----------------------------------------------------------------------------)
      ; compound search
      (is= (td/walk-compact (match-triples->tagged [[{:param :x} (->Prim :a) {:param :y}]
                                                    [{:param :y} (->Prim :b) (->Prim 2)]]))
        [{{:param :x} {:eid 1001}
          {:param :y} {:eid 1002}}])
      (is= (td/walk-compact (match-triples [[(->Param :x) (->Prim :a) (->Param :y)]
                                            [(->Param :y) (->Prim :b) (->Prim 2)]]))
        [{:x 1001 :y 1002}])

      ; (prn :-----------------------------------------------------------------------------)
      ; failing search
      (is= [] (match-triples->tagged [[{:param :x} (->Prim :a) {:param :y}]
                                      [{:param :y} (->Prim :b) (->Prim 99)]]))
      (is= [] (match-triples [[{:param :x} (->Prim :a) {:param :y}]
                              [{:param :y} (->Prim :b) (->Prim 99)]]))
      ; (prn :-----------------------------------------------------------------------------)
      ; wildcard search - match all
      (is= (td/walk-compact (match-triples->tagged [[{:param :x} {:param :y} {:param :z}]]))
        [{{:param :x} {:eid 1001}
          {:param :y} {:prim :a}
          {:param :z} {:eid 1002}}
         {{:param :x} {:eid 1002}
          {:param :y} {:prim :b}
          {:param :z} {:prim 2}}])
      (is= (td/walk-compact (match-triples [[(->Param :x) (->Param :y) (->Param :z)]]))
        [{:x 1001, :y :a, :z 1002}
         {:x 1002, :y :b, :z 2}]))))

(comment  ; #todo broken, fix this
  (dotest
    (is= (td/search-triple-impl (quote [:a :b :c]))
      '(tupelo.data/search-triple-fn (quote [:a :b :c])))
    (is= (td/search-triple-impl (quote [a b c]))
      '(tupelo.data/search-triple-fn (quote [a b c])))

    (is= (td/search-triple 123 :color "Joey")
      [(->Eid 123) (->Prim :color) (->Prim "Joey")])))

(dotest
  (with-tdb (new-tdb)
    (eid-count-reset)
    (let [edn-val     {:a {:b 2}}
          root-eid    (td/add-entity-edn edn-val)
          search-spec [(search-triple x :a y)
                       (search-triple y :b 2)]]
      (is= (td/walk-compact (match-triples search-spec))
        [{:x 1001, :y 1002}])))

  (with-tdb (new-tdb)
    (eid-count-reset)
    (let [edn-val     {:a {:b 2}}
          root-eid    (td/add-entity-edn edn-val)
          search-spec [(search-triple y :b 2)
                       (search-triple x :a y)]]
      (is= (td/walk-compact (match-triples search-spec))
        [{:y 1002, :x 1001}])))

  (with-tdb (new-tdb)
    (eid-count-reset)
    (let [edn-val     {:a {:b 2}}
          root-eid    (td/add-entity-edn edn-val)
          search-spec [(search-triple x :a y) (search-triple y :b 99)]]
      (is= [] (match-triples search-spec))))

  (with-tdb (new-tdb)
    (eid-count-reset)
    (let [edn-val     {:a {:b 2}}
          root-eid    (td/add-entity-edn edn-val)
          search-spec [(search-triple y :b 99) (search-triple x :a y)]]
      (is= [] (match-triples search-spec)))))

(dotest
  (let [map-triples (td/match->triples (quote [{:eid x :map y}
                                               {:eid y :a a}]))]
    (is= (td/walk-compact map-triples)
      [[{:param :x} {:prim :map} {:param :y}]
       [{:param :y} {:prim :a} {:param :a}]]))

  (let [map-triples (td/match->triples (quote [{:eid ? :map y}]))]
    (is= (td/walk-compact map-triples)
      [[{:param :eid} {:prim :map} {:param :y}]]))

  (let [map-triples (td/match->triples (quote [{:eid ? :map y}
                                               {:eid y :a a}]))]
    (is= (td/walk-compact map-triples)
      [[{:param :eid} {:prim :map} {:param :y}]
       [{:param :y} {:prim :a} {:param :a}]]))

  (throws? (td/match->triples (quote [{:eid ? :map y}
                                      {:eid ? :a a}]))))

(dotest
  (is (td/param-tmp-eid? (->Param :tmp-eid-99999)))
  (is (td/tmp-attr-kw? :tmp-attr-99999)))

;---------------------------------------------------------------------------------------------------
(def nested-edn-val (glue (sorted-map)
                      {:num     5
                       :map     {:a 1 :b 2}
                       :hashmap {:a 21 :b 22}
                       :vec     [5 6 7]
                       ;  :set #{3 4}  ; #todo add sets
                       :str     "hello"
                       :kw      :nothing}))

(dotest
  (td/with-tdb (td/new-tdb)
    (td/eid-count-reset)
    (let [root-hid (td/add-entity-edn nested-edn-val)]
      ; (spyx-pretty (grab :idx-eav (deref *tdb*)))
      (is= nested-edn-val (td/eid->edn root-hid))
      (when false
        (let [idx-eav (vec (grab :idx-eav @*tdb*))]
          (spyx-pretty (td/walk-compact idx-eav))
          ;result
          (comment
            (td/walk-compact idx-eav) =>
            [[{:eid 1001} {:prim :hashmap} {:eid 1002}]
             [{:eid 1001} {:prim :kw} {:prim :nothing}]
             [{:eid 1001} {:prim :map} {:eid 1003}]
             [{:eid 1001} {:prim :num} {:prim 5}]
             [{:eid 1001} {:prim :str} {:prim "hello"}]
             [{:eid 1001} {:prim :vec} {:eid 1004}]
             [{:eid 1002} {:prim :a} {:prim 21}]
             [{:eid 1002} {:prim :b} {:prim 22}]
             [{:eid 1003} {:prim :a} {:prim 1}]
             [{:eid 1003} {:prim :b} {:prim 2}]
             [{:eid 1004} {:idx 0} {:prim 5}]
             [{:eid 1004} {:idx 1} {:prim 6}]
             [{:eid 1004} {:idx 2} {:prim 7}]])))

      (when true
        (is= (td/walk-compact (match-triples [(search-triple e :num v)]))
          [{:e 1001, :v 5}])
        (is= (td/walk-compact (match-triples [(search-triple e a "hello")]))
          [{:e 1001, :a :str}])
        (is= (td/walk-compact (match-triples [(search-triple e a 7)]))
          [{:e 1004, :a 2}])))))

(dotest
  (td/with-tdb (td/new-tdb)
    (td/eid-count-reset)
    (let [root-hid (td/add-entity-edn nested-edn-val)]
      ; (spyx-pretty (grab :idx-eav (deref *tdb*)))
      (is= nested-edn-val (td/eid->edn root-hid))
      (is= nested-edn-val (td/eid->edn 1001))
      (is= (td/eid->edn 1003) {:a 1 :b 2})
      (comment ; #todo API:  output should look like
        {:x 1001 :y 1002 :a 1})

      (let [r1 (td/walk-compact (only (td/match-triples [(search-triple e i 7)])))]
        (is= r1 {:e 1004, :i 2})
        (is= (td/eid->edn 1004) [5 6 7])))))

(dotest
  (td/with-tdb (td/new-tdb)
    (td/eid-count-reset)
    (let [root-hid (td/add-entity-edn nested-edn-val)]
      ; (spyx-pretty (grab :idx-eav (deref *tdb*)))
      (is= nested-edn-val (td/eid->edn root-hid)))))

(dotest
  (td/with-tdb (td/new-tdb)
    (td/eid-count-reset)
    ; (spyx-pretty nested-edn-val)
    (let [root-hid (td/add-entity-edn nested-edn-val)]
      ; (spyx-pretty (grab :idx-eav (deref *tdb*)))
      (is= nested-edn-val (td/eid->edn root-hid))

      (binding [td/*autosyms-seen* (atom #{})]
        (is= (symbol "a") (td/autosym-resolve :a (quote ?)))
        (throws? (td/autosym-resolve :a (quote ?)))) ;attempted duplicate throws

      (throws? (td/exclude-reserved-identifiers {:a {:tmp-eid-123 :x}}))
      (throws? (td/exclude-reserved-identifiers (quote {:a {:x [1 2 3 tmp-eid-123 4 5 6]}})))

      (is= (td/untag-match-results [{(->Param :a) 1}])
        [{:a 1}])
      (is= (td/walk-compact
             (td/untag-match-results [{(->Param :x) (->Eid 1001),
                                       (->Param :y) (->Eid 1002),
                                       (->Param :a) 1}]))
        [{:x 1001, :y 1002, :a 1}])

      (is= (td/walk-compact
             (td/untag-match-results [{(->Param :e) (->Eid 1003)
                                       (->Param :i) 2}]))
        [{:e 1003, :i 2}])

      (is= (td/match [{:map     {:a a1}
                       :hashmap {:a a2}}])
        [{:a1 1
          :a2 21}])

      (is-set= (td/match [{kk {:a ?}}]) ; Awesome!  Found both solutions!
        [{:kk :map, :a 1}
         {:kk :hashmap, :a 21}])

      (is= (only (td/match [{:num ?}])) {:num 5})
      (is= (only (td/walk-compact
                   (td/match [{:eid ? :num ?}]))) {:eid 1001, :num 5})
      (is= (only (td/walk-compact
                   (td/match [{:eid ? :num num}]))) {:eid 1001, :num 5})
      )))

; #todo need to convert all from compile-time macros to runtime functions
(dotest
  (td/eid-count-reset)
  (td/with-tdb (td/new-tdb)
    ; (td/eid-count-reset)
    (let [hospital             {:hospital "Hans Jopkins"
                                :staff    {10 {:first-name "John"
                                               :last-name  "Doe"
                                               :salary     40000
                                               :position   :resident}
                                           11 {:first-name "Joey"
                                               :last-name  "Buttafucco"
                                               :salary     42000
                                               :position   :resident}
                                           20 {:first-name "Jane"
                                               :last-name  "Deer"
                                               :salary     100000
                                               :position   :attending}
                                           21 {:first-name "Dear"
                                               :last-name  "Jane"
                                               :salary     102000
                                               :position   :attending}
                                           30 {:first-name "Sam"
                                               :last-name  "Waterman"
                                               :salary     0
                                               :position   :volunteer}
                                           31 {:first-name "Sammy"
                                               :last-name  "Davis"
                                               :salary     0
                                               :position   :volunteer}}}
          root-hid             (td/add-entity-edn hospital)
          nm-sal-all           (td/match [{:first-name ? :salary ?}])
          nm-sal-attending     (td/match [{:first-name ? :salary ? :position :attending}])
          nm-sal-resident      (td/match [{:first-name ? :salary ? :position :resident}])
          nm-sal-volunteer     (td/match [{:first-name ? :salary ? :position :volunteer}])
          average              (fn [vals] (/ (reduce + 0 vals)
                                            (count vals)))
          salary-avg-attending (average (mapv :salary nm-sal-attending))
          salary-avg-resident  (average (mapv :salary nm-sal-resident))
          salary-avg-volunteer (average (mapv :salary nm-sal-volunteer))
          ]
      (is-set= nm-sal-all
        [{:first-name "Joey", :salary 42000}
         {:first-name "Dear", :salary 102000}
         {:first-name "Jane", :salary 100000}
         {:first-name "John", :salary 40000}
         {:first-name "Sam", :salary 0}
         {:first-name "Sammy", :salary 0}])
      (is-set= nm-sal-attending
        [{:first-name "Dear", :salary 102000}
         {:first-name "Jane", :salary 100000}])

      (is= salary-avg-attending 101000)
      (is= salary-avg-resident 41000)
      (is= salary-avg-volunteer 0))))

(dotest
  (td/with-tdb (td/new-tdb)
    (td/eid-count-reset)
    (let [edn-val  {:aa [1 2 3]
                    :bb [2 3 4]
                    :cc [3 4 5 6]}
          root-eid (td/add-entity-edn edn-val)]
      (comment
        (spyx-pretty @*tdb*)
        (spyx-pretty (td/db-pretty (deref td/*tdb*)))
        {:eid-type {{:eid 1001} :map,
                    {:eid 1002} :array,
                    {:eid 1003} :array,
                    {:eid 1004} :array},
         :idx-ave  #{[{:idx 0} 1 {:eid 1002}] [{:idx 0} 2 {:eid 1003}]
                     [{:idx 0} 3 {:eid 1004}] [{:idx 1} 2 {:eid 1002}]
                     [{:idx 1} 3 {:eid 1003}] [{:idx 1} 4 {:eid 1004}]
                     [{:idx 2} 3 {:eid 1002}] [{:idx 2} 4 {:eid 1003}]
                     [{:idx 2} 5 {:eid 1004}] [{:idx 3} 6 {:eid 1004}]
                     [:aa {:eid 1002} {:eid 1001}] [:bb {:eid 1003} {:eid 1001}]
                     [:cc {:eid 1004} {:eid 1001}]},
         :idx-eav  #{[{:eid 1001} :aa {:eid 1002}]
                     [{:eid 1001} :bb {:eid 1003}]
                     [{:eid 1001} :cc {:eid 1004}]
                     [{:eid 1002} {:idx 0} 1]
                     [{:eid 1002} {:idx 1} 2]
                     [{:eid 1002} {:idx 2} 3]
                     [{:eid 1003} {:idx 0} 2]
                     [{:eid 1003} {:idx 1} 3]
                     [{:eid 1003} {:idx 2} 4]
                     [{:eid 1004} {:idx 0} 3]
                     [{:eid 1004} {:idx 1} 4]
                     [{:eid 1004} {:idx 2} 5]
                     [{:eid 1004} {:idx 3} 6]},
         :idx-vae  #{[{:eid 1002} :aa {:eid 1001}] [{:eid 1003} :bb {:eid 1001}]
                     [{:eid 1004} :cc {:eid 1001}] [1 {:idx 0} {:eid 1002}]
                     [2 {:idx 0} {:eid 1003}] [2 {:idx 1} {:eid 1002}]
                     [3 {:idx 0} {:eid 1004}] [3 {:idx 1} {:eid 1003}]
                     [3 {:idx 2} {:eid 1002}] [4 {:idx 1} {:eid 1004}]
                     [4 {:idx 2} {:eid 1003}] [5 {:idx 2} {:eid 1004}]
                     [6 {:idx 3} {:eid 1004}]}})

      (let [st    (td/search-triple-fn [(quote ?) (->Idx 1) 2])
            ; >> (spyx st)
            found (td/match-triples [st])]
        (is= (td/eid->edn root-eid) {:aa [1 2 3]
                                     :bb [2 3 4]
                                     :cc [3 4 5 6]})
        (is= (td/eid->edn (val (t/only2 found)))
          [1 2 3]))

      (let [found    (td/match-triples [(td/search-triple eid idx 3)])
            entities (mapv #(td/eid->edn (grab :eid %)) found)]
        (is= (td/walk-compact found)
          [{:eid 1002, :idx 2}
           {:eid 1003, :idx 1}
           {:eid 1004, :idx 0}])
        (is-set= entities [[1 2 3] [2 3 4] [3 4 5 6]]))
      (is= [1 2 3]
        (it-> (td/match-triples [(td/search-triple eid {:idx 2} 3)])
          (only it)
          (fetch-in it [:eid])
          (td/eid->edn it)))
      (is= [2 3 4]
        (it-> (td/match-triples [(td/search-triple eid 1 3)]) ; raw integer is like idx-literal (macro quotes forms!)
          (only it)
          (fetch-in it [:eid])
          (td/eid->edn it)))
      (is= [3 4 5 6]
        (it-> (td/match-triples [(td/search-triple eid {:idx 0} 3)])
          (only it)
          (fetch-in it [:eid])
          (td/eid->edn it))))))

(dotest
  (td/with-tdb (td/new-tdb)
    (let [edn-val  {:a 1 :b 2}
          root-eid (td/add-entity-edn edn-val)]
      (is= edn-val (td/eid->edn root-eid))))

  (td/with-tdb (td/new-tdb)
    (let [edn-val  [1 2 3]
          root-eid (td/add-entity-edn edn-val)]
      (is= edn-val (td/eid->edn root-eid))))

  (td/with-tdb (td/new-tdb)
    (let [edn-val  #{1 2 3}
          root-eid (td/add-entity-edn edn-val)]
      (is= edn-val (td/eid->edn root-eid))))

  (td/with-tdb (td/new-tdb)
    (let [edn-val  {:val "hello"}
          root-eid (td/add-entity-edn edn-val)]
      (is= edn-val (td/eid->edn root-eid))))

  (td/with-tdb (td/new-tdb)
    (let [data-val {:a [{:b 2}
                        {:c 3}
                        {:d 4}]
                    :e {:f 6}
                    :g :green
                    :h "hotel"
                    :i 1}
          root-eid (td/add-entity-edn data-val)]
      (is= data-val (td/eid->edn root-eid)))))

(dotest
  (td/with-tdb (td/new-tdb)
    (let [edn-val    #{1 2 3}
          root-hid   (td/add-entity-edn edn-val)
          ; >> (spyx-pretty (deref td/*tdb*))
          edn-result (td/eid->edn root-hid)]
      (is (set? edn-result)) ; ***** Sets are coerced to vectors! *****
      (is-set= [1 2 3] edn-result)))
  (td/with-tdb (td/new-tdb)
    (let [edn-val  #{:a 1 :b 2}
          root-hid (td/add-entity-edn edn-val)]
      (is= edn-val (td/eid->edn root-hid))))
  (td/with-tdb (td/new-tdb)
    (let [edn-val  {:a 1 :b #{1 2 3}}
          root-hid (td/add-entity-edn edn-val)]
      (is= edn-val (td/eid->edn root-hid)))))

(dotest
  (td/eid-count-reset)
  (td/with-tdb (td/new-tdb)
    (let [data     {:a [{:b 2}
                        {:c 3}
                        {:d 4}]
                    :e {:f 6}
                    :g :green
                    :h "hotel"
                    :i 1}
          root-eid (td/add-entity-edn data)]
      (is= 1001 root-eid)
      (let [found (td/match [{:a ?}])]
        (is= (td/eid->edn (val (only2 found)))
          [{:b 2} {:c 3} {:d 4}]))

      (let [found (td/match [{:a e1}
                             {:eid e1 {:idx 0} val}])]
        (is= (td/eid->edn (:val (only found)))
          {:b 2}))

      (let [found (td/match-triples [(td/search-triple e1 :a e2)
                                     (td/search-triple e2 {:idx 2} e3)])]
        (is= (td/eid->edn (grab :e3 (only found))) {:d 4}))

      (let [found (td/walk-compact
                    (td/match-triples [(td/search-triple e1 a1 e2)
                                       (td/search-triple e2 a2 e3)
                                       (td/search-triple e3 a3 4)]))]
        (is= found
          [{:a1 :a
            :a2 2
            :a3 :d
            :e1 1001
            :e2 1002
            :e3 1005}])
        (is= data (td/eid->edn (fetch-in (only found) [:e1])))))))

(dotest
  (td/with-tdb (td/new-tdb)
    (td/eid-count-reset)
    (let [data          [{:a 1 :b :first}
                         {:a 2 :b :second}
                         {:a 3 :b :third}
                         {:a 4 :b "fourth"}
                         {:a 5 :b "fifth"}
                         {:a 1 :b 101}
                         {:a 1 :b 102}]
          root-hid      (td/add-entity-edn data)
          found         (td/match [{:eid ? a1 1}])
          eids          (mapv #(grab :eid %) found)
          one-leaf-maps (mapv #(td/eid->edn %) eids)]
      (is-set= (td/walk-compact found)
        [{:eid 1002 :a1 :a}
         {:eid 1007 :a1 :a}
         {:eid 1008 :a1 :a}])
      (is-set= one-leaf-maps [{:a 1, :b :first}
                              {:a 1, :b 101}
                              {:a 1, :b 102}])))

  (td/with-tdb (td/new-tdb)
    (let [data     [{:a 1 :x :first}
                    {:a 2 :x :second}
                    {:a 3 :x :third}
                    {:b 1 :x 101}
                    {:b 2 :x 102}
                    {:c 1 :x 301}
                    {:c 2 :x 302}]
          root-hid (td/add-entity-edn data)
          found    (td/match [{:eid ? :a 1}])]
      (is= (td/eid->edn (grab :eid (only found)))
        {:a 1 :x :first}))))

(dotest
  (td/with-tdb (td/new-tdb)
    (let [data     [{:a 1 :b 1 :c 1}
                    {:a 1 :b 2 :c 2}
                    {:a 1 :b 1 :c 3}
                    {:a 2 :b 2 :c 4}
                    {:a 2 :b 1 :c 5}
                    {:a 2 :b 2 :c 6}]
          root-hid (td/add-entity-edn data)]
      (let [found (td/match [{:eid ? :a 1}])]
        (is-set= (mapv #(td/eid->edn (grab :eid %)) found)
          [{:a 1, :b 1, :c 1}
           {:a 1, :b 2, :c 2}
           {:a 1, :b 1, :c 3}]))
      (let [found (td/match [{:eid ? :a 2}])]
        (is-set= (mapv #(td/eid->edn (grab :eid %)) found)
          [{:a 2, :b 2, :c 4}
           {:a 2, :b 1, :c 5}
           {:a 2, :b 2, :c 6}]))
      (let [found (td/match [{:eid ? :b 1}])]
        (is-set= (mapv #(td/eid->edn (grab :eid %)) found)
          [{:a 1, :b 1, :c 1}
           {:a 1, :b 1, :c 3}
           {:a 2, :b 1, :c 5}]))
      (let [found (td/match [{:eid ? :c 6}])]
        (is-set= (mapv #(td/eid->edn (grab :eid %)) found)
          [{:a 2, :b 2, :c 6}]))

      (let [found (td/match [{:eid ? :a 1 :b 2}])]
        (is-set= (mapv #(td/eid->edn (grab :eid %)) found)
          [{:a 1, :b 2, :c 2}]))
      (let [found (td/match [{:eid ? :a 1 :b 1}])]
        (is-set= (mapv #(td/eid->edn (grab :eid %)) found)
          [{:a 1, :b 1, :c 1}
           {:a 1, :b 1, :c 3}])))))

(dotest
  (is= (td/seq->idx-map [:a :b :c]) {0 :a, 1 :b, 2 :c})

  (td/with-tdb (td/new-tdb)
    (td/eid-count-reset)
    (let [data     {:a [{:id [2 22] :color :red}
                        {:id [3 33] :color :yellow}
                        {:id [4 44] :color :blue}]}
          root-eid (td/add-entity-edn data)]
      (is-set= (td/match [{:a [{:color cc}]}])
        [{:cc :red}
         {:cc :blue}
         {:cc :yellow}])))

  (td/with-tdb (td/new-tdb)
    (td/eid-count-reset)
    (let [data     {:a [{:id [2 22] :color :red}
                        {:id [3 33] :color :yellow}
                        {:id [4 44] :color :blue}]}
          root-eid (td/add-entity-edn data)
          result   (td/walk-compact (td/match [{:a [{:eid eid-red :color :red}]}]))]
      (is= result ; #todo fix duplicates for array search
        [{:eid-red 1003}
         {:eid-red 1003}
         {:eid-red 1003}]))))

(dotest
  (td/with-tdb (td/new-tdb)
    (td/eid-count-reset)
    (let [data     {:a [{:id 2 :color :red}
                        {:id 3 :color :yellow}
                        {:id 4 :color :blue}
                        {:id 5 :color :pink}
                        {:id 6 :color :white}]
                    :b {:c [{:ident 2 :flower :rose}
                            {:ident 3 :flower :daisy}
                            {:ident 4 :flower :tulip}
                            ]}}
          root-hid (td/add-entity-edn data)
          result   (td/walk-compact (td/match [{:eid ? :a [{:id ?}]}]))]
      (is-set= result
        [{:eid 1001 :id 2}
         {:eid 1001 :id 6}
         {:eid 1001 :id 3}
         {:eid 1001 :id 4}
         {:eid 1001 :id 5}])

      (is-set= (td/walk-compact (td/match [{:b {:eid ? :c [{:ident ?}]}}]))
        [{:eid 1008, :ident 2}
         {:eid 1008, :ident 4}
         {:eid 1008, :ident 3}]))))

(dotest
  (td/with-tdb (td/new-tdb)
    (td/eid-count-reset)
    (let [data     {:a [{:id 2 :color :red}
                        {:id 3 :color :yellow}
                        {:id 4 :color :blue}]}
          root-hid (td/add-entity-edn data)]
      (is= (td/eid->edn (grab :eid (only (td/match [{:eid ? :color :red}]))))
        {:color :red, :id 2})
      (is= (td/eid->edn (grab :eid (only (td/match [{:eid ? :id 4}]))))
        {:color :blue, :id 4}))))

(dotest
  (td/with-tdb (td/new-tdb)
    (td/eid-count-reset)
    (let [data     {:a [{:id 2 :color :red}
                        {:id 3 :color :yellow}
                        {:id 4 :color :blue}
                        {:id 5 :color :pink}
                        {:id 6 :color :white}]
                    :b {:c [{:ident 2 :flower :rose}
                            {:ident 3 :flower :daisy}
                            {:ident 4 :flower :tulip}
                            ]}}
          root-hid (td/add-entity-edn data)]
      (is= (td/eid->edn (grab :eid (only (td/match [{:eid ?, :id 2}]))))
        {:color :red, :id 2})
      (is= (td/eid->edn (grab :eid (only (td/match [{:eid ?, :ident 2}]))))
        {:flower :rose, :ident 2})
      (is= (td/eid->edn (grab :eid (only (td/match [{:eid ?, :flower :rose}]))))
        {:flower :rose, :ident 2})
      (is= (td/eid->edn (grab :eid (only (td/match [{:eid ?, :color :pink}]))))
        {:color :pink, :id 5}))))

(dotest
  (td/with-tdb (td/new-tdb)
    (let [
          root-eid         (td/add-entity-edn skynet-widgets)
          search-results   (td/match [{:basic-info   {:producer-code ?}
                                       :widgets      [{:widget-code      ?
                                                       :widget-type-code wtc}]
                                       :widget-types [{:widget-type-code wtc
                                                       :description      ?}]}])
          results-filtered (t/walk-with-parents search-results
                             {:enter (fn [parents item]
                                       (t/cond-it-> item
                                         (map? item) (t/drop-if (fn [k v] (td/tmp-attr-kw? k))
                                                       item)))})]
      (is-set= results-filtered
        [{:producer-code "ACME",
          :widget-code   "Dynamite",
          :description   "Boom!",
          :wtc           "c40"}
         {:producer-code "Cyberdyne",
          :widget-code   "Model-101",
          :description   "Resistance Infiltrator",
          :wtc           "t800"}
         {:producer-code "Cyberdyne",
          :widget-code   "Model-201",
          :description   "Mimetic polyalloy",
          :wtc           "t1000"}
         {:producer-code "Cyberdyne",
          :widget-code   "Model-102",
          :description   "Resistance Infiltrator",
          :wtc           "t800"}])

      (let [results-normalized (mapv (fn [result-map]
                                       [(grab :producer-code result-map)
                                        (grab :widget-code result-map)
                                        (grab :description result-map)])
                                 results-filtered)
            normalized-desired [["Cyberdyne" "Model-101" "Resistance Infiltrator"]
                                ["Cyberdyne" "Model-102" "Resistance Infiltrator"]
                                ["Cyberdyne" "Model-201" "Mimetic polyalloy"]
                                ["ACME" "Dynamite" "Boom!"]]]
        (is-set= results-normalized normalized-desired)))))

(dotest
  (td/eid-count-reset)
  (td/with-tdb (td/new-tdb)
    (let [data     [{:a 1 :b 1 :c 1}
                    {:a 1 :b 2 :c 2}
                    {:a 1 :b 1 :c 3}
                    {:a 2 :b 2 :c 4}
                    {:a 2 :b 1 :c 5}
                    {:a 2 :b 2 :c 6}]
          root-hid (td/add-entity-edn data)]
      (let [found (td/match [{:eid eid :a 1}
                             (search-triple eid :b 2)])]
        (is-set= (mapv #(td/eid->edn (grab :eid %)) found)
          [{:a 1, :b 2, :c 2}]))
      (let [found (td/match [{:eid eid :a 1}
                             (search-triple eid :b b)
                             (search-triple eid :c c)])]
        (is-set= found
          [{:eid 1002, :b 1, :c 1}
           {:eid 1004, :b 1, :c 3}
           {:eid 1003, :b 2, :c 2}])))))

(dotest
  (td/with-tdb (td/new-tdb)
    (let [person   {:name "jimmy"
                    :preferred-address
                          {:address1 "123 street ave"
                           :address2 "apt 2"
                           :city     "Townville"
                           :state    "IN"
                           :zip      "46203"}
                    :other-addresses
                          [{:address1 "432 street ave"
                            :address2 "apt 7"
                            :city     "Cityvillage"
                            :state    "New York"
                            :zip      "12345"}
                           {:address1 "534 street ave"
                            :address2 "apt 5"
                            :city     "Township"
                            :state    "IN"
                            :zip      "46203"}]}

          root-eid (td/add-entity-edn person)
          ]
      (is-set= (distinct (td/match [{:zip ?}]))
        [{:zip "12345"}
         {:zip "46203"}])

      (is-set= (distinct (td/match [{:zip ? :city ?}]))
        [{:zip "46203", :city "Township"}
         {:zip "46203", :city "Townville"}
         {:zip "12345", :city "Cityvillage"}]))))

(dotest
  (td/with-tdb (td/new-tdb)
    (let [people   [{:name      "jimmy"
                     :addresses [{:address1  "123 street ave"
                                  :address2  "apt 2"
                                  :city      "Townville"
                                  :state     "IN"
                                  :zip       "46203"
                                  :preferred true}
                                 {:address1  "534 street ave",
                                  :address2  "apt 5",
                                  :city      "Township",
                                  :state     "IN",
                                  :zip       "46203"
                                  :preferred false}
                                 {:address1  "543 Other St",
                                  :address2  "apt 50",
                                  :city      "Town",
                                  :state     "CA",
                                  :zip       "86753"
                                  :preferred false}]}
                    {:name      "joel"
                     :addresses [{:address1  "2026 park ave"
                                  :address2  "apt 200"
                                  :city      "Town"
                                  :state     "CA"
                                  :zip       "86753"
                                  :preferred true}]}]

          root-eid (td/add-entity-edn people)
          results  (td/match [{:name ? :addresses [{:address1 ? :zip "86753"}]}])]
      (is-set= results
        [{:name "jimmy", :address1 "543 Other St"}
         {:name "joel", :address1 "2026 park ave"}]))))

(dotest
  (td/eid-count-reset)
  (td/with-tdb (td/new-tdb)
    (let [root-eid (td/add-entity-edn users-and-accesses)]
      (let [results (td/match [{:people [{:name ? :id id}]}])]
        (is-set= results
          [{:name "jimmy", :id 1}
           {:name "joel", :id 2}
           {:name "tim", :id 3}]))

      (let [pref-zip-data     (td/match [{:addrs {id [{:zip ? :pref true}]}}])
            user-id->pref-zip (apply glue
                                (forv [m pref-zip-data]
                                  {(grab :id m) (grab :zip m)}))

            access-data       (td/match [{:visits {id [{:date ? :geo-loc {:zip ?}}]}}])
            accesses-bad      (keep-if (fn [access-map]
                                         (let [user-id  (grab :id access-map)
                                               zip-acc  (grab :zip access-map)
                                               zip-pref (grab user-id user-id->pref-zip)]
                                           (not= zip-acc zip-pref)))
                                access-data)]
        (is-set= pref-zip-data
          [{:id 2, :zip "11753"}
           {:id 1, :zip "11201"}
           {:id 3, :zip "11456"}])
        (is-set= access-data
          [{:date "1-1-1970", :zip "12345", :id 2}
           {:date "2-1-1970", :zip "11753", :id 2}
           {:date "4-4-4444", :zip "54221", :id 3}
           {:date "5-4-4444", :zip "11456", :id 3}
           {:date "12-31-1900", :zip "00666", :id 1}
           {:date "12-25-1900", :zip "11201", :id 1}])

        (is= user-id->pref-zip {2 "11753", 1 "11201", 3 "11456"})
        (is-set= accesses-bad
          [{:date "12-31-1900", :zip "00666", :id 1},
           {:date "1-1-1970", :zip "12345", :id 2},
           {:date "4-4-4444", :zip "54221", :id 3}]))

      (let [results (td/match
                      [{:people [{:name ? :id id}]}
                       {:addrs {id [{:zip ? :pref false}]}}
                       {:visits {id [{:date ? :geo-loc {:zip zip}}]}}])]
        (is= results [{:date "12-31-1900"
                       :id   1
                       :name "jimmy"
                       :zip  "00666"}]))

      ; can break it down into low level, including array index values
      (let [results-3 (td/match-triples [(search-triple eid-pers :name name)
                                         (search-triple eid-pers :id person-id)
                                         (search-triple eid-addrs person-id eid-addr-deets)
                                         (search-triple eid-addr-deets idx-deet eid-addr-deet)
                                         (search-triple eid-addr-deet :zip zip)
                                         (search-triple eid-addr-deet :pref true)])]
        (is-set= results-3
          [{:eid-pers       1003,
            :name           "jimmy",
            :person-id      1,
            :eid-addrs      1006,
            :eid-addr-deets 1007,
            :idx-deet       0,
            :eid-addr-deet  1008,
            :zip            "11201"}
           {:eid-pers       1004,
            :name           "joel",
            :person-id      2,
            :eid-addrs      1006,
            :eid-addr-deets 1010,
            :idx-deet       0,
            :eid-addr-deet  1011,
            :zip            "11753"}
           {:eid-pers       1005,
            :name           "tim",
            :person-id      3,
            :eid-addrs      1006,
            :eid-addr-deets 1012,
            :idx-deet       0,
            :eid-addr-deet  1013,
            :zip            "11456"}]))

      (let [results-4 (td/match [{:people {:eid eid-pers :name ? :id ?}
                                  :addrs  {:eid eid-addrs}
                                  :visits {:eid eid-visits}
                                  }
                                 (search-triple eid-addrs id eid-addr-deets)
                                 (search-triple eid-addr-deets idx-deet eid-addr-deet)
                                 {:eid eid-addr-deet :zip zip :pref true}])]
        (is-set= results-4
          [{:eid-addr-deets 1012,
            :eid-pers       1005,
            :eid-addr-deet  1013,
            :zip            "11456",
            :idx-deet       0,
            :id             3,
            :name           "tim",
            :eid-addrs      1006}
           {:eid-addr-deets 1010,
            :eid-pers       1004,
            :eid-addr-deet  1011,
            :zip            "11753",
            :idx-deet       0,
            :id             2,
            :name           "joel",
            :eid-addrs      1006}
           {:eid-addr-deets 1007,
            :eid-pers       1003,
            :eid-addr-deet  1008,
            :zip            "11201",
            :idx-deet       0,
            :id             1,
            :name           "jimmy",
            :eid-addrs      1006}]))
      )))

(comment  ; #todo doesn't work w/o eval
  (dotest
    (let [query-result {(->Param :tmp-eid-36493) (->Eid 1003)
                        (->Param :name)          "jimmy"
                        (->Param :date)          "12-25-1900"
                        (->Param :zip-acc)       "11201"
                        (->Param :id)            42
                        (->Param :zip-pref)      "11201"}
          result       (td/eval-with-tagged-params query-result
                         (quote [
                                 ;(println :name-3 name)
                                 ;(println :id-4 id)
                                 (+ 42 7)]))]
      (is= result 49))))

(dotest
  ; ***** this is the big one! *****
  (td/eid-count-reset)
  (td/with-tdb (td/new-tdb)
    (let [root-eid (td/add-entity-edn users-and-accesses)]
      ; (spyx-pretty (td/db-pretty (deref td/*tdb*)))
      (let [qpred            (fn [env]
                               (with-map-vals env [zip-acc zip-pref]
                                 (let [pred-res (not= zip-acc zip-pref)]
                                   pred-res)))
            ; pass a predicate to filter results
            results          (td/match-fn
                               (quote {:people [{:name ? :id id}]
                                       :addrs  {id [{:zip zip-pref :pref true}]}
                                       :visits {id [{:date ? :geo-loc {:zip zip-acc}}]}})
                               qpred)

            ; user filters results
            results-userfilt (keep-if qpred
                               (td/match [{:people [{:name ? :id id}]
                                           :addrs  {id [{:zip zip-pref :pref true}]}
                                           :visits {id [{:date ? :geo-loc {:zip zip-acc}}]}}]))]
        (is= results results-userfilt
          [{:name "jimmy" :date "12-31-1900" :zip-acc "00666" :id 1 :zip-pref "11201"}
           {:name "joel" :date "1-1-1970" :zip-acc "12345" :id 2 :zip-pref "11753"}
           {:name "tim" :date "4-4-4444" :zip-acc "54221" :id 3 :zip-pref "11456"}])))))

(dotest
  (td/eid-count-reset)
  (td/with-tdb (td/new-tdb)
    (let [root-eid (td/add-entity-edn skynet-widgets)
          results  (td/match
                     [{:basic-info   {:producer-code ?}
                       :widgets      [{:widget-code      ?
                                       :widget-type-code wtc}]
                       :widget-types [{:widget-type-code wtc
                                       :description      ?}]}])]
      (is= results
        [{:description "Resistance Infiltrator" :widget-code "Model-101" :producer-code "Cyberdyne" :wtc "t800"}
         {:description "Resistance Infiltrator" :widget-code "Model-102" :producer-code "Cyberdyne" :wtc "t800"}
         {:description "Mimetic polyalloy" :widget-code "Model-201" :producer-code "Cyberdyne" :wtc "t1000"}
         {:description "Boom!" :widget-code "Dynamite" :producer-code "ACME" :wtc "c40"}]))))

;---------------------------------------------------------------------------------------------------
(dotest
  (td/eid-count-reset)
  (td/with-tdb (td/new-tdb)
    (let [root-eid (td/add-entity-edn {:a 1 :b {:c 3 :d [4 5 6]}})
          str-out  (with-out-str
                     (td/walk-entity root-eid
                       {:enter (fn [triple] (spy :enter triple))
                        :leave (fn [triple] (spy :leave triple))}))]
      ; (println str-out)
      (is-nonblank= str-out
        ":enter => [<Eid 1001> <Prim :a> <Prim 1>]
         :leave => [<Eid 1001> <Prim :a> <Prim 1>]
         :enter => [<Eid 1001> <Prim :b> <Eid 1002>]
           :enter => [<Eid 1002> <Prim :c> <Prim 3>]
           :leave => [<Eid 1002> <Prim :c> <Prim 3>]
           :enter => [<Eid 1002> <Prim :d> <Eid 1003>]
             :enter => [<Eid 1003> <Idx 0> <Prim 4>]
             :leave => [<Eid 1003> <Idx 0> <Prim 4>]
             :enter => [<Eid 1003> <Idx 1> <Prim 5>]
             :leave => [<Eid 1003> <Idx 1> <Prim 5>]
             :enter => [<Eid 1003> <Idx 2> <Prim 6>]
             :leave => [<Eid 1003> <Idx 2> <Prim 6>]
           :leave => [<Eid 1002> <Prim :d> <Eid 1003>]
         :leave => [<Eid 1001> <Prim :b> <Eid 1002>]")
      (is= (td/eid->edn root-eid) {:a 1 :b {:c 3 :d [4 5 6]}})
      ; (newline)
      ; (println ":1425-enter-----------------------------------------------------------------------------")
      (binding [td/*tdb-deltas* (atom {:add [] :remove []})] ; #todo kludgy!  clean up
        (td/remove-triple-impl [1001 :a 1]))
      ; (println ":1425-leave-----------------------------------------------------------------------------")
      (is= (td/eid->edn root-eid) {:b {:c 3, :d [4 5 6]}})
      (binding [td/*tdb-deltas* (atom {:add [] :remove []})]
        (td/remove-triple-impl [1002 :d (->Eid 1003)]))
      (is= (td/eid->edn root-eid) {:b {:c 3}}))))

(dotest
  (is= (td/eav->eav [:e :a :v]) [:e :a :v])
  (is= (td/eav->vea [:e :a :v]) [:v :e :a])
  (is= (td/vea->eav [:v :e :a]) [:e :a :v])
  (is= (td/eav->ave [:e :a :v]) [:a :v :e])
  (is= (td/ave->eav [:a :v :e]) [:e :a :v])
  (td/eid-count-reset)
  (td/with-tdb (td/new-tdb)
    (let [root-eid (td/add-entity-edn {:a "fred" :b [0 1 2] :c #{4 5 6}})]
      (is= (td/eid->type (->Eid 1001)) :map)
      (is= (td/eid->type (->Eid 1002)) :array)
      (is= (td/eid->type (->Eid 1003)) :set))))

(dotest
  (td/eid-count-reset)
  (td/with-tdb (td/new-tdb)
    (let [root-eid (td/add-entity-edn {:a "fred" :b [0 1 2]})]
      ; (prn dashes) (td/walk-compact (deref *tdb*))
      (throws? (td/entity-remove 1002))
      (is= (td/eid->edn 1002) [0 1 2])
      (td/entity-array-elem-remove 1002 1)
      ; (prn dashes) (td/walk-compact (deref *tdb*))
      (is= (td/eid->edn root-eid) {:a "fred", :b [0 2]})
      (td/entity-map-entry-remove 1001 :b)
      (is= (td/eid->edn root-eid) {:a "fred"}) ) )
  (td/eid-count-reset)
  (td/with-tdb (td/new-tdb)
    (let [e0 (td/add-entity-edn {:f "fred"})
          e1 (td/add-entity-edn {:w "wilma"})]
      (is= (td/eid->edn e0) {:f "fred"})
      (td/entity-remove e0)
      (throws? (td/eid->edn e0))
      (is= (td/eid->edn e1) {:w "wilma"}))) )

(dotest
  (td/eid-count-reset)
  (td/with-tdb (td/new-tdb)
    (let [root-eid (td/add-entity-edn {:a 1})]
      ; (prn dashes) (spyx-pretty (td/walk-compact (deref *tdb*)))

      (td/entity-map-entry-add root-eid :b 2) ; legal add
      ; (prn dashes) (spyx-pretty (td/walk-compact (deref *tdb*)))
      (is= (td/eid->edn root-eid) {:a 1 :b 2})

      (throws? (td/entity-map-entry-add 9999 [1 2] 2)) ; invalid eid
      (throws? (td/entity-map-entry-add root-eid [1 2] 2)) ; non-primitive key
      (throws? (td/entity-map-entry-add root-eid :a 99)) ; duplicate key

      (td/entity-map-entry-add root-eid :c {:d 4 :e [5 6 7]})
      ; (prn dashes) (spyx-pretty (td/walk-compact (deref *tdb*)))

      (is= (td/walk-compact (td/eid->edn root-eid))
        {:a 1, :b 2, :c {:d 4, :e [5 6 7]}})
      (td/entity-map-entry-update root-eid :a inc)
      (is= (td/walk-compact (td/eid->edn root-eid))
        {:a 2, :b 2, :c {:d 4, :e [5 6 7]}}))))

(dotest
  (td/eid-count-reset)
  (td/with-tdb (td/new-tdb)
    (let [root-eid (td/add-entity-edn [0 1])]
      ; (prn dashes) (spyx-pretty (td/walk-compact (deref *tdb*)))
      (throws? (td/entity-array-elem-add 9999 9 99)) ; invalid eid
      (throws? (td/entity-array-elem-add root-eid :a 99)) ; non-primitive key
      (throws? (td/entity-array-elem-add root-eid 0 99)) ; duplicate key

      (td/entity-array-elem-add root-eid 3 3) ; legal add
      (td/entity-array-elem-add root-eid 9 9) ; legal add
      ; (prn dashes) (spyx-pretty (td/walk-compact (deref *tdb*)))
      (is= (td/eid->edn root-eid) [0 1 3 9])

      (td/entity-array-elem-add root-eid 99 {:a 1 :b #{:c :d}}) ; legal add
      ; (prn dashes) (spyx-pretty (td/walk-compact (deref *tdb*)))
      (is= (td/eid->edn root-eid) [0 1 3 9 {:a 1, :b #{:c :d}}])
      (td/entity-array-elem-remove root-eid 1)
      (td/entity-array-elem-remove root-eid 0)
      ; (prn dashes) (spyx-pretty (td/walk-compact (deref *tdb*)))
      (is= (td/eid->edn root-eid)
        [3 9 {:a 1, :b #{:c :d}}])
      (td/entity-array-elem-update root-eid 0 #(* 14 %))
      ; (prn dashes) (spyx-pretty (td/walk-compact (deref *tdb*)))
      (is= (td/eid->edn root-eid)
        [42 9 {:a 1, :b #{:c :d}}])
      )))

(dotest
  (td/eid-count-reset)
  (td/with-tdb (td/new-tdb)
    (let [root-eid (td/add-entity-edn #{:a :b})]
      ; (prn dashes) (spyx-pretty (td/walk-compact (deref *tdb*)))
      (throws? (td/entity-set-elem-add 9999 2)) ; invalid eid
      (throws? (td/entity-set-elem-add root-eid [1 2])) ; non-primitive key
      (throws? (td/entity-set-elem-add root-eid :a)) ; duplicate key
      (td/entity-set-elem-add root-eid :c) ; legal add
      (is= (td/eid->edn root-eid) #{:a :b :c})
      (td/entity-set-elem-remove root-eid :c)
      (is= (td/eid->edn root-eid) #{:a :b})

      (td/entity-set-elem-add root-eid 1)
      (is= (td/eid->edn root-eid) #{:a :b 1})
      (td/entity-set-elem-update root-eid 1 inc)
      (is= (td/eid->edn root-eid) #{:a :b 2}))))

;---------------------------------------------------------------------------------------------------
; example of query/filter/mutate paradigm (contrasted to map/filter/reduce)
(def clojutre-2019-power-of-lenses-laurinharju
  {:employees [{:name   "justice ward"
                :role   :programmer
                :salary 86750}
               {:name   "geovanni morgan"
                :role   :service-designer
                :salary 73882}
               {:name nil}
               {:name   ""
                :role   :programmer
                :salary nil}
               {:name   "raymond richard mathews"
                :role   :programmer
                :salary 45930}]})

(defn valid-name?
  [frame] (it-> frame
            (grab :name it)
            (and (string? it)
              (pos? (count it)))))

(defn valid-empl?
  [frame] (and (valid-name? frame)
            (number? (grab :salary frame))))

(s/defn name-cap-fn :- s/Str
  [name :- s/Str]
  (str/join \space
    (forv [word (str/split name #"\s")]
      (str/capitalize word))))

; #todo allow inline & local functions
(dotest
  (td/eid-count-reset)
  (td/with-tdb (td/new-tdb)
    (let [root-eid    (td/add-entity-edn clojutre-2019-power-of-lenses-laurinharju)
          frames-name (keep-if valid-name? (td/match [{:eid ? :name ?}]))] ; don't even need to reference :employees or array
      (is= frames-name [{:eid 1004, :name "geovanni morgan"}
                        {:eid 1003, :name "justice ward"}
                        {:eid 1007, :name "raymond richard mathews"}])

      ; Capitalize all the names
      (doseq [frame frames-name]
        (td/entity-map-entry-update (grab :eid frame) :name name-cap-fn))
      (is= (td/eid->edn root-eid) ; verify result
        {:employees
         [{:name "Justice Ward", :role :programmer, :salary 86750}
          {:name "Geovanni Morgan", :role :service-designer, :salary 73882}
          {:name nil}
          {:name "", :role :programmer, :salary nil}
          {:name "Raymond Richard Mathews", :role :programmer, :salary 45930}]})

      ; Search for programmers, returning eid, name, salary.
      (let [frames-prog (keep-if valid-empl?
                          (td/match [{:eid ? :name ? :role :programmer :salary ?}]))]
        (is= frames-prog ; verify found only programmers
          [{:eid 1003, :name "Justice Ward", :salary 86750}
           {:eid 1007, :name "Raymond Richard Mathews", :salary 45930}])

        ; Update salary with 10% raise
        (doseq [frame frames-name]
          (td/entity-map-entry-update (grab :eid frame) :salary #(int (* % 1.05))))
        (is= (td/eid->edn root-eid) ; Verify result
          {:employees
           [{:name "Justice Ward", :role :programmer, :salary 91087}
            {:name "Geovanni Morgan", :role :service-designer, :salary 77576}
            {:name nil}
            {:name "", :role :programmer, :salary nil}
            {:name "Raymond Richard Mathews", :role :programmer, :salary 48226}]})))))

(comment  ; #todo idea
  (let [ctx    {:name "Joe" :city "Springfield"}
        query  (with-ctx ctx
                 (q/tmpl {:eid ? :state ? :age ? :name ? :city ?})) ; fills in vals from ctx
        preds  {; basic preds are called at each state if corresponding param is resolved
                :age    #(odd? %)
                :state  (s/fn [state-code :- s/Str]
                          (let [first-letter (str (first (str/lower-case state-code)))]
                            (< "a" first-letter "k")))
                :td/all [; all of these are called for all ctx vals at each stage
                         (fn [ctx]
                           (t/destruct ctx
                             [:age ?
                              :state ?
                              :eid ?]
                             (when (and age eid)
                               (even? (+ age eid)))))]}
        result (match* {:query query
                        :preds preds
                        :ctx   ctx ; #todo maybe pass in the ctx ???
                        })])
  )


(def ^:dynamic *result* nil)
(dotest
  ; (spyx (swap! *result* append :a))
  ; (spyx *result*)

  (is= {:a 1} (assoc nil :a 1))
  (is= nil (dissoc nil :a))
  (is= {:a {:b 12}} (assoc-in nil [:a :b] 12)) ; replaces nil with nested maps as req'd
  (is= {:a {:b 12}} (assoc-in {:a nil} [:a :b] 12))
  (is= {:a {}} (t/dissoc-in {:a {}} [:a :b])) ; demo with missing data
  (is= {:a nil} (t/dissoc-in {:a nil} [:a :b]))
  (is= {:a nil} (t/dissoc-in {} [:a :b]))

  (td/with-tdb (td/new-tdb)
    (td/entity-watch-add 101 :101a (t/const-fn :watch-101-a))
    (td/entity-watch-add 102 :102a (t/const-fn :watch-102-a))
    (td/entity-watch-add 101 :101b (fn [& args] [:watch-101-b args]))
    (td/entity-watch-add 102 :102b (t/const-fn :watch-102-b))
    (is-set= (td/entity-watchers-notify 101 1 2 3) [:watch-101-a [:watch-101-b [:101b 1 2 3]]])
    (is-set= (td/entity-watchers-notify 102 :a :b :c) [:watch-102-a :watch-102-b]))

  (binding [*result* (atom [])]
    (td/eid-count-reset)
    (td/with-tdb (td/new-tdb)
      (let [root-eid (td/add-entity-edn {:a 1 :b 2})]
        (td/entity-watch-add root-eid :watch-a (fn watch-a-fn [key arg]
                                                 (swap! *result* append [:got key arg])))
        (td/entity-map-entry-update root-eid :a inc)
        (is= (td/walk-compact @*result*)
          [[:got :watch-a {:add    [[{:eid 1001} {:prim :a} {:prim 2}]],
                           :remove [[{:eid 1001} {:prim :a} {:prim 1}]]}]])))))


(comment  ; <<comment>>
  )       ; <<comment>>




