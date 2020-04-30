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
                                       only only2 forv glue grab nl keep-if drop-if ->sym xfirst xsecond xthird
                                       ]]
            [tupelo.data :as td :refer [with-tdb new-tdb eid-count-reset lookup query-triples query-triples->tagged boolean->binary search-triple
                                        *tdb* <tag <val tag-param tag-eid tag-idx
                                        ]]
            [clojure.string :as str]
            [schema.core :as s]
            [tupelo.data.index :as index]
            )))

; #todo fix for cljs

#?(:cljs (enable-console-print!))

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

(defn tv [t v] (td/->TagVal t v))

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
                                  {:name "tim" :id 3} ]
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
                                      :pref     true}] }
                         :visits {1 [{:date "12-25-1900" :geo-loc {:zip "11201"}}
                                     {:date "12-31-1900" :geo-loc {:zip "00666"}}]
                                  2 [{:date "1-1-1970" :geo-loc {:zip "12345"}}
                                     {:date "2-1-1970" :geo-loc {:zip "11753"}}]
                                  3 [{:date "4-4-4444" :geo-loc {:zip "54221"}}
                                     {:date "5-4-4444" :geo-loc {:zip "11456"}}]}})

#?(:clj
   (do

     ;-----------------------------------------------------------------------------
     (dotest
       (is= true (and true))
       (is= [true] (cons true []))
       (is= true (every? t/truthy? (cons true []))) )


     (dotest
       (let [tv     (td/->TagVal :a 1)
             tv-str (with-out-str (println tv))]
         (is= {:tag :a :val 1} (unlazy tv))
         (is= :a (td/<tag tv))
         (is= 1 (td/<val tv))
         (is= "<:a 1>" (str/trim tv-str))
         (is (td/tagged? tv))
         (isnt (td/tagged? 1))
         (is= 1 (td/untagged tv))
         (is= 1 (td/untagged 1)))

       (let [sa  (t/->sym :a)
             tva (td/->TagVal :sym sa)]
         (is= (unlazy tva) {:tag :sym, :val (quote a)})))

     ;-----------------------------------------------------------------------------
     (def vec234 [2 3 4])

     (dotest
       (is (td/unquote-form? (quote (unquote (+ 2 3)))))
       (is (td/unquote-splicing-form? (quote (unquote-splicing (+ 2 3)))))

       (is= (td/quote-template-impl (quote {:a 1 :b (unquote (+ 2 3))}))
         {:a 1, :b 5})
       (is= (td/quote-template-impl (quote [a b (unquote (+ 2 3))]))
         (quote [a b 5]))
       (is= (td/quote-template {:a 1 :b (unquote (+ 2 3))})
         {:a 1, :b 5})
       (is= (td/quote-template {:a 1 :b (unquote (vec (range 3)))})
         {:a 1, :b [0 1 2]})
       (is= (td/quote-template {:a 1 :b (unquote vec234)})
         {:a 1, :b [2 3 4]})

       (let [result (td/quote-template (list 1 2 (unquote (inc 2)) 4 5))]
         (is (list? result))
         (is= result (quote (1 2 3 4 5))))

       (is= (td/quote-template-impl (quote [1 (unquote-splicing (range 2 5)) 5]))
         [1 2 3 4 5])
       (is= (td/quote-template-impl (quote [1 (unquote-splicing tst.tupelo.data/vec234) 5])) ; must be fully-qualified Var here
         [1 2 3 4 5])
       (is= (td/quote-template [1 (unquote-splicing vec234) 5]) ; unqualified name OK here
         [1 2 3 4 5])
       (is= (td/quote-template [1 (unquote-splicing (t/thru 2 4)) 5])
         [1 2 3 4 5])
       (is= (td/quote-template [1 (unquote (t/thru 2 4)) 5])
         [1 [2 3 4] 5])
       )


     (dotest
       (is= 3 (eval (quote (+ 1 2)))))

     (dotest ; #todo => tupelo.core
       (is (td/only? [1]))
       (is (td/only? {:a 1}))
       (is (td/only? #{:stuff}))
       (isnt (td/only? [1 2]))
       (isnt (td/only? {:a 1 :b 2}))
       (isnt (td/only? #{:stuff :more}))

       (is (td/only2? [[1]]))
       (is (td/only2? #{{:a 1}}))
       (is (td/only2? #{#{:stuff}}))
       (isnt (td/only2? [[1 2]]))
       (isnt (td/only2? [{:a 1 :b 2}]))
       (isnt (td/only2? [#{:stuff :more}])))

     (dotest ; #todo => tupelo.core
       (is= 5 (td/with-cum-val 0
                (doseq [ii (t/thru 5)]
                  (td/cum-val-set-it ii))))
       (is= 15 (td/with-cum-val 0
                 (doseq [ii (t/thru 5)]
                   (td/cum-val-set-it (+ it ii)))))
       (is= (td/with-cum-val {}
              (doseq [ii (t/thru 3)]
                (td/cum-val-set-it (glue it {ii (+ 10 ii)}))))
         {0 10
          1 11
          2 12
          3 13})
       (is= (td/with-cum-val {}
              (doseq [ii (t/thru 3)]
                (swap! td/*cumulative-val* assoc ii (+ 10 ii)))) ; can do it "manually" if desired
         {0 10
          1 11
          2 12
          3 13}))

     (dotest ; #todo => tupelo.core
       (is= (td/with-cum-vector
              (dotimes [ii 5]
                (td/cum-vector-append ii)))
         [0 1 2 3 4])
       (let [ff (fn ff-fn [n]
                  (when (t/nonneg? n)
                    (td/cum-vector-append n)
                    (ff-fn (dec n))))]
         (is= (td/with-cum-vector (ff 5))
           [5 4 3 2 1 0]))
       ; It will even work across multiple futures:  https://clojure.org/reference/vars#conveyance
       (let [N     10
             randy (fn [n]
                     (Thread/sleep (int (+ 50 (* 50 (Math/random)))))
                     (td/cum-vector-append n)
                     n)
             nums  (td/with-cum-vector
                     (let [futures     (forv [ii (range N)]
                                         (future (randy ii)))
                           future-vals (forv [future futures] @future)] ; wait for all futures to finish
                       (is= future-vals (range N))))] ; in order of creation
         (is-set= nums (range N)))) ; nums is in random order


     (dotest
       (let [ss123 (t/it-> (index/empty-index)
                     (conj it [1 :a])
                     (conj it [3 :a])
                     (conj it [2 :a]))
             ss13  (disj ss123 [2 :a])]
         (is= ss123 #{[1 :a] [2 :a] [3 :a]})
         (is= (vec ss123) [[1 :a] [2 :a] [3 :a]])
         (is= ss13 #{[1 :a] [3 :a]}))

       (is (map? (td/tag-eid 3)))
       (is (map? {:a 1}))
       (is (record? (td/tag-idx 3)))
       (isnt (record? {:a 1}))

       ; Leaf and entity-id records sort separately in the index. Eid sorts first since the type name
       ; `tupelo.data.Eid` sorts before `tupelo.data.Leaf`
       (is= (td/tag-eid 5) (td/->TagVal :eid 5))
       (throws? (td/tag-eid "hello"))
       (is= (td/tag-idx 5) (td/->TagVal :idx 5))
       (throws? (td/tag-idx "hello"))

       (let [idx (-> (index/empty-index)

                   (index/add-entry [1 3])
                   (index/add-entry [1 (td/tag-eid 3)])
                   (index/add-entry [1 1])
                   (index/add-entry [1 (td/tag-eid 1)])
                   (index/add-entry [1 2])
                   (index/add-entry [1 (td/tag-eid 2)])

                   (index/add-entry [0 3])
                   (index/add-entry [0 (td/tag-eid 3)])
                   (index/add-entry [0 1])
                   (index/add-entry [0 (td/tag-eid 1)])
                   (index/add-entry [0 2])
                   (index/add-entry [0 (td/tag-eid 2)]))]
         (is= (td/tagval-walk-compact (vec idx))
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
           {:eid-type {} :idx-eav #{} :idx-vea #{} :idx-ave #{}})
         (let [edn-val  {:a 1}
               root-eid (td/add-edn edn-val)]
           (is= root-eid 1001)
           (is= (td/tagval-walk-compact (deref *tdb*))
             {:eid-type {{:eid 1001} :map},
              :idx-ave  #{[:a 1 {:eid 1001}]},
              :idx-eav  #{[{:eid 1001} :a 1]},
              :idx-vea  #{[1 {:eid 1001} :a]}} )
           (is= edn-val (td/eid->edn root-eid)))))

     (dotest
       (with-tdb (new-tdb)
         (eid-count-reset)
         (let [edn-val  {:a 1 :b 2}
               root-eid (td/add-edn edn-val)]
           (is= 1001 root-eid)
           (is= (td/tagval-walk-compact (deref *tdb*))
             {:eid-type {{:eid 1001} :map},
              :idx-ave  #{[:b 2 {:eid 1001}] [:a 1 {:eid 1001}]},
              :idx-eav  #{[{:eid 1001} :a 1] [{:eid 1001} :b 2]},
              :idx-vea  #{[2 {:eid 1001} :b] [1 {:eid 1001} :a]}})
           (is= edn-val (td/eid->edn root-eid)))))

     (dotest
       (with-tdb (new-tdb)
         (eid-count-reset)
         (let [edn-val  {:a 1 :b 2 :c {:d 4}}
               root-eid (td/add-edn edn-val)]
           (is= 1001 root-eid)
           (is= (td/tagval-walk-compact (deref *tdb*))
             {:eid-type {{:eid 1001} :map, {:eid 1002} :map},
              :idx-ave  #{[:b 2 {:eid 1001}]
                          [:d 4 {:eid 1002}]
                          [:c {:eid 1002} {:eid 1001}]
                          [:a 1 {:eid 1001}]},
              :idx-eav  #{[{:eid 1002} :d 4]
                          [{:eid 1001} :a 1]
                          [{:eid 1001} :b 2]
                          [{:eid 1001} :c {:eid 1002}]},
              :idx-vea  #{[2 {:eid 1001} :b]
                          [4 {:eid 1002} :d]
                          [{:eid 1002} {:eid 1001} :c]
                          [1 {:eid 1001} :a]}} )
           (is= edn-val (td/eid->edn root-eid)))))

     (dotest
       (with-tdb (new-tdb)
         (eid-count-reset)
         (let [edn-val  [1 2 3]
               root-eid (td/add-edn edn-val)]
           (is= (td/tagval-walk-compact (deref *tdb*))
             {:eid-type {{:eid 1001} :array},
              :idx-ave  #{[{:idx 0} 1 {:eid 1001}]
                          [{:idx 1} 2 {:eid 1001}]
                          [{:idx 2} 3 {:eid 1001}]},
              :idx-eav  #{[{:eid 1001} {:idx 2} 3]
                          [{:eid 1001} {:idx 1} 2]
                          [{:eid 1001} {:idx 0} 1]},
              :idx-vea  #{[3 {:eid 1001} {:idx 2}]
                          [1 {:eid 1001} {:idx 0}]
                          [2 {:eid 1001} {:idx 1}]}} )
           (is= edn-val (td/eid->edn root-eid)))))

     (dotest
       (with-tdb (new-tdb)
         (eid-count-reset)
         (let [edn-val  {:a 1 :b 2 :c [10 11 12]}
               root-eid (td/add-edn edn-val)]
           (is= (td/tagval-walk-compact (deref *tdb*))
             {:eid-type {{:eid 1001} :map, {:eid 1002} :array},
              :idx-ave  #{[{:idx 1} 11 {:eid 1002}]
                          [{:idx 2} 12 {:eid 1002}]
                          [:b 2 {:eid 1001}]
                          [{:idx 0} 10 {:eid 1002}]
                          [:c {:eid 1002} {:eid 1001}]
                          [:a 1 {:eid 1001}]},
              :idx-eav  #{[{:eid 1002} {:idx 0} 10]
                          [{:eid 1002} {:idx 2} 12]
                          [{:eid 1001} :a 1]
                          [{:eid 1002} {:idx 1} 11]
                          [{:eid 1001} :b 2]
                          [{:eid 1001} :c {:eid 1002}]},
              :idx-vea  #{[2 {:eid 1001} :b]
                          [10 {:eid 1002} {:idx 0}]
                          [{:eid 1002} {:eid 1001} :c]
                          [11 {:eid 1002} {:idx 1}]
                          [1 {:eid 1001} :a]
                          [12 {:eid 1002} {:idx 2}]}})
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
             (td/add-edn m))
           (is= (td/tagval-walk-compact @*tdb*) ; #todo error: missing :array
             {:eid-type {{:eid 1009} :map,
                         {:eid 1002} :map,
                         {:eid 1001} :map,
                         {:eid 1008} :map,
                         {:eid 1006} :map,
                         {:eid 1007} :map,
                         {:eid 1003} :map,
                         {:eid 1005} :map,
                         {:eid 1004} :map},
              :idx-ave  #{[:b 2 {:eid 1005}] [:a 2 {:eid 1002}] [:c 2 {:eid 1008}]
                          [:b 1 {:eid 1004}] [:c 3 {:eid 1009}] [:a 3 {:eid 1003}]
                          [:c 1 {:eid 1007}] [:b 3 {:eid 1006}] [:a 1 {:eid 1001}]},
              :idx-eav  #{[{:eid 1002} :a 2]
                          [{:eid 1005} :b 2]
                          [{:eid 1007} :c 1]
                          [{:eid 1009} :c 3]
                          [{:eid 1003} :a 3]
                          [{:eid 1001} :a 1]
                          [{:eid 1008} :c 2]
                          [{:eid 1004} :b 1]
                          [{:eid 1006} :b 3]},
              :idx-vea  #{[3 {:eid 1006} :b] [3 {:eid 1003} :a] [3 {:eid 1009} :c]
                          [2 {:eid 1005} :b] [2 {:eid 1002} :a] [1 {:eid 1007} :c]
                          [2 {:eid 1008} :c] [1 {:eid 1001} :a] [1 {:eid 1004} :b]}})
           ;---------------------------------------------------------------------------------------------------
           (is= (td/tagval-walk-compact (lookup [(td/tag-eid 1003) nil nil]))
             [[{:eid 1003} :a 3]])
           (is= (td/tagval-walk-compact (lookup [nil :b nil]))
             [[{:eid 1004} :b 1]
              [{:eid 1005} :b 2]
              [{:eid 1006} :b 3]])
           (is= (td/tagval-walk-compact (lookup [nil nil 3]))
             [[{:eid 1003} :a 3]
              [{:eid 1006} :b 3]
              [{:eid 1009} :c 3]])
           (is= (td/tagval-walk-compact (lookup [nil nil nil]))
             [[{:eid 1001} :a 1]
              [{:eid 1002} :a 2]
              [{:eid 1003} :a 3]
              [{:eid 1004} :b 1]
              [{:eid 1005} :b 2]
              [{:eid 1006} :b 3]
              [{:eid 1007} :c 1]
              [{:eid 1008} :c 2]
              [{:eid 1009} :c 3]])

           ;---------------------------------------------------------------------------------------------------
           (is= (td/tagval-walk-compact (lookup [nil :a 3]))
             [[{:eid 1003} :a 3]])
           (is= (td/tagval-walk-compact (lookup [(td/tag-eid 1009) nil 3]))
             [[{:eid 1009} :c 3]])
           (is= (td/tagval-walk-compact (lookup [(td/tag-eid 1005) :b nil]))
             [[{:eid 1005} :b 2]]))))

     (dotest
       (with-tdb (new-tdb)
         (eid-count-reset)
         (let [edn-val  {:a 1 :b 2}
               root-eid (td/add-edn edn-val)]
           (is= edn-val (td/eid->edn root-eid))
           (is= (td/tagval-walk-compact (deref *tdb*))
             {:eid-type {{:eid 1001} :map},
              :idx-ave  #{[:b 2 {:eid 1001}]
                          [:a 1 {:eid 1001}]},
              :idx-eav  #{[{:eid 1001} :a 1]
                          [{:eid 1001} :b 2]},
              :idx-vea  #{[2 {:eid 1001} :b]
                          [1 {:eid 1001} :a]}}))

         (let [triple-specs [[(tag-param :x) :a 1]]]
           (let [pred1 (fn [query-result]
                         (t/with-map-vals query-result [x]
                           (pos? x)))
                 pred2 (fn [query-result]
                         (t/with-map-vals query-result [x]
                           (int? x)))]
             (is= (td/tagval-walk-compact (query-triples->tagged triple-specs))
               [{{:param :x} {:eid 1001}}])
             (is= (td/query-triples triple-specs) [{:x 1001}])

             (is= (td/tagval-walk-compact (td/query-triples+preds
                                            triple-specs
                                            [pred1 pred2]))
               [{{:param :x} {:eid 1001}}])
             ))))

     (dotest
       (with-tdb (new-tdb)
         (eid-count-reset)
         (let [edn-val  {:a 1 :b 2}
               root-eid (td/add-edn edn-val)]
           (is= edn-val (td/eid->edn root-eid))
           (is= (td/tagval-walk-compact (deref *tdb*))
             {:eid-type {{:eid 1001} :map},
              :idx-ave  #{[:b 2 {:eid 1001}] [:a 1 {:eid 1001}]},
              :idx-eav  #{[{:eid 1001} :a 1]
                          [{:eid 1001} :b 2]},
              :idx-vea  #{[2 {:eid 1001} :b] [1 {:eid 1001} :a]}}))

         (let [triple-specs [[(tag-param :x) :a 1]]]
           (let [pred1 (fn [query-result]
                         (t/with-map-vals query-result [x]
                           (pos? x)))
                 pred2 (fn [query-result]
                         (t/with-map-vals query-result [x]
                           (int? x)))
                 ]
             (is= (td/tagval-walk-compact (query-triples->tagged triple-specs))
               [{{:param :x} {:eid 1001}}])
             (is= (td/tagval-walk-compact (td/query-triples+preds
                                            triple-specs
                                            [pred1 pred2]))
               [{{:param :x} {:eid 1001}}])))))

     (dotest
       (let [search-param-x (td/->SearchParam x)]
         (is= search-param-x (td/->TagVal :param :x))
         (is= search-param-x (td/->TagVal :param :x))
         (is= (unlazy search-param-x) {:tag :param, :val :x})
         (is= :param (<tag search-param-x))
         (is= :x (<val search-param-x)))
       (is= (td/search-triple x y z)
         [(td/->SearchParam x)
          (td/->SearchParam y)
          (td/->SearchParam z)]))

     (dotest
       (with-tdb (new-tdb)
         (eid-count-reset)
         (let [edn-val  {:a 1
                         :b 1}
               root-eid (td/add-edn edn-val)]
           (is= edn-val (td/eid->edn root-eid))
           (is= (td/tagval-walk-compact (deref *tdb*))
             {:eid-type {{:eid 1001} :map},
              :idx-ave  #{[:b 1 {:eid 1001}]
                          [:a 1 {:eid 1001}]},
              :idx-eav  #{[{:eid 1001} :a 1]
                          [{:eid 1001} :b 1]},
              :idx-vea  #{[1 {:eid 1001} :b]
                          [1 {:eid 1001} :a]}} ))

         (let [search-spec [[(td/->TagVal :param :x) :a 1]]]
           (is= (unlazy (query-triples->tagged search-spec))
             [{{:tag :param, :val :x} {:tag :eid, :val 1001}}])
           (is= (query-triples search-spec) [{:x 1001}]))

         (let [search-spec [[(td/->TagVal :param :x) :b 1]]]
           (is= (unlazy (query-triples->tagged search-spec))
             [{{:tag :param, :val :x} {:tag :eid, :val 1001}}])
           (is= (query-triples search-spec) [{:x 1001}]))

         (let [search-spec [[(tag-param :x) (td/->TagVal :param :y) 1]]]
           (is= (td/tagval-walk-compact (query-triples->tagged search-spec))
             [{{:param :x} {:eid 1001}, {:param :y} :a}
              {{:param :x} {:eid 1001}, {:param :y} :b}])
           (is= (query-triples search-spec)
             [{:x 1001, :y :a} {:x 1001, :y :b}]))))

     (dotest
       (with-tdb (new-tdb)
         (eid-count-reset)
         (let [edn-val  {:a {:b 2}}
               root-eid (td/add-edn edn-val)]
           (is= (td/tagval-walk-compact (deref *tdb*))
             {:eid-type {{:eid 1001} :map, {:eid 1002} :map},
              :idx-ave  #{[:a {:eid 1002} {:eid 1001}]
                          [:b 2 {:eid 1002}]},
              :idx-eav  #{[{:eid 1001} :a {:eid 1002}]
                          [{:eid 1002} :b 2]},
              :idx-vea  #{[2 {:eid 1002} :b]
                          [{:eid 1002} {:eid 1001} :a]}})
           ; (prn :-----------------------------------------------------------------------------)
           ; compound search
           (is= (td/tagval-walk-compact (query-triples->tagged [[(tag-param :x) :a (tag-param :y)]
                                                                [(tag-param :y) :b 2]]))
             [{{:param :x} {:eid 1001},
               {:param :y} {:eid 1002}}])
           (is= (query-triples [[(tag-param :x) :a (tag-param :y)]
                                [(tag-param :y) :b 2]])
             [{:x 1001, :y 1002}])

           ; (prn :-----------------------------------------------------------------------------)
           ; failing search
           (is= [] (query-triples->tagged [[{:param :x} :a {:param :y}]
                                           [{:param :y} :b 99]]))
           (is= [] (query-triples [[{:param :x} :a {:param :y}]
                                   [{:param :y} :b 99]]))
           ; (prn :-----------------------------------------------------------------------------)
           ; wildcard search - match all
           (is= (td/tagval-walk-compact (query-triples->tagged [[(tag-param :x) (tag-param :y) (tag-param :z)]]))
             [{{:param :x} {:eid 1001}
               {:param :y} :a
               {:param :z} {:eid 1002}}
              {{:param :x} {:eid 1002}
               {:param :y} :b
               {:param :z} 2}])
           (is= (query-triples [[(tag-param :x) (tag-param :y) (tag-param :z)]])
             [{:x 1001, :y :a, :z 1002}
              {:x 1002, :y :b, :z 2}]))))

     (dotest
       (is= 1 (boolean->binary true))
       (is= 0 (boolean->binary false))
       (throws? (boolean->binary))
       (throws? (boolean->binary 234)))

     (dotest
       (is= (td/search-triple-impl (quote [:a :b :c]))
         '(tupelo.data/search-triple-fn (quote [:a :b :c])))
       (is= (td/search-triple-impl (quote [a b c]))
         '(tupelo.data/search-triple-fn (quote [a b c])))

       (is= (td/search-triple 123 :color "Joey")
         [(td/tag-eid 123) :color "Joey"]))

     (dotest
       (with-tdb (new-tdb)
         (eid-count-reset)
         (let [edn-val     {:a {:b 2}}
               root-eid    (td/add-edn edn-val)
               search-spec [(search-triple x :a y)
                            (search-triple y :b 2)]]
           (is= (query-triples search-spec)
             [{:x 1001, :y 1002}])))

       (with-tdb (new-tdb)
         (eid-count-reset)
         (let [edn-val     {:a {:b 2}}
               root-eid    (td/add-edn edn-val)
               search-spec [(search-triple y :b 2) (search-triple x :a y)]]
           (is= (query-triples search-spec)
             [{:y 1002, :x 1001}])))

       (with-tdb (new-tdb)
         (eid-count-reset)
         (let [edn-val     {:a {:b 2}}
               root-eid    (td/add-edn edn-val)
               search-spec [(search-triple x :a y) (search-triple y :b 99)]]
           (is= [] (query-triples search-spec))))

       (with-tdb (new-tdb)
         (eid-count-reset)
         (let [edn-val     {:a {:b 2}}
               root-eid    (td/add-edn edn-val)
               search-spec [(search-triple y :b 99) (search-triple x :a y)]]
           (is= [] (query-triples search-spec)))))

     (dotest
       (let [map-triples (td/query->triples (quote [{:eid x :map y}
                                                         {:eid y :a a}]))]
         (is= (td/tagval-walk-compact map-triples)
           [[{:param :x} :map {:param :y}]
            [{:param :y} :a {:param :a}]]))

       (let [map-triples (td/query->triples (quote [{:eid ? :map y}]))]
         (is= (td/tagval-walk-compact map-triples)
           [[{:param :eid} :map {:param :y}]]))

       (let [map-triples (td/query->triples (quote [{:eid ? :map y}
                                                         {:eid y :a a}]))]
         (is= (td/tagval-walk-compact map-triples)
           [[{:param :eid} :map {:param :y}]
            [{:param :y} :a {:param :a}]]))

       (throws? (td/query->triples (quote [{:eid ? :map y}
                                                {:eid ? :a a}]))))

     (dotest
       (is (td/param-tmp-eid? (tag-param :tmp-eid-99999)))
       (is (td/tmp-attr-kw? :tmp-attr-99999)))

     ;---------------------------------------------------------------------------------------------------
     (def edn-val (glue (sorted-map)
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
         (let [root-hid (td/add-edn edn-val)]
           ; (spyx-pretty (grab :idx-eav (deref *tdb*)))
           (is= edn-val (td/eid->edn root-hid))
           (comment
             (let [idx-eav (vec (grab :idx-eav @*tdb*))]
               (spyx-pretty (td/tagval-walk-compact idx-eav)))
             ;result
             (td/tagval-walk-compact idx-eav) =>
             [[{:eid 1001} :hashmap {:eid 1002}]
              [{:eid 1001} :kw :nothing]
              [{:eid 1001} :map {:eid 1003}]
              [{:eid 1001} :num 5]
              [{:eid 1001} :str "hello"]
              [{:eid 1001} :vec {:eid 1004}]
              [{:eid 1002} :a 21]
              [{:eid 1002} :b 22]
              [{:eid 1003} :a 1]
              [{:eid 1003} :b 2]
              [{:eid 1004} {:idx 0} 5]
              [{:eid 1004} {:idx 1} 6]
              [{:eid 1004} {:idx 2} 7]])

           (when true
             (let [eids-match (td/index-find-leaf 1) ; only 1 match
                   entity-edn (td/eid->edn (<val (only eids-match)))]
               (is= entity-edn {:a 1, :b 2}))
             (is= (query-triples [(search-triple e :num v)])
               [{:e 1001, :v 5}])
             (is= (query-triples [(search-triple e a "hello")])
               [{:e 1001, :a :str}])
             (is= (query-triples [(search-triple e a 7)])
               [{:e 1004, :a 2}])))))

     (dotest
       (td/with-tdb (td/new-tdb)
         (td/eid-count-reset)
         (let [root-hid (td/add-edn edn-val)]
           ; (spyx-pretty (grab :idx-eav (deref *tdb*)))
           (is= edn-val (td/eid->edn root-hid))
           (is= edn-val (td/eid->edn 1001))
           (is= (td/eid->edn 1003) {:a 1 :b 2})
           (comment ; #todo API:  output should look like
             {:x 1001 :y 1002 :a 1})

           (let [r1 (only (td/query-triples [(search-triple e i 7)]))
                 r2 (only (td/index-find-leaf 7))]
             (is= r1 {:e 1004, :i 2})
             (is= (td/eid->edn 1004) [5 6 7])
             (is= r2 (tag-eid 1004))
             (is= (td/eid->edn (<val r2)) [5 6 7])))))

     (dotest
       (td/with-tdb (td/new-tdb)
         (td/eid-count-reset)
         (let [root-hid (td/add-edn edn-val)]
           ; (spyx-pretty (grab :idx-eav (deref *tdb*)))
           (is= edn-val (td/eid->edn root-hid)))))

     (dotest
       (td/with-tdb (td/new-tdb)
         (td/eid-count-reset)
         (let [root-hid (td/add-edn edn-val)]
           ; (spyx-pretty (grab :idx-eav (deref *tdb*)))
           (is= edn-val (td/eid->edn root-hid))

           (binding [td/*autosyms-seen* (atom #{})]
             (is= (symbol "a") (td/autosym-resolve :a (quote ?)))
             (throws? (td/autosym-resolve :a (quote ?)))) ;attempted duplicate throws

           (throws? (td/exclude-reserved-identifiers {:a {:tmp-eid-123 :x}}))
           (throws? (td/exclude-reserved-identifiers (quote {:a {:x [1 2 3 tmp-eid-123 4 5 6]}})))

           (is= (td/untag-query-results [{(tag-param :a) 1}])
             [{:a 1}])
           (is= (td/untag-query-results [{(tag-param :x) (tag-eid 1001),
                                          (tag-param :y) (tag-eid 1002),
                                          (tag-param :a) 1}])
             [{:x 1001, :y 1002, :a 1}])
           (is= (td/untag-query-results [{(tag-param :e) (tag-eid 1003)
                                          (tag-param :i) 2}])
             [{:e 1003, :i 2}])

           (is= (td/query [{:map          {:a a1}
                                 :hashmap {:a a2}}])
             [{:a1 1
               :a2 21}])

           (is-set= (td/query [{kk {:a ?}}]) ; Awesome!  Found both solutions!
             [{:kk :map, :a 1}
              {:kk :hashmap, :a 21}])

           (is= (only (td/query [{:num ?}])) {:num 5})
           (is= (only (td/query [{:eid ? :num ?}])) {:eid 1001, :num 5})
           (is= (only (td/query [{:eid ? :num num}])) {:eid 1001, :num 5}))))

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
               root-hid             (td/add-edn hospital)
               nm-sal-all           (td/query [{:first-name ? :salary ?}])
               nm-sal-attending     (td/query [{:first-name ? :salary ? :position :attending}])
               nm-sal-resident      (td/query [{:first-name ? :salary ? :position :resident}])
               nm-sal-volunteer     (td/query [{:first-name ? :salary ? :position :volunteer}])
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
               root-eid (td/add-edn edn-val)]
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

           (let [st    (td/search-triple-fn [(quote ?) (tag-idx 1) 2])
                 ; >> (spyx st)
                 found (td/query-triples [st])]
             (is= (td/eid->edn root-eid) {:aa [1 2 3]
                                          :bb [2 3 4]
                                          :cc [3 4 5 6]})
             (is= (td/eid->edn (val (t/only2 found))) [1 2 3]))

           (let [found    (td/query-triples [(td/search-triple eid idx 3)])
                 entities (mapv #(td/eid->edn (grab :eid %)) found)]
             (is-set= found
               [{:eid 1003, :idx 1}
                {:eid 1002, :idx 2}
                {:eid 1004, :idx 0}])
             (is-set= entities [[1 2 3] [2 3 4] [3 4 5 6]]))

           (is= (td/eid->edn (val (t/only2 (td/query-triples [(td/search-triple eid {:idx 2} 3)])))) [1 2 3])
           (is= (td/eid->edn (val (t/only2 (td/query-triples [(td/search-triple eid {:idx 1} 3)])))) [2 3 4])
           (is= (td/eid->edn (val (t/only2 (td/query-triples [(td/search-triple eid {:idx 0} 3)])))) [3 4 5 6])
           )))

     (dotest
       (td/with-tdb (td/new-tdb)
         (let [edn-val  {:a 1 :b 2}
               root-eid (td/add-edn edn-val)]
           (is= edn-val (td/eid->edn root-eid))))

       (td/with-tdb (td/new-tdb)
         (let [edn-val  [1 2 3]
               root-eid (td/add-edn edn-val)]
           (is= edn-val (td/eid->edn root-eid))))

       (td/with-tdb (td/new-tdb)
         (let [edn-val  #{1 2 3}
               root-eid (td/add-edn edn-val)]
           (is= edn-val (td/eid->edn root-eid))))

       (td/with-tdb (td/new-tdb)
         (let [edn-val  {:val "hello"}
               root-eid (td/add-edn edn-val)]
           (is= edn-val (td/eid->edn root-eid))))

       (td/with-tdb (td/new-tdb)
         (let [data-val {:a [{:b 2}
                             {:c 3}
                             {:d 4}]
                         :e {:f 6}
                         :g :green
                         :h "hotel"
                         :i 1}
               root-eid (td/add-edn data-val)]
           (is= data-val (td/eid->edn root-eid)))))

     (dotest
       (td/with-tdb (td/new-tdb)
         (let [edn-val    #{1 2 3}
               root-hid   (td/add-edn edn-val)
               ; >> (spyx-pretty (deref td/*tdb*))
               edn-result (td/eid->edn root-hid)]
           (is (set? edn-result)) ; ***** Sets are coerced to vectors! *****
           (is-set= [1 2 3] edn-result)))
       (td/with-tdb (td/new-tdb)
         (let [edn-val  #{:a 1 :b 2}
               root-hid (td/add-edn edn-val)]
           (is= edn-val (td/eid->edn root-hid))))
       (td/with-tdb (td/new-tdb)
         (let [edn-val  {:a 1 :b #{1 2 3}}
               root-hid (td/add-edn edn-val)]
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
               root-eid (td/add-edn data)]
           (is= 1001 root-eid)
           (let [found (td/query [{:a ?}])]
             (is= (td/eid->edn (val (only2 found)))
               [{:b 2} {:c 3} {:d 4}]))

           (let [found (td/query [{:a e1}
                                       {:eid e1 {:idx 0} val}])]
             (is= (td/eid->edn (:val (only found)))
               {:b 2}))

           (let [found (td/query-triples [(td/search-triple e1 :a e2)
                                          (td/search-triple e2 {:idx 2} e3)])]
             (is= (td/eid->edn (grab :e3 (only found))) {:d 4}))

           (let [found (td/query-triples [(td/search-triple e1 a1 e2)
                                          (td/search-triple e2 a2 e3)
                                          (td/search-triple e3 a3 4)])]
             (is= found [{:a1 :a
                          :a2 2
                          :a3 :d
                          :e1 1001
                          :e2 1002
                          :e3 1005}])
             (is= data (td/eid->edn (grab :e1 (only found)))))
           )))

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
               root-hid      (td/add-edn data)
               found         (td/query [{:eid ? a1 1}])
               eids          (mapv #(grab :eid %) found)
               one-leaf-maps (mapv #(td/eid->edn %) eids)]
           (is-set= found [{:eid 1008, :a1 :a} {:eid 1007, :a1 :a} {:eid 1002, :a1 :a}])
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
               root-hid (td/add-edn data)
               found    (td/query [{:eid ? :a 1}])]
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
               root-hid (td/add-edn data)]
           (let [found (td/query [{:eid ? :a 1}])]
             (is-set= (mapv #(td/eid->edn (grab :eid %)) found)
               [{:a 1, :b 1, :c 1}
                {:a 1, :b 2, :c 2}
                {:a 1, :b 1, :c 3}]))
           (let [found (td/query [{:eid ? :a 2}])]
             (is-set= (mapv #(td/eid->edn (grab :eid %)) found)
               [{:a 2, :b 2, :c 4}
                {:a 2, :b 1, :c 5}
                {:a 2, :b 2, :c 6}]))
           (let [found (td/query [{:eid ? :b 1}])]
             (is-set= (mapv #(td/eid->edn (grab :eid %)) found)
               [{:a 1, :b 1, :c 1}
                {:a 1, :b 1, :c 3}
                {:a 2, :b 1, :c 5}]))
           (let [found (td/query [{:eid ? :c 6}])]
             (is-set= (mapv #(td/eid->edn (grab :eid %)) found)
               [{:a 2, :b 2, :c 6}]))

           (let [found (td/query [{:eid ? :a 1 :b 2}])]
             (is-set= (mapv #(td/eid->edn (grab :eid %)) found)
               [{:a 1, :b 2, :c 2}]))
           (let [found (td/query [{:eid ? :a 1 :b 1}])]
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
               root-eid (td/add-edn data)]
           (is-set= (td/query [{:a [{:color cc}]}])
             [{:cc :red}
              {:cc :blue}
              {:cc :yellow}])))

       (td/with-tdb (td/new-tdb)
         (td/eid-count-reset)
         (let [data     {:a [{:id [2 22] :color :red}
                             {:id [3 33] :color :yellow}
                             {:id [4 44] :color :blue}]}
               root-eid (td/add-edn data)
               result   (td/query [{:a [{:eid eid-red :color :red}]}])]
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
               root-hid (td/add-edn data)
               result   (td/query [{:eid ? :a [{:id ?}]}])]
           (is-set= result
             [{:eid 1001, :id 2}
              {:eid 1001, :id 6}
              {:eid 1001, :id 3}
              {:eid 1001, :id 4}
              {:eid 1001, :id 5}])

           (is-set= (td/query [{:b {:eid ? :c [{:ident ?}]}}]) ; =>
             [{:eid 1008, :ident 2}
              {:eid 1008, :ident 4}
              {:eid 1008, :ident 3}]))))

     (dotest
       (td/with-tdb (td/new-tdb)
         (td/eid-count-reset)
         (let [data     {:a [{:id 2 :color :red}
                             {:id 3 :color :yellow}
                             {:id 4 :color :blue}]}
               root-hid (td/add-edn data)]
           (is= (td/eid->edn (grab :eid (only (td/query [{:eid ? :color :red}]))))
             {:color :red, :id 2})
           (is= (td/eid->edn (grab :eid (only (td/query [{:eid ? :id 4}]))))
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
               root-hid (td/add-edn data)]
           (is= (td/eid->edn (grab :eid (only (td/query [{:eid ?, :id 2}]))))
             {:color :red, :id 2})
           (is= (td/eid->edn (grab :eid (only (td/query [{:eid ?, :ident 2}]))))
             {:flower :rose, :ident 2})
           (is= (td/eid->edn (grab :eid (only (td/query [{:eid ?, :flower :rose}]))))
             {:flower :rose, :ident 2})
           (is= (td/eid->edn (grab :eid (only (td/query [{:eid ?, :color :pink}]))))
             {:color :pink, :id 5}))))

     (dotest
       (td/with-tdb (td/new-tdb)
         (let [
               root-eid         (td/add-edn skynet-widgets)
               search-results   (td/query [{:basic-info        {:producer-code ?}
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
               root-hid (td/add-edn data)]
           (let [found (td/query [{:eid eid :a 1}
                                       (search-triple eid :b 2)])]
             (is-set= (mapv #(td/eid->edn (grab :eid %)) found)
               [{:a 1, :b 2, :c 2}]))
           (let [found (td/query [{:eid eid :a 1}
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

                 root-eid (td/add-edn person)
                 ]
             (is-set= (distinct (td/query [{:zip ?}]))
               [{:zip "12345"}
                {:zip "46203"}])

             (is-set= (distinct (td/query [{:zip ? :city ?}]))
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

               root-eid (td/add-edn people)
               results  (td/query [{:name ? :addresses [{:address1 ? :zip "86753"}]}])]
           (is-set= results
             [{:name "jimmy", :address1 "543 Other St"}
              {:name "joel", :address1 "2026 park ave"}]))))

     (dotest
       (td/eid-count-reset)
       (td/with-tdb (td/new-tdb)
         (let [root-eid (td/add-edn users-and-accesses)]
           (let [results (td/query [{:people [{:name ? :id id}]}])]
             (is-set= results
               [{:name "jimmy", :id 1}
                {:name "joel", :id 2}
                {:name "tim", :id 3}]))

           (let [pref-zip-data     (td/query [{:addrs {id [{:zip ? :pref true}]}}])
                 user-id->pref-zip (apply glue
                                     (forv [m pref-zip-data]
                                       {(grab :id m) (grab :zip m)}))

                 access-data       (td/query [{:visits {id [{:date ? :geo-loc {:zip ?}}]}}])
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

           ; ***** this is the big one! *****
           (let [results (td/query
                           [{:people [{:name ? :id id}]}
                            {:addrs {id [{:zip ? :pref false}]}}
                            {:visits {id [{:date ? :geo-loc {:zip zip}}]}}])]
             (is= results [{:date "12-31-1900"
                            :id   1
                            :name "jimmy"
                            :zip  "00666"}]))

           ; can break it down into low level, including array index values
           (let [results-3 (td/query-triples [(search-triple eid-pers :name name)
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

           (let [results-4 (td/query [{:people      {:eid eid-pers :name ? :id ?}
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

     (comment ; <<comment>>
       )  ; <<comment>>

     (dotest
       (let [query-result {(tv :param :tmp-eid-36493) (tv :eid 1003)
                           (tv :param :name)          "jimmy"
                           (tv :param :date)          "12-25-1900"
                           (tv :param :zip-acc)       "11201"
                           (tv :param :id)            42
                           (tv :param :zip-pref)      "11201"}
             result       (td/eval-with-tagged-params query-result
                            (quote [
                                   ;(println :name-3 name)
                                   ;(println :id-4 id)
                                    (+ 42 7)]))]
         (is= result 49) ))

     (dotest
       (td/eid-count-reset)
       (td/with-tdb (td/new-tdb)
         (let [root-eid (td/add-edn users-and-accesses)]
           ; ***** this is the big one! *****
           (let [results (td/query
                           [{:people [{:name ? :id id}]
                             :addrs  {id [{:zip zip-pref :pref true}]}
                             :visits {id [{:date ? :geo-loc {:zip zip-acc}}]}}
                            (not= zip-acc zip-pref)])]
             (is= results
               [{:name "jimmy" :date "12-31-1900" :zip-acc "00666" :id 1 :zip-pref "11201"}
                {:name "joel" :date "1-1-1970" :zip-acc "12345" :id 2 :zip-pref "11753"}
                {:name "tim" :date "4-4-4444" :zip-acc "54221" :id 3 :zip-pref "11456"}])))))

     (dotest
       (td/eid-count-reset)
       (td/with-tdb (td/new-tdb)
         (let [root-eid (td/add-edn skynet-widgets)
               results  (td/query
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

     (comment
       (def a-1400 1401)
       (def b-1400 1402)
       (dotest
         (let [a 1
               b (inc a)]
           (println (quote [a b]))
           (println '{:out [~a ~b]}) ; ***** doesn't work *****
           (println `{:out [~a ~b]})
           ; (println (td/quote-template {:out (unquote [a b])})) ; ***** fails due to locals *****
           (println (td/quote-template {:out (unquote [a-1400 b-1400])})) ; globals are OK
           (println (td/quote-template {:a 1 :b (unquote (+ 2 3))})) ; global function works too
           (newline)
           (let [env {:a 1 :b 2}]
             (println :with-env
               (td/eval-with-env-map env
                 (quote [a b])))))))

     (dotest
       (let [env {:a 1 :b 2}]
         (t/with-map-vals env [a b]
           (is= {:likes {:a a :b b}} ; normal
             {:likes {:a 1, :b 2}})
           (is= (td/construct-impl (quote {:likes {:a ? :b ?}}))
             (quote {:likes {:a a, :b b}}))
           (is= (td/construct {:likes {:a ? :b ?}})
             {:likes {:a 1, :b 2}}))))








     ))

