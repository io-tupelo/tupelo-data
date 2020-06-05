;   Copyright (c) Alan Thompson. All rights reserved.
;   The use and distribution terms for this software are covered by the Eclipse Public License 1.0
;   (http://opensource.org/licenses/eclipse-1.0.php) which can be found in the file epl-v10.html at
;   the root of this distribution.  By using this software in any fashion, you are agreeing to be
;   bound by the terms of this license.  You must not remove this notice, or any other, from this
;   software.
(ns tst.tupelo.graph
  ;---------------------------------------------------------------------------------------------------
  ;   https://code.thheller.com/blog/shadow-cljs/2019/10/12/clojurescript-macros.html
  ;   http://blog.fikesfarm.com/posts/2015-12-18-clojurescript-macro-tower-and-loop.html
  #?(:cljs (:require-macros
             [tupelo.core]
             [tupelo.graph]
             [tupelo.testy]
             ; #todo finish dotest-focus for tupelo.testy
             ; #todo finish tupelo.testy => tupelo.test
             ))
  (:require
    [clojure.test] ; sometimes this is required - not sure why
    [schema.core :as s]
    [tupelo.testy :refer [deftest testing is dotest dotest-focus isnt is= isnt= is-set= is-nonblank=
                          throws? throws-not? define-fixture]]
    [tupelo.core :as t :refer [spy spyx spyxx spy-pretty spyx-pretty spyq unlazy let-spy with-spy-indent
                               only only2 forv glue grab nl keep-if drop-if ->sym xfirst xsecond xthird not-nil?
                               it-> fetch-in with-map-vals map-plain? append prepend
                               ]]
    [tupelo.graph :as g :refer [with-grf new-grf new-grf-reset id-reset *grf* add-node get-node
                                add-rel get-rel
                                ]]
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
(comment  ; <<comment>>
  )       ; <<comment>>

(dotest-focus
  (g/with-grf (new-grf-reset)
    (let [nid-jack (g/add-node {:tag :person :name "Jack"})
          nid-jill (g/add-node {:tag :person :name "Jill"})
          rid-1    (g/add-rel {:tag   :likes
                               :from  nid-jack
                               :to    nid-jill
                               :since "Monday"})
          rid-2    (g/add-rel {:tag   :tolerates
                               :from  nid-jill
                               :to    nid-jack
                               :since "forever"}) ]
      (is= nid-jack 1001)
      (is= nid-jill 1002)
      (is= rid-1 2001)
      (is= rid-2 2002)
      (is= (get-node nid-jack) {:nid   1001
                                :tag   :person
                                :props {:name "Jack"}
                                :rels  {:from [2001]
                                        :to   [2002]}})
      (is= (get-node nid-jill) {:nid   1002
                                :tag   :person
                                :props {:name "Jill"}
                                :rels  {:from [2002]
                                        :to   [2001]}})
      (is= (get-rel rid-1)
        {:rid   2001
         :tag   :likes
         :from  1001
         :to    1002
         :props {:since "Monday"}})
      (is= (get-rel rid-2)
        {:rid   2002
         :tag   :tolerates
         :from  1002
         :to    1001
         :props {:since "forever"}})
      )))





















