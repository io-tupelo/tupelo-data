;   Copyright (c) Alan Thompson. All rights reserved.
;   The use and distribution terms for this software are covered by the Eclipse Public License 1.0
;   (http://opensource.org/licenses/eclipse-1.0.php) which can be found in the file epl-v10.html at
;   the root of this distribution.  By using this software in any fashion, you are agreeing to be
;   bound by the terms of this license.  You must not remove this notice, or any other, from this
;   software.
(ns tst.tupelo.tag
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
            [tupelo.tag :as tv]
            [tupelo.string :as ts]))
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
     (let [x (tv/new :stuff 5)]
       (is (tv/tagged-value? x))
       (isnt (tv/tagged-value? {:a 1 :b 2}))
       (isnt (tv/tagged-value? [:a 1] ))
       (is= x {:stuff 5})
       (is= :stuff (tv/tag x))
       (is= 5 (tv/val x))))

   ))

