;   Copyright (c) Alan Thompson. All rights reserved.
;   The use and distribution terms for this software are covered by the Eclipse Public License 1.0
;   (http://opensource.org/licenses/eclipse-1.0.php) which can be found in the file epl-v10.html at
;   the root of this distribution.  By using this software in any fashion, you are agreeing to be
;   bound by the terms of this license.  You must not remove this notice, or any other, from this
;   software.
(ns tst.tupelo.clubdata
  (:require
    [clojure.test] ; sometimes this is required - not sure why
    [clojure.java.io :as io]
    [tupelo.testy :refer [deftest testing is dotest dotest-focus isnt is= isnt= is-set= is-nonblank=
                          throws? throws-not? define-fixture]]
    [tupelo.core :as t :refer [spy spyx spyxx spy-pretty spyx-pretty spyq unlazy let-spy with-spy-indent
                               only only2 forv glue grab nl keep-if drop-if ->sym xfirst xsecond xthird not-nil?
                               it-> fetch-in with-map-vals map-plain? append prepend
                               ]]
    [tupelo.data :as td :refer [with-tdb new-tdb eid-count-reset lookup match-triples match-triples->tagged search-triple
                                *tdb* ->Eid Eid? ->Idx Idx? ->Prim Prim? ->Param Param?
                                ]]
    [tupelo.csv :as csv]
    [tupelo.string :as ts]) )


(dotest
  (when true
    (let [fac-str      (ts/quotes->double (slurp (io/resource "facilities.csv")))
          fac-entities (csv/parse->entities fac-str)]
      ; (println fac-str)
      ; (spyx-pretty fac-entities)
      (spit "resources/facilities.edn" (t/pretty-str (unlazy fac-entities))))
    (let [memb-str      (ts/quotes->double (slurp (io/resource "members.csv")))
          ;  >>            (println memb-str)
          memb-entities (csv/parse->entities memb-str)]
      ; (spyx-pretty memb-entities)
      (spit "resources/members.edn" (t/pretty-str (unlazy memb-entities))))
    (let [book-str      (ts/quotes->double (slurp (io/resource "bookings.csv")))
          ; >> (println book-str)
          book-entities (csv/parse->entities book-str)]
      ; (spyx-pretty book-entities)
      (spit "resources/bookings.edn" (t/pretty-str (unlazy book-entities))))
    ))
