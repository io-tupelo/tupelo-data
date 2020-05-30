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
                               it-> cond-it-> fetch-in with-map-vals map-plain? append prepend
                               ]]
    [tupelo.data :as td :refer [with-tdb new-tdb eid-count-reset lookup match-triples match-triples->tagged search-triple
                                *tdb* ->Eid Eid? ->Idx Idx? ->Prim Prim? ->Param Param?
                                ]]
    [clojure.string :as str]
    [clojure.tools.reader.edn :as edn]
    [schema.core :as s]
    [tupelo.csv :as csv]
    [tupelo.parse :as parse]
    [tupelo.profile :as prof]
    [tupelo.schema :as tsk]
    [tupelo.string :as ts]
    ) )


(when false
  (dotest
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
      (spit "resources/bookings.edn" (t/pretty-str (unlazy book-entities))))))

(s/defn normalize-facility :- tsk/KeyMap
  [arg :- tsk/KeyMap]
  (glue
    arg
    (t/map-vals
      (t/submap-by-keys arg [:facid ])
      parse/parse-int)
    (t/map-vals
      (t/submap-by-keys arg [:guestcost :initialoutlay :membercost :monthlymaintenance])
      parse/parse-float)))

(s/defn normalize-member :- tsk/KeyMap
  [arg :- tsk/KeyMap]
  (glue
    arg
    (t/map-vals
      (t/submap-by-keys arg [:memid])
      parse/parse-int)
    (t/map-vals
      (t/submap-by-keys arg [:recommendedby])
      (fn [arg]
        (cond-it-> arg
          (ts/lowercase= it "null") nil)))))

(s/defn normalize-booking :- tsk/KeyMap
  [arg :- tsk/KeyMap]
  (glue
    arg
    (t/map-vals
      (t/submap-by-keys arg [:bookid :facid :memid :slots  ])
      parse/parse-int)))

(dotest-focus
  (let [facilities (mapv normalize-facility
                     (edn/read-string (slurp (io/resource "facilities.edn"))))
        members    (mapv normalize-member
                     (edn/read-string (slurp (io/resource "members.edn"))))
        bookings   (mapv normalize-booking
                     (edn/read-string (slurp (io/resource "bookings.edn"))))

        facilities-3       (vec (take 3 facilities))
        members-3      (vec (take 3 members))
        bookings-3      (vec (take 3 bookings))]
    (spyx (count facilities))
    (spyx (count members))
    (spyx (count bookings))

(when false
      (nl) (spyx-pretty facilities-3)
      (nl) (spyx-pretty members-3)
      (nl) (spyx-pretty bookings-3))
    (is= facilities-3 [{:facid              0,
                :guestcost          25.0,
                :initialoutlay      10000.0,
                :membercost         5.0,
                :monthlymaintenance 200.0,
                :name               "Tennis Court 1"}
               {:facid              1,
                :guestcost          25.0,
                :initialoutlay      8000.0,
                :membercost         5.0,
                :monthlymaintenance 200.0,
                :name               "Tennis Court 2"}
               {:facid              2,
                :guestcost          15.5,
                :initialoutlay      4000.0,
                :membercost         0.0,
                :monthlymaintenance 50.0,
                :name               "Badminton Court"}])

    (is= members-3 [{:address       "<nowhere>",
                 :firstname     "GUEST",
                 :joindate      "2012-07-01 00:00:00",
                 :memid         0,
                 :recommendedby nil,
                 :surname       "GUEST",
                 :telephone     "000 000-0000",
                 :zipcode       "0"}
                {:address       "8 Bloomsbury Close; Boston",
                 :firstname     "Darren",
                 :joindate      "2012-07-02 12:02:05",
                 :memid         1,
                 :recommendedby nil,
                 :surname       "Smith",
                 :telephone     "555-555-5555",
                 :zipcode       "4321"}
                {:address       "8 Bloomsbury Close; New York",
                 :firstname     "Tracy",
                 :joindate      "2012-07-02 12:08:23",
                 :memid         2,
                 :recommendedby nil,
                 :surname       "Smith",
                 :telephone     "555-555-5555",
                 :zipcode       "4321"}])

    (is= bookings-3 [{:bookid    0,
                 :facid     3,
                 :memid     1,
                 :slots     2,
                 :starttime "2012-07-03 11:00:00"}
                {:bookid    1,
                 :facid     4,
                 :memid     1,
                 :slots     2,
                 :starttime "2012-07-03 08:00:00"}
                {:bookid    2,
                 :facid     6,
                 :memid     0,
                 :slots     2,
                 :starttime "2012-07-03 18:00:00"}])

    (eid-count-reset)
    (prof/timer-stats-reset)
    (with-tdb (new-tdb)
      (let [
            >> (println :-----start)
            root-fac     (td/add-entity-edn facilities)
            >> (println :-----fac)
            root-memb    (td/add-entity-edn members)
            >> (println :-----memb)
            root-book (td/add-entity-edn (take 9900 bookings))
            >> (println :-----book)

            ;eid0  (td/match [{:eid ? :facid 0}])
            ;>> (spyx (take 9 eid0))

            ;eid0b (grab :eid (only (td/match [{:eid ? :name "Tennis Court 1"}])))
            ;>> (spyx eid0b)
            ;  fac0         (td/eid->edn eid0b)
            ; (spyx fac0)

            bookings-tc1 (prof/with-timer-print :match-big-0
                           (td/match [{:facid facid :name "Tennis Court 1"}
                                      {:facid facid :memid memid :starttime ?}
                                      {:memid memid :firstname ?}]))
            guests-tc1 (set (mapv #(grab :firstname %) bookings-tc1))
            ]
        (spyx-pretty (take 5 bookings-tc1))
        (spyx  guests-tc1)

        )
      )
    (prof/print-profile-stats)
    ))




