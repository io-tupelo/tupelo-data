;   Copyright (c) Alan Thompson. All rights reserved.
;   The use and distribution terms for this software are covered by the Eclipse Public License 1.0
;   (http://opensource.org/licenses/eclipse-1.0.php) which can be found in the file epl-v10.html at
;   the root of this distribution.  By using this software in any fashion, you are agreeing to be
;   bound by the terms of this license.  You must not remove this notice, or any other, from this
;   software.
(ns tupelo.tag
  (:refer-clojure :exclude [load ->VecNode val])
  #?(:clj (:require
            [tupelo.core :as t :refer [spy spyx spyxx spyx-pretty with-spy-indent
                                       grab glue map-entry indexed
                                       forv vals->map fetch-in let-spy xfirst xsecond xthird xlast xrest
                                       keep-if drop-if append prepend
                                       ]]
            [tupelo.schema :as tsk]

            [schema.core :as s]
            [tupelo.lexical :as lex]))
  #?(:cljs (:require
             [tupelo.core :as t :refer [spy spyx spyxx spyx-pretty grab]] ; #todo :include-macros true
             [tupelo.schema :as tsk]

             [schema.core :as s]
             ))
  )

#?(:cljs (enable-console-print!))


#?(:clj
   (do

     (def TaggedValue {s/Any s/Any}) ; a map with a SINGLE entry

     (s/defn tagged-value? :- s/Bool
       "Returns true iff arg is a TaggedValue"
       [arg] (and (map? arg)
               (= 1 (count arg))))

     (s/defn tag :- s/Any
       [tv :- TaggedValue]
       (clojure.core/key
         (t/only
           (t/validate tagged-value? tv))))

     (s/defn val :- s/Any
       [tv :- TaggedValue]
       (clojure.core/val
         (t/only
           (t/validate tagged-value? tv))))

     (defn new
       "Create a new TaggedValue"
       [tag val]
       {tag val})

))


