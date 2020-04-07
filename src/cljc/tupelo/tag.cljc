;   Copyright (c) Alan Thompson. All rights reserved.
;   The use and distribution terms for this software are covered by the Eclipse Public License 1.0
;   (http://opensource.org/licenses/eclipse-1.0.php) which can be found in the file epl-v10.html at
;   the root of this distribution.  By using this software in any fashion, you are agreeing to be
;   bound by the terms of this license.  You must not remove this notice, or any other, from this
;   software.
(ns tupelo.tag
  (:refer-clojure :exclude [load ->VecNode val ])
  #?(:clj (:require
            [tupelo.core :as t :refer [spy spyx spyxx spyx-pretty with-spy-indent
                                       grab glue map-entry indexed
                                       forv vals->map fetch-in let-spy xfirst xsecond xthird xlast xrest
                                       keep-if drop-if append prepend
                                       ]]
            [tupelo.data.index :as index]
            [tupelo.schema :as tsk]
            [tupelo.vec :as vec]

            [clojure.set :as set]
            [schema.core :as s]
            [tupelo.lexical :as lex]))
  #?(:cljs (:require
             [tupelo.core :as t :refer [spy spyx spyxx spyx-pretty grab]] ; #todo :include-macros true
             [tupelo.schema :as tsk]
             [tupelo.data.index :as index]

             [clojure.set :as set]
             [schema.core :as s]
             ))
  )

#?(:cljs (enable-console-print!))


#?(:clj
   (do

     (defprotocol ITag (tag [tag]))
     (defprotocol IVal (val [this]))

     (defrecord TaggedValue [tag value]
       ITag (tag [this] tag)
       IVal (val [this] value))


))


