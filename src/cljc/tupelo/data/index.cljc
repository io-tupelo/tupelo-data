;   Copyright (c) Alan Thompson. All rights reserved.
;   The use and distribution terms for this software are covered by the Eclipse Public License 1.0
;   (http://opensource.org/licenses/eclipse-1.0.php) which can be found in the file epl-v10.html at
;   the root of this distribution.  By using this software in any fashion, you are agreeing to be
;   bound by the terms of this license.  You must not remove this notice, or any other, from this
;   software.
(ns tupelo.data.index
  #?(:cljs (:require-macros tupelo.data.index))
  (:require
    [clojure.data.avl :as avl]
    [schema.core :as s]
    [tupelo.schema :as tsk]
    [tupelo.lexical :as lex]
    [tupelo.profile :as prof :refer [defnp ]]
    [tupelo.core :as t :refer [spy spyx spyxx spyx-pretty grab glue map-entry indexed
                               grab forv vals->map fetch-in ]]
    ))

; #todo add indexes
; #todo add sets (primative only or HID) => map with same key/value
; #todo copy destruct syntax for search

#?(:cljs (enable-console-print!))

; #todo Tupelo Data Language (TDL)

;---------------------------------------------------------------------------------------------------
; Plumatic Schema type definition
(def Index #{tsk/Vec})

(s/defn empty-index
  "Returns an empty lexically-sorted index"
  [] (avl/sorted-set-by lex/compare-lex))

(s/defn ->index :- Index
  "Converts a sequence (vec or set) into a lexically-sorted index"
  [some-set :- (s/cond-pre tsk/Set tsk/Vec)]
  (into (empty-index) some-set))
; #todo add (->sorted-map <map>)        => (into (sorted-map) <map>)
; #todo add (->sorted-vec <sequential>) => (vec (sort <vec>))

(s/defn bound-lower :- tsk/Vec
  "Given a lexical value as a vector such as [1 :a], returns a lower bound like [1]"
  [val :- tsk/Vec]
  (when (zero? (count val))
    (throw (ex-info "Cannot find lower bound for empty vec" {:val val})))
  (t/xbutlast val))

(s/defn prefix-match? :- s/Bool
  "Returns true if the sample value equals the pattern when truncated to the same length"
  [pattern :- tsk/Vec
   sample :- tsk/Vec]
  (= pattern (t/xtake (count pattern) sample)))

(s/defn split-key-prefix :- {s/Keyword Index}
  "Like clojure.data.avl/split-key, but allows prefix matches. Given a lexically sorted set like:
    #{[:a 1]
      [:a 2]
      [:a 3]
      [:b 1]
      [:b 2]
      [:b 3]
      [:c 1]
      [:c 2]}
   splits data by prefix match for patterns like [:b], returning a map of 3 sorted sets like:
  {:smaller #{[:a 1]
              [:a 2]
              [:a 3]}
   :matches #{[:b 1]
              [:b 2]
              [:b 3]}
   :larger  #{[:c 1]
              [:c 2]} ]
      "
  [match-val :- tsk/Vec
   lex-set :- Index]
  (prof/with-timer-accum :split-key-prefix
    (let [[smaller-set found-val larger-set] (avl/split-key match-val lex-set)
          result (if (t/not-nil? found-val)
                   {:smaller smaller-set
                    :matches (avl/sorted-set found-val)
                    :larger  larger-set}
                   (let [[matches-seq larger-seq] (clojure.core/split-with #(prefix-match? match-val %) larger-set)]
                     {:smaller smaller-set
                      :matches (->index matches-seq)
                      :larger  (->index larger-seq)}))]
      (when false
        (s/validate Index (grab :smaller result))
        (s/validate Index (grab :matches result))
        (s/validate Index (grab :larger result)))
      result)))

(s/defn prefix-match->index :- Index
  "Return the `:matches` values found via `split-key-prefix`."
  [match-val :- tsk/Vec
   lex-set :- Index]
  (t/grab :matches (split-key-prefix match-val lex-set)))

(s/defn prefix-match->seq :- [tsk/Triple]
  "Degenerate implementation of `split-key-prefix` that returns only the `:matches` value as a seq."
  [match-val :- tsk/Vec
   lex-set :- Index]
  (let [[-smaller-set- found-val larger-set] (avl/split-key match-val lex-set)]
    (if (t/not-nil? found-val)
      [found-val]
      (let [matches-seq (clojure.core/take-while #(prefix-match? match-val %) larger-set)]
        matches-seq))))

; #todo add-entry & remove-entry instead of conj/disj  ???
(s/defn add-entry
  "Add an entry to the index, returning the modified index"
  [index :- Index
   entry :- tsk/Vec]
  (conj index entry))

(s/defn remove-entry
  "Remove an entry to the index, returning the modified index. Throws if entry not found in index."
  [index :- Index
   entry :- tsk/Vec]
  (when-not (contains? index entry)
    (throw (ex-info "entry not found in index" (vals->map entry))))
  (disj index entry))


