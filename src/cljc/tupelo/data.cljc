;   Copyright (c) Alan Thompson. All rights reserved.
;   The use and distribution terms for this software are covered by the Eclipse Public License 1.0
;   (http://opensource.org/licenses/eclipse-1.0.php) which can be found in the file epl-v10.html at
;   the root of this distribution.  By using this software in any fashion, you are agreeing to be
;   bound by the terms of this license.  You must not remove this notice, or any other, from this
;   software.
(ns tupelo.data
  (:refer-clojure :exclude [load ->VecNode])
  ; #?(:clj (:use tupelo.core)) ; #todo remove for cljs
  #?(:clj (:require
            [tupelo.core :as t :refer [spy spyx spyxx spyx-pretty with-spy-indent
                                       grab glue map-entry indexed only only2
                                       it-> forv vals->map fetch-in let-spy xfirst xsecond xthird xlast xrest
                                       keep-if drop-if append prepend
                                       ]]
            [tupelo.data.index :as index]
            [tupelo.lexical :as lex]
            [tupelo.schema :as tsk]
            [tupelo.vec :as vec]

            [clojure.walk :as walk]
            [schema.core :as s]
            [clojure.core :as cc]))
  )

; #todo Treeify: {k1 v1 k2 v2} =>
; #todo   {:data/id 100 :edn/type :edn/map ::kids [101 102] }
; #todo     {:data/id 101 :edn/type :edn/MapEntry  :edn/MapEntryKey 103  :edn/MapEntryVal 104}
; #todo     {:data/id 102 :edn/type :edn/MapEntry  :edn/MapEntryKey 105  :edn/MapEntryVal 106}
; #todo       {:data/id 103 :edn/type :edn/primitive  :data/type :data/keyword   :data/value k1 }
; #todo       {:data/id 104 :edn/type :edn/primitive  :data/type :data/int       :data/value v1 }
; #todo       {:data/id 105 :edn/type :edn/primitive  :data/type :data/keyword   :data/value k1 }
; #todo       {:data/id 106 :edn/type :edn/primitive  :data/type :data/int       :data/value v1 }
; #todo Treeify: [v0 v1] =>
; #todo   {:data/id 100 :edn/type :edn/list ::kids [101 102] }
; #todo     {:data/id 101 :edn/type :edn/ListEntry  :edn/ListEntryIdx 0  :edn/ListEntryVal 103}
; #todo     {:data/id 102 :edn/type :edn/ListEntry  :edn/ListEntryIdx 1  :edn/ListEntryVal 104}
; #todo       {:data/id 103 :edn/type :edn/primitive  :data/type :data/string  :data/value v0 }
; #todo       {:data/id 104 :edn/type :edn/primitive  :data/type :data/string  :data/value v1 }

; #todo Add destruct features in search (hiccup for db search):
; #todo   basic:  (find [ {:hid ? :kid id5} { :eid id5 :value 5} ]) ; parent of node with {:value 5}
; #todo   better: (find [ {:hid ? :kid [{ :value 5}]} ]) ; parent of node with {:value 5}

; #todo also use for logging, debugging, metrics

; #todo add enflame-style subscriptions/flames (like db views, but with notifications/reactive)
; #todo add sets (primative only or EID) => map with same key/value
; #todo copy destruct syntax for search

; #todo gui testing: add repl fn (record-start/stop) (datapig-save <name>) so can recording events & result db state

#?(:cljs (enable-console-print!))


#?(:clj
   (do

     ; #todo (spyl value) prints:   spy-line-xxxx => value
     (defn spyq ; #todo => tupelo.core/spy
       "(spyq <value>) - Spy Quiet
             This variant is intended for use in very simple situations and is the same as the
             2-argument arity where <msg-string> defaults to 'spy'.  For example (spy (+ 2 3))
             prints 'spy => 5' to stdout.  "
       [value]
       (when t/*spy-enabled*
         (println (str (t/spy-indent-spaces) (pr-str value))))
       value)

     ; #todo Tupelo Data Language (TDL)

     ;-----------------------------------------------------------------------------
     (defprotocol IVal (<val [this]))
     (defprotocol ITag (<tag [this]))

     (defrecord TagVal
       [tag val]
       ITag (<tag [this] tag)
       IVal (<val [this] val))

     ; ***** fails strangely under lein-test-refresh after 1st file save if define this using Plumatic schema *****
     (defn tagged? ; :- s/Bool
       [arg] (= TagVal (type arg)))

     (s/defn untagged :- s/Any
       [arg :- s/Any]
       (t/cond-it-> arg
         (tagged? arg) (<val arg)))

     (defmethod print-method TagVal
       [tv ^java.io.Writer writer]
       (.write writer
         (format "<%s %s>" (<tag tv) (<val tv))))

     (s/defn tagval-map? :- s/Bool
       "Returns true iff arg is a map that looks like a TagVal record:  {:tag :something :val 42}"
       [item]
       (and (map? item)
         (= #{:tag :val} (set (keys item)))))

     (defn tagval-walk-compact
       "Walks a data structure, converting any TagVal record like
         {:tag :something :val 42}  =>  {:something 42}"
       [data]
       (t/walk-with-parents data
         {:leave (fn [-parents- item]
                   (t/cond-it-> item
                     (or (tagged? item)
                       (tagval-map? item))
                     {(grab :tag item) (grab :val item)}))}))

     ;-----------------------------------------------------------------------------
     (defn unquote-form?
       [arg]
       (and (list? arg)
         (= (quote unquote) (first arg))))

     (defn unquote-splicing-form?
       [arg]
       (and (list? arg)
         (= (quote unquote-splicing) (first arg))))

     (defn quote-template-impl
       [form]
       (walk/prewalk
         (fn [item]
           (cond
             (unquote-form? item) (eval (xsecond item))
             (sequential? item) (let [unquoted-vec (apply glue
                                                     (forv [it item]
                                                       (if (unquote-splicing-form? it)
                                                         (eval (xsecond it))
                                                         [it])))
                                      final-result (if (list? item)
                                                     (t/->list unquoted-vec)
                                                     unquoted-vec)]
                                  final-result)
             :else item))
         form))

     (defmacro quote-template
       [form]
       (quote-template-impl form))

     (do  ; #todo => tupelo.core
       ;-----------------------------------------------------------------------------
       (def ^:dynamic *cumulative-val*
         "A dynamic Var pointing to an `atom`. Used by `with-cum-val` to accumulate state,
         such as in a vector or map.  Typically manipulated via helper functions such as
         `cum-val-set-it` or `cum-vector-append`. Can also be manipulated directly via `swap!` et al."
         nil)

       (defmacro cum-val-set-it
         "Works inside of a `with-cum-val` block to append a new val value."
         [& forms]
         `(swap! *cumulative-val*
            (fn [~'it]
              ~@forms)))

       (defmacro with-cum-val
         "Wraps forms containing `cum-val-set-it` calls to accumulate values into a vector."
         [init-val & forms]
         `(binding [*cumulative-val* (atom ~init-val)]
            (do ~@forms)
            (deref *cumulative-val*)))

       ;-----------------------------------------------------------------------------
       (s/defn cum-vector-append :- s/Any
         "Works inside of a `with-cum-vector` block to append a new vector value."
         [value :- s/Any] (cum-val-set-it (append it value)))

       (defmacro with-cum-vector
         "Wraps forms containing `cum-vector-append` calls to accumulate values into a vector."
         [& forms]
         `(with-cum-val []
            ~@forms))

       ;-----------------------------------------------------------------------------
       (s/defn only? :- s/Bool
         "Returns true iff collection has length=1"
         [coll :- s/Any] (and (t/has-length? coll 1)))
       (s/defn only2? :- s/Bool
         "Returns true iff arg is two nested collections of length=1"
         [coll :- s/Any] (and (t/has-length? coll 1)
                           (t/has-length? (first coll) 1))))

     ;---------------------------------------------------------------------------------------------------
     (do  ; keep these in sync
       (def EidType
         "The Plumatic Schema type name for a pointer to a tdb node (abbrev. for Hex ID)"
         s/Int)
       ;(s/defn eid? :- s/Bool ; #todo keep?  rename -> validate-eid  ???
       ;  "Returns true iff the arg type is a legal EID value"
       ;  [arg] (int? arg))
       )

     (do  ; keep these in sync
       (def AttrType
         "The Plumatic Schema type name for an attribute"
         (s/cond-pre s/Keyword s/Int))
       ;(s/defn attr? :- s/Bool ; #todo keep?
       ;  "Returns true iff the arg type is a legal attribute value"
       ;  [arg] (or (keyword? arg) (int? arg)))
       )

     (do  ; keep these in sync
       (def LeafType (s/maybe ; maybe nil
                       (s/cond-pre s/Num s/Str s/Keyword))) ; instant, uuid, Time ID (TID) (as strings?)
       (s/defn leaf-val? :- s/Bool
         "Returns true iff a value is of leaf type (number, string, keyword, nil)"
         [arg :- s/Any] (or (nil? arg) (number? arg) (string? arg) (keyword? arg) (boolean? arg)))
       )

     ; #todo add tsk/Set
     (do  ; keep these in sync
       (def EntityType (s/cond-pre tsk/Map tsk/Vec))
       (s/defn entity-like? [arg] (or (map? arg) (set? arg) (sequential? arg))))

     (def TripleIndex #{tsk/Triple})

     ;-----------------------------------------------------------------------------
     ; #todo => tupelo.ocre
     (defn sorted-map-generic
       "Returns a sorted map with a generic comparitor"
       [] (sorted-map-by lex/compare-generic))
     (defn sorted-set-generic
       "Returns a sorted map with a generic comparitor"
       [] (sorted-set-by lex/compare-generic))

     ;-----------------------------------------------------------------------------
     ; #todo inline all of these?
     (s/defn tag-param :- TagVal
       [arg :- s/Any] (->TagVal :param (t/->kw arg)))
     (s/defn tag-eid :- TagVal
       [eid :- s/Int] (->TagVal :eid (t/validate int? eid)))
     (s/defn tag-idx :- TagVal
       [idx :- s/Int] (->TagVal :idx (t/validate int? idx)))

     ; (defn pair-map? [arg] (and (map? arg) (= 1 (count arg))))
     (defn tagged-param? [x] (and (= TagVal (type x)) (= :param (<tag x))))
     (defn tagged-eid? [x] (and (= TagVal (type x)) (= :eid (<tag x))))

     (defn idx-literal? [x] (and (map? x) (= [:idx] (keys x))))
     (defn idx-literal->tagged [x] (tag-idx (only (vals x))))

     ;-----------------------------------------------------------------------------
     (s/defn tmp-eid-prefix-str? :- s/Bool
       "Returns true iff arg is a String like `tmp-eid-99999`"
       [arg :- s/Str]
       (.startsWith arg "tmp-eid-"))

     (s/defn tmp-eid-sym? :- s/Bool
       "Returns true iff arg is a symbol like `tmp-eid-99999`"
       [arg :- s/Any] (and (symbol? arg) (tmp-eid-prefix-str? (t/sym->str arg))))

     (s/defn tmp-eid-kw? :- s/Bool
       "Returns true iff arg is a symbol like `tmp-eid-99999`"
       [arg :- s/Any] (and (keyword? arg) (tmp-eid-prefix-str? (t/kw->str arg))))

     (s/defn ^:no-doc param-tmp-eid? :- s/Bool
       "Returns true iff arg is a map that looks like {:param :tmp-eid-99999}"
       [arg]
       (and (tagged-param? arg)
         (tmp-eid-kw? (<val arg))))

     ;-----------------------------------------------------------------------------
     (s/defn ^:no-doc tmp-attr-prefix-str? :- s/Bool
       [arg :- s/Str]
       (.startsWith arg "tmp-attr-"))

     (s/defn ^:no-doc tmp-attr-sym? :- s/Bool
       [arg :- s/Any] (and (symbol? arg)
                        (tmp-attr-prefix-str? (t/sym->str arg))))

     (s/defn ^:no-doc tmp-attr-kw? :- s/Bool
       [arg :- s/Any] (and (keyword? arg)
                        (tmp-attr-prefix-str? (t/kw->str arg))))

     (s/defn ^:no-doc tmp-attr-param? :- s/Bool
       "Returns true iff arg is a map that looks like {:param :tmp-attr-99999}"
       [arg] (and (tagged-param? arg)
               (tmp-attr-kw? (<val arg))))

     ;-----------------------------------------------------------------------------
     (def ^:dynamic ^:no-doc *tdb* nil)

     (defmacro with-tdb ; #todo swap names?
       [tdb-arg & forms]
       `(binding [*tdb* (atom ~tdb-arg)]
          ~@forms))

     (def TDB
       "Plumatic Schema type definition for tupelo.data DB"
       {:eid-type {TagVal s/Keyword}
        :idx-eav  #{tsk/Triple}
        :idx-vae  #{tsk/Triple}
        :idx-ave  #{tsk/Triple}})

     (s/defn new-tdb :- TDB
       "Returns a new, empty db."
       []
       (into (sorted-map)
         {:eid-type (sorted-map-generic) ; source type of entity (:map :array :set)
          :idx-eav  (index/empty-index)
          :idx-vae  (index/empty-index)
          :idx-ave  (index/empty-index)
          }))

     (s/defn db-pretty :- tsk/KeyMap
       "Returns a pretty version of the DB"
       [db :- TDB]
       (let [db-compact (tagval-walk-compact db) ; returns plain maps & sets instead of sorted or index
             result     (it-> (new-tdb)
                          (update it :eid-type glue (grab :eid-type db-compact))
                          (update it :idx-ave glue (grab :idx-ave db-compact))
                          (update it :idx-eav glue (grab :idx-eav db-compact))
                          (update it :idx-vae glue (grab :idx-vae db-compact)))]
         result))

     (def ^:no-doc eid-count-base 1000)
     (def ^:no-doc eid-counter (atom eid-count-base))

     (defn ^:no-doc eid-count-reset
       "Reset the eid-count to its initial value"
       [] (reset! eid-counter eid-count-base))

     (s/defn ^:no-doc new-eid :- EidType
       "Returns the next integer EID"
       [] (swap! eid-counter inc))

     (s/defn ^:no-doc add-edn-impl :- TagVal ; EidType ; #todo maybe rename:  load-edn->eid  ???
       [edn-in :- s/Any]
       ;(spyq :-----------------------------------------------------------------------------)
       ;(newline)
       ;(spyx-pretty edn-in)
       (with-spy-indent
         (when-not (entity-like? edn-in)
           (throw (ex-info "invalid edn-in" (vals->map edn-in))))
         (let [eid-rec (tag-eid (new-eid))
               ctx     (cond ; #todo add set
                         (map? edn-in) {:entity-type :map :edn-use edn-in}
                         (set? edn-in) {:entity-type :set :edn-use (zipmap edn-in edn-in)}
                         (sequential? edn-in) {:entity-type :array
                                               :edn-use ;  (indexed edn-in)
                                                            (forv [[idx val] (indexed edn-in)]
                                                              [(tag-idx idx) val])}
                         :else (throw (ex-info "unknown value found" (vals->map edn-in))))]
           (t/with-map-vals ctx [entity-type edn-use]
             ;(newline)
             ;(spyx-pretty entity-type )
             ;(spyx-pretty edn-use)
             ; #todo could switch to transients & reduce here in a single swap
             (swap! *tdb* update :eid-type assoc eid-rec entity-type)
             (doseq [[attr-edn val-edn] edn-use]
               ;(spyx [attr-edn val-edn])
               (let [attr-rec attr-edn
                     val-rec  (if (leaf-val? val-edn)
                                val-edn
                                (add-edn-impl val-edn))]
                 (swap! *tdb* update :idx-eav index/add-entry [eid-rec attr-rec val-rec])
                 (swap! *tdb* update :idx-vae index/add-entry [val-rec attr-rec eid-rec])
                 (swap! *tdb* update :idx-ave index/add-entry [attr-rec val-rec eid-rec]))))
           eid-rec)))

     (s/defn add-edn :- s/Int
       "Add the EDN arg to the indexes, returning the EID"
       [edn-in :- s/Any] (<val (add-edn-impl edn-in)))

     ; #todo need to handle sets
     (s/defn ^:no-doc eid->edn-impl :- s/Any
       [eid-rec :- TagVal]
       (let [eav-matches (index/prefix-matches [eid-rec] (grab :idx-eav @*tdb*))
             result-map  (apply glue
                           (forv [[match-eid match-attr match-val] eav-matches]
                             ; (spyx [match-eid match-attr match-val])
                             (assert (= eid-rec match-eid)) ; verify is a prefix match
                             (let [attr-edn match-attr
                                   val-edn  (if (tagged-eid? match-val)
                                              (eid->edn-impl match-val) ; Eid rec
                                              match-val)] ; Leaf rec
                               (t/map-entry attr-edn val-edn))))
             ; >> (spyx-pretty result-map)
             result-out  (let [entity-type (fetch-in @*tdb* [:eid-type eid-rec])]
                           (cond
                             (= entity-type :map) result-map
                             (= entity-type :set) (into #{} (keys result-map))
                             (= entity-type :array) (let [result-keys (mapv <val (keys result-map))
                                                          result-vals (vec (vals result-map))]
                                                      ; if array entity, keys should be in 0..N-1 (already sorted by eav index)
                                                      (assert (= result-keys (range (count result-keys))))
                                                      result-vals)
                             :else (throw (ex-info "invalid entity type found" (vals->map entity-type)))))]
         result-out))

     (s/defn eid->edn :- s/Any
       "Returns the EDN subtree rooted at a eid."
       [eid-in :- s/Int]
       (eid->edn-impl (tag-eid eid-in)))


     (s/defn boolean->binary :- s/Int ; #todo => misc
       "Convert true => 1, false => 0"
       [arg :- s/Bool] (if arg 1 0))

     (s/defn lookup :- [tsk/Triple] ; #todo maybe use :unk or :* for unknown?
       "Given a triple of [e a v] values, use the best index to find a matching subset, where
       'nil' represents unknown values. Returns an index in [e a v] format."
       ([triple :- tsk/Triple]
        (lookup (deref *tdb*) triple))
       ([db :- TDB
         triple :- tsk/Triple]
        (let [known-flgs (mapv #(boolean->binary (t/not-nil? %)) triple)]
          (if (= known-flgs [0 0 0])
            (vec (grab :idx-eav db))
            (let [[e a v] triple
                  found-entries (cond
                                  ; #todo: instead of these blocks => map, apply a function like (mapv ave->eav entries)
                                  (= known-flgs [1 0 0]) (let [entries (index/prefix-matches [e] (grab :idx-eav db))
                                                               result  {:e-vals (mapv xfirst entries)
                                                                        :a-vals (mapv xsecond entries)
                                                                        :v-vals (mapv xthird entries)}]
                                                           result)
                                  (= known-flgs [0 1 0]) (let [entries (index/prefix-matches [a] (grab :idx-ave db))
                                                               result  {:a-vals (mapv xfirst entries)
                                                                        :v-vals (mapv xsecond entries)
                                                                        :e-vals (mapv xthird entries)}]
                                                           result)

                                  (= known-flgs [0 0 1]) (let [entries (index/prefix-matches [v] (grab :idx-vae db))
                                                               result  {:v-vals (mapv xfirst entries)
                                                                        :a-vals (mapv xsecond entries)
                                                                        :e-vals (mapv xthird entries)}]
                                                           result)

                                  (= known-flgs [1 1 0]) (let [entries (index/prefix-matches [e a] (grab :idx-eav db))
                                                               result  {:e-vals (mapv xfirst entries)
                                                                        :a-vals (mapv xsecond entries)
                                                                        :v-vals (mapv xthird entries)}]
                                                           result)

                                  (= known-flgs [0 1 1]) (let [entries (index/prefix-matches [a v] (grab :idx-ave db))
                                                               result  {:a-vals (mapv xfirst entries)
                                                                        :v-vals (mapv xsecond entries)
                                                                        :e-vals (mapv xthird entries)}]
                                                           result)

                                  (= known-flgs [1 0 1]) (let [entries-e  (index/prefix-matches [e] (grab :idx-eav db))
                                                               entries-ev (keep-if #(= v (xlast %)) entries-e)
                                                               result     {:e-vals (mapv xfirst entries-ev)
                                                                           :a-vals (mapv xsecond entries-ev)
                                                                           :v-vals (mapv xthird entries-ev)}]
                                                           result)

                                  (= known-flgs [1 1 1]) (let [entries (index/prefix-matches [e a v] (grab :idx-eav db))
                                                               result  {:e-vals (mapv xfirst entries)
                                                                        :a-vals (mapv xsecond entries)
                                                                        :v-vals (mapv xthird entries)}]
                                                           result)

                                  :else (throw (ex-info "invalid known-flags" (vals->map triple known-flgs))))
                  result-index  (t/with-map-vals found-entries [e-vals a-vals v-vals]
                                  (mapv vector e-vals a-vals v-vals))
                  ]
              result-index)))))

     (s/defn apply-env
       [env :- tsk/Map
        elements :- tsk/Vec]
       (forv [elem elements]
         (if (contains? env elem) ; #todo make sure works with `nil` value
           (get env elem)
           elem)))

     (s/defn ^:no-doc query-triples-impl :- s/Any
       [query-result env qspec-list]
       (t/with-spy-indent
         ;(println :---------------------------------------------------------------------------------------------------)
         ;(spyx-pretty env)
         ;(spyx-pretty qspec-list)
         ;(spyx-pretty @query-result)
         ;(newline)
         (if (empty? qspec-list)
           (swap! query-result t/append env)
           (let [qspec-curr         (xfirst qspec-list)
                 qspec-rest         (xrest qspec-list)
                 qspec-curr-env     (apply-env env qspec-curr)
                 ;>>                 (spyx qspec-curr)
                 ;>>                 (spyx qspec-curr-env)

                 {idxs-param :idxs-true
                  idxs-other :idxs-false} (vec/pred-index tagged-param? qspec-curr-env)
                 qspec-lookup       (vec/set-lax qspec-curr-env idxs-param nil)
                 ;>>                 (spyx idxs-param)
                 ;>>                 (spyx idxs-other)
                 ;>>                 (spyx qspec-lookup)

                 params             (vec/get qspec-curr idxs-param)
                 found-triples      (lookup qspec-lookup)
                 param-frames-found (mapv #(vec/get % idxs-param) found-triples)
                 env-frames-found   (mapv #(zipmap params %) param-frames-found)]
             ;(spyx params)
             ;(spyx-pretty found-triples)
             ;(spyx-pretty param-frames-found)
             ;(spyx-pretty env-frames-found)

             (forv [env-frame env-frames-found]
               (let [env-next (glue env env-frame)]
                 (query-triples-impl query-result env-next qspec-rest)))))))

     (s/defn untag-query-result :- tsk/KeyMap
       [resmap :- tsk/Map]
       ; (newline) (spyx :unwrap-query-result-enter resmap)
       (let [result (apply glue
                      (forv [me resmap]
                        {(<val (key me)) ; mapentry key is always a TagVal
                         (untagged (val me))}))] ; mapentry val might be a primative
         ; (newline) (spyx :unwrap-query-result-leave result)
         result))

     (s/defn query-triples->tagged
       [qspec-list :- [tsk/Triple]]
       (let [query-results (atom [])]
         (query-triples-impl query-results {} qspec-list)
         ; (spyx-pretty :query-triples--results @query-results)
         @query-results))

     (s/defn query-triples :- [tsk/KeyMap]
       [qspec-list :- [tsk/Triple]]
       (let [results-tagged (query-triples->tagged qspec-list)
             results-plain (forv [result-tagged results-tagged]
                             (untag-query-result result-tagged))]
         results-plain))


     (s/defn query-triples+preds
       [qspec-list :- [tsk/Triple]
        pred-list :- [tsk/Fn]]
       (let [query-results-tagged (query-triples->tagged qspec-list)
             ; >>                 (spyx-pretty query-results-tagged)
             keep-result?         (fn fn-keep-result? [query-result]
                                    (let [keep-flags (forv [pred pred-list]
                                                       (pred query-result))
                                          all-keep   (every? t/truthy? keep-flags)]
                                      all-keep))
             query-results-kept   (with-cum-vector
                                    (doseq [query-result query-results-tagged]
                                      ; (spyx-pretty query-result)
                                      (let [query-result-plain (untag-query-result query-result)
                                            keep-result        (keep-result? query-result-plain)]
                                        (when keep-result
                                          (cum-vector-append query-result)))))]
         ; (spyx-pretty query-results-tagged-kept)
         query-results-kept))

     (s/defn ^:no-doc ->SearchParam-fn
       [arg :- (s/cond-pre s/Keyword s/Symbol)]
       (->TagVal :param (t/->kw arg)))

     (defn ^:no-doc ->SearchParam-impl
       [arg] `(->SearchParam-fn (quote ~arg)))

     (defmacro ->SearchParam
       [arg]
       (->SearchParam-impl arg))

     (s/defn ^:no-doc search-triple-fn
       [args :- tsk/Triple]
       ; (assert (= 3 (count args)))
       (with-spy-indent
         (let [[e a v] args
               e-out (if (symbol? e)
                       (tag-param e)
                       (tag-eid e))
               a-out (cond
                       (symbol? a) (tag-param a)
                       (idx-literal? a) (idx-literal->tagged a)
                       :else a)
               v-out (if (symbol? v)
                       (tag-param v)
                       v)]
           [e-out a-out v-out])))

     (defn ^:no-doc search-triple-impl
       [args]
       `(search-triple-fn (quote ~args)))

     (defmacro search-triple
       [& args]
       (search-triple-impl args))

     (defn search-triple-form?
       [form]
       (and (list? form)
         (= (quote search-triple) (xfirst form))))

     (s/defn index-find-leaf :- [TagVal]
       [target :- LeafType]
       ; (println :index-find-leaf )
       (let [results (query-triples+preds
                       [[(tag-param :e) (tag-param :a) target]]
                       []) ; no preds
             ; >> (spyx-pretty results)
             eids    (mapv #(t/fetch % (tag-param :e)) results)]
         eids))

     (def ^:no-doc ^:dynamic *autosyms-seen* nil)

     (s/defn ^:no-doc autosym-resolve :- s/Symbol
       [kk :- s/Any
        vv :- s/Symbol]
       (t/with-nil-default vv
         (when (and (keyword? kk)
                 (= (quote ?) vv))
           (let [kk-sym (t/kw->sym kk)]
             (when (contains? (deref *autosyms-seen*) kk-sym)
               (throw (ex-info "Duplicate autosym found:" (vals->map kk kk-sym))))
             (swap! *autosyms-seen* conj kk-sym)
             kk-sym))))

     (s/defn seq->idx-map :- tsk/Map
       "Converts a sequential like [:a :b :c] into an indexed-map like
       {0 :a
        1 :b
        2 :c} "
       [seq-entity :- tsk/List]
       (into {} (indexed seq-entity)))

     (defn ^:no-doc query-maps->triples-impl
       [qmaps]
       (with-spy-indent
         (newline)
         ;(spyq :query-maps->triples)
         ;(spyx qmaps)
         (doseq [qmap qmaps]
           ; (spyx-pretty qmap)
           (s/validate tsk/Map qmap)
           (let [eid-val       (if (contains? qmap :eid)
                                 (autosym-resolve :eid (grab :eid qmap))
                                 (gensym "tmp-eid-"))
                 map-remaining (dissoc qmap :eid)]
             ; (spyx map-remaining)
             (forv [[kk vv] map-remaining]
               ; (spyx [kk vv])
               (cond
                 (sequential? vv) ; (throw (ex-info "not implemented" {:type :sequential :value vv}))
                 (let [array-val       vv
                       tmp-eid         (gensym "tmp-eid-")
                       triple-modified (search-triple-fn [eid-val kk tmp-eid])
                       ; >>              (spyx triple-modified)
                       qmaps-modified  (forv [elem array-val]
                                         {:eid tmp-eid (gensym "tmp-attr-") elem})]
                   ; (spyx qmaps-modified)
                   (cum-vector-append triple-modified)
                   (query-maps->triples-impl qmaps-modified))

                 (map? vv) (let [tmp-eid         (gensym "tmp-eid-")
                                 triple-modified (search-triple-fn [eid-val kk tmp-eid])
                                 ;>>              (spyx triple-modified)
                                 qmaps-modified  [(glue {:eid tmp-eid} vv)]]
                             ;(spyx qmaps-modified)
                             (cum-vector-append triple-modified)
                             (query-maps->triples-impl qmaps-modified))
                 (leaf-val? vv) (let [triple (search-triple-fn [eid-val kk vv])]
                                  ;(spyx triple)
                                  (cum-vector-append triple))
                 (symbol? vv) (do
                                (let [sym-to-use (autosym-resolve kk vv)
                                      triple     (search-triple-fn [eid-val kk sym-to-use])]
                                  ;(spyx triple)
                                  (cum-vector-append triple)))
                 :else (throw (ex-info "unrecognized value" (vals->map kk vv map-remaining)))
                 ))))))

     (defn ^:no-doc query-maps->triples
       [qmaps]
       (binding [*autosyms-seen* (atom #{})]
         (with-cum-vector
           (query-maps->triples-impl qmaps))))

     (defn ^:no-doc query-results-filter-tmp-attr-mapentry ; #todo make public & optional
       [query-results]
       (let [results-filtered (forv [qres query-results]
                                (drop-if
                                  (fn [k v] (tmp-attr-param? k))
                                  qres))]
         results-filtered))

     (defn ^:no-doc query-results-filter-tmp-eid-mapentry ; #todo make public & optional
       [query-results]
       (let [results-filtered (forv [qres query-results]
                                (drop-if
                                  (fn [k v] (param-tmp-eid? k))
                                  qres))]
         results-filtered))

     (defn ^:no-doc exclude-reserved-identifiers
       " Verify search data does not include reserved identifiers like `tmp-eid-9999` "
       [form]
       (t/walk-with-parents-readonly form
         {:enter (fn [-parents- item]
                   (when (or (tmp-eid-sym? item) (tmp-eid-kw? item))
                     (throw (ex-info "Error: detected reserved tmp-eid-xxxx value" (vals->map item)))))}))

     (s/defn fn-form? :- s/Bool
       [arg :- s/Any]
       (spyx :fn-form?--enter arg)
       (spy :fn-form?--result
         (and (list? arg)
           (let [elem0 (xfirst arg)]
             (and (symbol? elem0)
               (let [eval-result (eval elem0)]
                 (spyxx eval-result)
                 (fn? eval-result)))))))

     (defn ^:no-doc query-maps->tagged
       [query-specs]
       ; (spyx-pretty :query-maps->wrapped-fn-enter query-specs)
       (exclude-reserved-identifiers query-specs)
       (let [maps-in      (keep-if map? query-specs)
             triple-forms (keep-if search-triple-form? query-specs)
             pred-forms   (keep-if #(and (not (search-triple-form? %))
                                      (fn-form? %)) query-specs)
             ;>>           (spyx-pretty maps-in)
             ;>>           (spyx triple-forms)
             ;>>           (spyx pred-forms)
             pred-fns     (forv [pred-form pred-forms]
                            (fn [arg]
                              (newline)
                              (println :pred-fn arg)
                              (newline)
                              (tupelo.core/with-map-vals arg [zip-acc zip-pref]
                                (not= zip-acc zip-pref))))

             triples-proc (forv [triple-form triple-forms]
                            (spyx triple-form)
                            (let [form-to-eval (cons
                                                 (quote tupelo.data/search-triple)
                                                 (rest triple-form))
                                  >>           (spyx form-to-eval)
                                  form-result  (eval form-to-eval)]
                              (spyx form-result)
                              form-result))]

         (let [map-triples    (query-maps->triples maps-in)
               search-triples (glue map-triples triples-proc)]
           ; (spyx-pretty
           (let [unfiltered-results (query-triples+preds
                                      search-triples
                                      pred-fns)
                 ; >>                 (spyx unfiltered-results)
                 filtered-results   (query-results-filter-tmp-attr-mapentry
                                      (query-results-filter-tmp-eid-mapentry
                                        unfiltered-results))]
             ; (spyx-pretty :query-maps->wrapped-fn-leave filtered-results)
             filtered-results))))

     (s/defn untag-query-results :- [tsk/KeyMap]
       [query-result-maps]
       (forv [result-map query-result-maps]
         (apply glue
           (forv [mapentry result-map]
             (let [[me-key me-val] mapentry
                   ; >> (spyxx me-key)
                   ; >> (spyxx me-val)

                   param-raw (<val me-key)
                   val-raw   (untagged me-val) ; me-val might not be a TagVal
                   ;(cond ; #todo kill this if continues to work
                   ;  (tagged-eid? me-val) (<val me-val)
                   ;  :else me-val)
                   ]
               {param-raw val-raw})))))

     (defn query-maps-fn
       [args]
       ; #todo need a linter to catch nonsensical qspecs (attr <> keyword for example)
       (let [unwrapped-query-results (untag-query-results
                                       (query-maps->tagged args))]
         ; (spyx unwrapped-query-results)
         unwrapped-query-results))

     (defn query-maps-impl
       [qspecs]
       `(query-maps-fn (quote ~qspecs)))

     (defmacro query-maps
       "Will evaluate embedded calls to `(search-triple ...)` "
       [qspecs]
       (query-maps-impl qspecs))

     ;----------------------------------------------------------------------------------------------
     ;(s/defn index-find-mapentry :- [EidType]
     ;  [tgt-me :- tsk/MapEntry]
     ;  (let [[tgt-key tgt-val] (mapentry->kv tgt-me)
     ;        tgt-prefix       [tgt-val tgt-key]
     ;        idx-avl-set      (t/validate set? (fetch-in (deref *tdb*) [:idx-map-entry-vk]))
     ;        matching-entries (grab :matches
     ;                           (index/split-key-prefix tgt-prefix idx-avl-set))
     ;        men-hids         (mapv xlast matching-entries)
     ;        ]
     ;    men-hids))

     ;(s/defn index-find-submap
     ;  [target-submap :- tsk/KeyMap]
     ;  (let [map-hids (apply set/intersection
     ;                   (forv [tgt-me target-submap]
     ;                     (set (mapv hid->parent-hid
     ;                            (index-find-mapentry tgt-me)))))]
     ;    map-hids))

     ;(s/defn index-find-mapentry-key :- [EidType]
     ;  [tgt-key :- LeafType]
     ;  (let [tgt-prefix       [tgt-key]
     ;        index            (t/validate set? (fetch-in (deref *tdb*) [:idx-map-entry-kv]))
     ;        matching-entries (grab :matches
     ;                           (index/split-key-prefix tgt-prefix index))
     ;        men-hids         (mapv xlast matching-entries)
     ;        ]
     ;    men-hids))

     ;(s/defn index-find-arrayentry :- [EidType]
     ;  [tgt-ae :- tsk/MapEntry] ; {idx elem} as a MapEntry
     ;  (let [[tgt-idx tgt-elem] (mapentry->kv tgt-ae)
     ;        tgt-prefix       [tgt-elem tgt-idx]
     ;        index            (t/validate set? (fetch-in (deref *tdb*) [:idx-array-entry-ei]))
     ;        matching-entries (grab :matches
     ;                           (index/split-key-prefix tgt-prefix index))
     ;        aen-hids         (mapv xlast matching-entries)
     ;        ]
     ;    aen-hids))

     ;(s/defn index-find-arrayentry-idx :- [EidType]
     ;  [tgt-idx :- LeafType]
     ;  (let [tgt-prefix       [tgt-idx]
     ;        index            (t/validate set? (fetch-in (deref *tdb*) [:idx-array-entry-ie]))
     ;        matching-entries (grab :matches
     ;                           (index/split-key-prefix tgt-prefix index))
     ;        aen-hids         (mapv xlast matching-entries)
     ;        ]
     ;    aen-hids))
     ;
     ;(s/defn parent-path-hid :- [EidType]
     ;  [hid-in :- EidType]
     ;  (let [node-in (hid->node hid-in)]
     ;    (loop [result   (cond
     ;                      (instance? MapEntryNode node-in) [hid-in (me-val-hid node-in)]
     ;                      (instance? ArrayEntryNode node-in) [hid-in (ae-elem-hid node-in)]
     ;                      (instance? LeafNode node-in) [hid-in]
     ;                      :else (throw (ex-info "unrecognized node type" (vals->map hid-in node-in))))
     ;           hid-curr hid-in]
     ;      (let [hid-par (parent-hid (hid->node hid-curr))]
     ;        (if (nil? hid-par)
     ;          result
     ;          (if (or
     ;                (instance? MapEntryNode (hid->node hid-par))
     ;                (instance? ArrayEntryNode (hid->node hid-par)))
     ;            (recur (t/prepend hid-par result) hid-par)
     ;            (recur result hid-par)))))))
     ;
     ;(s/defn parent-path-vals
     ;  [hid-in :- EidType]
     ;  (let [path-hids        (parent-path-hid hid-in)
     ;        parent-selectors (forv [path-hid path-hids]
     ;                           (let [path-node (hid->node path-hid)]
     ;                             (cond
     ;                               (instance? MapEntryNode path-node) (me-key path-node)
     ;                               (instance? ArrayEntryNode path-node) (ae-idx path-node)
     ;                               (instance? LeafNode path-node) (edn path-node)
     ;                               :else (throw (ex-info "invalid parent node" (vals->map path-node))))))]
     ;    parent-selectors))


     ;(def idx-prefix-lookup
     ;  (index/->index [[:e :a :v :idx-eav]
     ;                  [:v :a :e :idx-vae]
     ;                  [:a :v :e :idx-ave]]))

     ))


