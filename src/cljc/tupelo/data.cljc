;   Copyright (c) Alan Thompson. All rights reserved.
;   The use and distribution terms for this software are covered by the Eclipse Public License 1.0
;   (http://opensource.org/licenses/eclipse-1.0.php) which can be found in the file epl-v10.html at
;   the root of this distribution.  By using this software in any fashion, you are agreeing to be
;   bound by the terms of this license.  You must not remove this notice, or any other, from this
;   software.
(ns tupelo.data
  "Effortless data access."
  (:refer-clojure :exclude [load ->VecNode])
  ; #?(:clj (:use tupelo.core)) ; #todo remove for cljs
  #?(:clj (:require
            [tupelo.core :as t :refer [spy spyx spyxx spyx-pretty with-spy-indent
                                       grab glue map-entry indexed only only2 xfirst xsecond xthird xlast xrest not-empty?
                                       it-> cond-it-> forv vals->map fetch-in let-spy
                                       keep-if drop-if append prepend ->sym ->kw kw->sym
                                       ]]
            [tupelo.data.index :as index]
            [tupelo.lexical :as lex]
            [tupelo.misc :as misc]
            [tupelo.schema :as tsk]
            [tupelo.vec :as vec]

            [clojure.walk :as walk]
            [schema.core :as s]
            [clojure.core :as cc]
            [clojure.set :as set]))
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

     (s/defn boolean->binary :- s/Int ; #todo => misc
       "Convert true => 1, false => 0"
       [arg :- s/Bool] (if arg 1 0))


     ;-----------------------------------------------------------------------------
     (defprotocol IVal (<val [this]))
     (defprotocol ITag (<tag [this]))

     (defrecord TagVal
       [tag val]
       ITag (<tag [this] tag)
       IVal (<val [this] val))

     (defrecord Param
       [val]
       IVal (<val [this] val))
     (defrecord Eid
       [val]
       IVal (<val [this] val))
     (defrecord Idx
       [val]
       IVal (<val [this] val))

     (s/defn Eid? :- s/Bool
       [arg :- s/Any] (instance? Eid arg))
     (s/defn Idx? :- s/Bool
       [arg :- s/Any] (instance? Idx arg))

     ; ***** fails strangely under lein-test-refresh after 1st file save if define this using Plumatic schema *****
     (defn tagval? ; :- s/Bool
       [arg] (= TagVal (type arg)))

     (s/defn untagged :- s/Any
       [arg :- s/Any]
       (cond
         (Eid? arg) (<val arg)
         (tagval? arg) (<val arg)
         :else arg))

     (defmethod print-method Eid
       [tv ^java.io.Writer writer]
       (.write writer
         (format "<Eid %s>" (<val tv))))

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
       (walk/postwalk (fn [item]
                        (t/cond-it-> item
                          (instance? Eid it) {:eid (<val it)}
                          (instance? Idx it) {:idx (<val it)}
                          (or (tagval? it)
                            (tagval-map? it)) {(grab :tag it) (grab :val it)}))
         data))

     ;-----------------------------------------------------------------------------

     ; #todo inline all of these?
     (s/defn tag-param :- TagVal
       [arg :- s/Any] (->TagVal :param (t/->kw arg)))

     (s/defn tag-eid-orig :- TagVal
       [eid :- s/Int] (->TagVal :eid (t/validate int? eid)))
     (s/defn tag-eid [arg] (->Eid arg))

     (s/defn tag-idx-orig :- TagVal
       [idx :- s/Int] (->TagVal :idx (t/validate int? idx)))
     (s/defn tag-idx :- TagVal
       [idx :- s/Int] (->TagVal :idx (t/validate int? idx)))

     ; (defn pair-map? [arg] (and (map? arg) (= 1 (count arg))))
     (defn tagged-param? [x] (and (= TagVal (type x)) (= :param (<tag x))))
     (defn tagged-eid? [x]
       (or (Eid? x)
         (and (= TagVal (type x))
           (= :eid (<tag x)))))

     (defn idx-literal? [x] (and (map? x) (= [:idx] (keys x))))
     (defn idx-literal->tagged [x] (tag-idx (only (vals x))))

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

     ;-----------------------------------------------------------------------------
     (defn construct-impl
       [template]
       ;(spyx template)
       ;(spy :impl-out)
       (t/walk-with-parents template
         {:leave (fn [parents item]
                   (t/with-nil-default item
                     (when (= (->sym :?) item)
                       (let [ancestors  (vec (reverse parents))
                             mv-ent     (xfirst ancestors)
                             me         (xsecond ancestors)
                             me-key     (xfirst me)
                             me-key-sym (kw->sym me-key)]
                         me-key-sym))))}) )
     (defmacro construct
       [template] (construct-impl template))

     ;-----------------------------------------------------------------------------
     (do  ; #todo => tupelo.core
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
       ;(s/defn eid-data? :- s/Bool ; #todo keep?  rename -> validate-eid  ???
       ;  "Returns true iff the arg type is a legal EID value"
       ;  [arg] (int? arg))
       )

     (do  ; keep these in sync
       (def AttrTypeInput
         "The Plumatic Schema type name for an attribute"
         (s/cond-pre s/Keyword s/Int))
       (def AttrType
         "The Plumatic Schema type name for an attribute"
         (s/cond-pre s/Keyword TagVal))
       ;(s/defn attr? :- s/Bool ; #todo keep?
       ;  "Returns true iff the arg type is a legal attribute value"
       ;  [arg] (or (keyword? arg) (int? arg)))
       )

     ; #todo update with other primitive types
     (do  ; keep these in sync
       (def Primitive (s/maybe ; maybe nil
                        (s/cond-pre s/Num s/Str s/Keyword s/Bool s/Symbol))) ; instant, uuid, Time ID (TID) (as strings?)
       (s/defn primitive-data? :- s/Bool
         "Returns true iff a value is of primitive data type"
         [arg :- s/Any]
         (or (nil? arg) (number? arg) (string? arg) (keyword? arg) (boolean? arg)))
       (s/defn primitive? :- s/Bool
         "Returns true iff a value is of primitive type (not a collection)"
         [arg :- s/Any]
         (or
           (primitive-data? arg)
           (and (tagval? arg)
             (primitive-data? (<val arg)))
           (symbol? arg))))

     (def TripleIndex #{tsk/Triple})

     ;-----------------------------------------------------------------------------
     ; #todo => tupelo.misc
     (defn normalized-sorted ; #todo need tests & docs. Use for datomic Entity?
       "Walks EDN data, converting all collections to vector, sorted-map-generic, or sorted-set-generic"
       [edn-data]
       (let [unlazy-item (fn [item]
                           (cond
                             (sequential? item) (vec item)
                             (map? item) (t/->sorted-map-generic item)
                             (set? item) (t/->sorted-set-generic item)
                             :else item))
             result      (walk/postwalk unlazy-item edn-data)]
         result))

     (s/defn edn->sha :- s/Str
       "Converts EDN data into a normalized SHA-1 string"
       [edn-data]
       (misc/str->sha
         (pr-str
           (normalized-sorted edn-data))))

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
       {:eid-type {Eid s/Keyword}
        :idx-eav  #{tsk/Triple}
        :idx-ave  #{tsk/Triple}
        :idx-vea  #{tsk/Triple} })

     (s/defn new-tdb :- TDB
       "Returns a new, empty db."
       []
       (into (sorted-map)
         {:eid-type (t/sorted-map-generic) ; source type of entity (:map :array :set)
          :idx-eav  (index/empty-index)
          :idx-ave  (index/empty-index)
          :idx-vea  (index/empty-index) }))

     (s/defn db-pretty :- tsk/KeyMap
       "Returns a pretty version of the DB"
       [db :- TDB]
       (let [db-compact (tagval-walk-compact db) ; returns plain maps & sets instead of sorted or index
             result     (it-> (new-tdb)
                          (update it :eid-type glue (grab :eid-type db-compact))
                          (update it :idx-eav glue (grab :idx-eav db-compact))
                          (update it :idx-ave glue (grab :idx-ave db-compact))
                          (update it :idx-vea glue (grab :idx-vea db-compact)) )]
         result))

     (s/defn edn->type :- s/Keyword
       "Given EDN data, returns a keyword indicating its type "
       [edn :- s/Any]
       (cond
         (primitive-data? edn) :primitive ; #todo specialize to :int :float etc
         (map? edn) :map
         (set? edn) :set
         (sequential? edn) :array
         :else (throw (ex-info "unknown EDN type " (vals->map edn)))))

     (s/defn eid->type
       "Returns the type of an entity"
       [eid :- Eid]
       (fetch-in (deref *tdb*) [:eid-type eid]))

     ; #todo add tsk/Set
     (do  ; keep these in sync
       (def EntityType (s/cond-pre tsk/Map tsk/Set tsk/Vec))
       (s/defn entity-like?
         [arg] (t/contains-key? #{:map :set :array} (edn->type arg))))

     ;-----------------------------------------------------------------------------
     (s/defn ^:no-doc eav->eav :- tsk/Triple
       [triple :- tsk/Triple] triple)

     (s/defn ^:no-doc eav->vea :- tsk/Triple
       [[e a v] :- tsk/Triple] [v e a])
     (s/defn ^:no-doc vea->eav :- tsk/Triple
       [[v e a] :- tsk/Triple] [e a v])

     (s/defn ^:no-doc eav->ave :- tsk/Triple
       [[e a v] :- tsk/Triple] [a v e])
     (s/defn ^:no-doc ave->eav :- tsk/Triple
       [[a v e] :- tsk/Triple] [e a v])

     (s/defn ^:no-doc map-eav->eav :- [tsk/Triple]
       [triples :- [tsk/Triple]] (vec triples))
     (s/defn ^:no-doc map-vea->eav :- [tsk/Triple]
       [triples :- [tsk/Triple]] (mapv vea->eav triples))
     (s/defn ^:no-doc map-ave->eav :- [tsk/Triple]
       [triples :- [tsk/Triple]] (mapv ave->eav triples))

     ;-----------------------------------------------------------------------------
     (def ^:no-doc eid-count-base 1000)
     (def ^:no-doc eid-counter (atom eid-count-base))

     (defn ^:no-doc eid-count-reset
       "Reset the eid-count to its initial value"
       [] (reset! eid-counter eid-count-base))

     (s/defn ^:no-doc new-eid :- EidType ; #todo maybe return Eid record???
       "Returns the next integer EID"
       [] (swap! eid-counter inc))

     (s/defn array->tagidx-map :- {TagVal s/Any}
       [edn-array :- tsk/List ]
       (apply glue {}
         (forv [[idx val] (indexed edn-array)]
           {(tag-idx idx) val})))

     (s/defn tagidx-map->array :- tsk/List
       [idx-map :- {TagVal s/Any}]
       (let [result (forv [idx (range (count idx-map))]
                      (grab (tag-idx idx) idx-map))]
         result))

     (s/defn ^:no-doc db-contains-triple?
       [triple-eav]
       (let [index (grab :idx-eav (deref *tdb*))]
         (contains? index triple-eav)))

     (s/defn ^:no-doc db-remove-triple
       [triple-eav]
       (let [[e a v] triple-eav]
         (swap! *tdb* update :idx-eav index/remove-entry [e a v])
         (swap! *tdb* update :idx-vea index/remove-entry [v e a])
         (swap! *tdb* update :idx-ave index/remove-entry [a v e])))

     (s/defn ^:no-doc db-add-triple
       [triple-eav]
       (let [[e a v] triple-eav]
         (swap! *tdb* update :idx-eav index/add-entry [e a v])
         (swap! *tdb* update :idx-vea index/add-entry [v e a])
         (swap! *tdb* update :idx-ave index/add-entry [a v e])))

     ; #todo need to handle sets
     (s/defn ^:no-doc eid->edn-impl :- s/Any
       [teid :- Eid ]
       (let [eav-matches (index/prefix-match->seq [teid] (grab :idx-eav @*tdb*))
             result-map  (apply glue
                           (forv [[-teid-match- attr val-match] eav-matches]
                             (do
                               (assert (= teid -teid-match-))
                               (let [val-edn (t/cond-it-> val-match
                                               (Eid? it) (eid->edn-impl it))]
                                 (t/map-entry attr val-edn)))))
             ; >>          (spyx-pretty result-map)
             result-out  (let [entity-type (fetch-in @*tdb* [:eid-type teid])]
                           (cond
                             (= entity-type :map) result-map
                             (= entity-type :set) (into #{} (keys result-map))
                             (= entity-type :array) (tagidx-map->array result-map)
                             :else (throw (ex-info "invalid entity type found" (vals->map entity-type)))))]
         result-out))

     (s/defn eid->edn :- s/Any ; #todo reimplement in terms of walk-entity ???
       "Returns the EDN subtree rooted at a eid."
       [eid-in ; :- (s/pred Eid s/Int) ; #todo fix
        ]
       (assert (or (int? eid-in  )
                 (Eid? eid-in)))
       (eid->edn-impl ; #todo fix crutch
         (cond-it-> eid-in
           (int? it) (->Eid it))))

     ;---------------------------------------------------------------------------------------------------
     (def Interceptor ; #todo tupelo.schema (& tupelo.forest)
       "Plumatic Schema type name for interceptor type used by `walk-entity`."
       {(s/required-key :enter) s/Any
        (s/required-key :leave) s/Any
        (s/optional-key :id)    s/Keyword})

     ; #todo need to handle sets
     (s/defn ^:no-doc walk-entity-impl :- s/Any
       [teid :- Eid
        interceptor :- Interceptor]
       (with-spy-indent
         (let [enter-fn    (grab :enter interceptor)
               leave-fn    (grab :leave interceptor)
               eav-matches (index/prefix-match->seq [teid] (grab :idx-eav @*tdb*))]
           (doseq [eav-curr eav-matches]
             (enter-fn eav-curr)
             (let [[-e- -a- v] eav-curr]
               (assert (= teid -e-)) ; #todo temp
               (when (Eid? v)
                 (walk-entity-impl v interceptor)))
             (leave-fn eav-curr)
             nil))))

     (s/defn walk-entity
       "Recursively walks a subtree rooted at an entity, applying the supplied `:enter` and ':leave` functions
        to each node.   Usage:

            (walk-entiry <eid-in> intc-map)

        where `intc-map` is an interceptor map like:

            { :enter <pre-fn>       ; defaults to `identity`
              :leave <post-fn> }    ; defaults to `identity`

        Here, `pre-fn` and `post-fn` look like:

            (fn [triple-eav] ...)

        where `eid-in` specifies the root of the sub-tree being walked. Returns nil."
       [eid-in :- EidType
        intc-map :- tsk/KeyMap]
       (let [legal-keys   #{:id :enter :leave}
             counted-keys #{:enter :leave}
             keys-present (set (keys intc-map))]
         (let [extra-keys (set/difference keys-present legal-keys)]
           (when (not-empty? extra-keys)
             (throw (ex-info "walk-entity: unrecognized keys found:" intc-map))))
         (let [counted-keys-present (set/intersection counted-keys keys-present)]
           (when (empty? counted-keys-present)
             (throw (ex-info "walk-entity: no counted keys found:" intc-map)))))
       (let [enter-fn              (get intc-map :enter t/noop)
             leave-fn              (get intc-map :leave t/noop)
             canonical-interceptor (glue intc-map {:enter enter-fn :leave leave-fn})]
         (walk-entity-impl (->Eid eid-in) canonical-interceptor))
       nil)

;-----------------------------------------------------------------------------
     (s/defn lookup :- [tsk/Triple] ; #todo maybe use :unk or :* for unknown?
       "Given a triple of [e a v] values, use the best index to find a matching subset, where
       'nil' represents unknown values. Returns an index in [e a v] format."
       ([triple :- tsk/Triple]
        (lookup (deref *tdb*) triple))
       ([db :- TDB
         triple :- tsk/Triple]
        (let [[e a v] triple
              known-flgs    (mapv #(boolean->binary (t/not-nil? %)) triple)
              found-entries (cond
                              (= known-flgs [1 0 0]) (map-eav->eav (index/prefix-match->seq [e] (grab :idx-eav db)))
                              (= known-flgs [0 1 0]) (map-ave->eav (index/prefix-match->seq [a] (grab :idx-ave db)))
                              (= known-flgs [0 0 1]) (map-vea->eav (index/prefix-match->seq [v] (grab :idx-vea db)))
                              (= known-flgs [0 1 1]) (map-ave->eav (index/prefix-match->seq [a v] (grab :idx-ave db)))
                              (= known-flgs [1 0 1]) (map-vea->eav (index/prefix-match->seq [v e] (grab :idx-vea db)))
                              (= known-flgs [1 1 0]) (map-eav->eav (index/prefix-match->seq [e a] (grab :idx-eav db)))
                              (= known-flgs [1 1 1]) (map-eav->eav (index/prefix-match->seq [e a v] (grab :idx-eav db)))
                              (= known-flgs [0 0 0]) (map-eav->eav (seq (grab :idx-eav db))) ; everything matches
                              :else (throw (ex-info "invalid known-flags" (vals->map triple known-flgs))))]
          found-entries)))

     ;-----------------------------------------------------------------------------
     (s/defn dissoc-in :- s/Any ; #todo upgrade tupelo.core
       "A sane version of dissoc-in that will not delete intermediate keys.
        When invoked as (dissoc-in the-map [:k1 :k2 :k3... :kZ]), acts like
        (clojure.core/update-in the-map [:k1 :k2 :k3...] dissoc :kZ). That is, only
        the map entry containing the last key :kZ is removed, and all map entries
        higher than kZ in the hierarchy are unaffected."
       [the-map :- tsk/Map
        keys-vec :- [s/Any]] ; #todo  Primitive?
       (let [num-keys     (count keys-vec)
             key-to-clear (last keys-vec)
             parent-keys  (butlast keys-vec)]
         (cond
           (zero? num-keys) the-map
           (= 1 num-keys) (dissoc the-map key-to-clear)
           :else (update-in the-map parent-keys dissoc key-to-clear))))

     ;-----------------------------------------------------------------------------
     (s/defn ^:no-doc array-entity-rerack
       [teid :- Eid] ; #todo make all require db param
       (when (not= :array (eid->type teid))
         (throw (ex-info "non-array found" (vals->map teid))))
       (let [idx-eav      (grab :idx-eav (deref *tdb*))
             triples-orig (index/prefix-match->seq [teid] idx-eav)
             vals         (mapv xthird triples-orig)
             triples-new  (forv [[idx val] (indexed vals)]
                            [teid (tag-idx idx) val])]
         (doseq [triple triples-orig]
           (db-remove-triple triple))
         (doseq [triple triples-new]
           (db-add-triple triple))))

     (declare add-edn-impl)
     (defn ^:no-doc edn->encoded
       [edn]
       (if (primitive-data? edn)
         edn
         (add-edn-impl edn)))

     (s/defn ^:no-doc add-edn-impl :- Eid ; #todo maybe rename:  load-edn->eid  ???
       [edn-in :- s/Any]
       ;(spyq :-----------------------------------------------------------------------------)
       ;(newline)
       ;(spyx-pretty edn-in)
       (with-spy-indent
         (when-not (entity-like? edn-in)
           (throw (ex-info "invalid edn-in" (vals->map edn-in))))
         (let [teid         (->Eid (new-eid))
               entity-type  (edn->type edn-in) ]
           ;(newline)
           ;(spyx-pretty entity-type)
           ;(spyx-pretty edn-in)
           ; #todo could switch to transients & reduce here in a single swap
           (swap! *tdb* assoc-in [:eid-type teid] entity-type)
           (cond
             (= :map entity-type) (doseq [[k-in v-in] edn-in]
                                    (when-not (primitive? k-in) ; #todo generalize?
                                      (throw (ex-info "Attribute must be primitive (non-collection) type" (vals->map k-in v-in edn-in))))
                                    (db-add-triple [teid (edn->encoded k-in) (edn->encoded v-in)]))

             (= :set entity-type) (doseq [k-in edn-in]
                                    (when-not (primitive? k-in) ; #todo generalize?
                                      (throw (ex-info "Attribute must be primitive (non-collection) type" (vals->map k-in edn-in))))
                                    (let [k (edn->encoded k-in)]
                                      (db-add-triple [teid k k])))

             (= :array entity-type) (doseq [[idx val] (indexed edn-in)]
                                      (db-add-triple [teid (tag-idx idx) (edn->encoded val)]))

             :else (throw (ex-info "unknown value found" (vals->map edn-in))))
           teid)))

     (s/defn add-edn :- s/Int
       "Add the EDN arg to the indexes, returning the EID"
       [edn-in :- s/Any] (<val (add-edn-impl edn-in)))

     (s/defn entity-add-map-entry
       "Adds a new attr-val pair to an existing map entity."
       [eid  :- s/Int
        attr-in :- Primitive
        edn-in :- s/Any] ; #todo add db arg version
       (when-not (primitive? attr-in) ; #todo generalize?
         (throw (ex-info "Attribute must be primitive (non-collection) type" (vals->map attr-in edn-in))))
       (with-spy-indent
         (let [teid        (->Eid eid)
               idx-eav     (grab :idx-eav (deref *tdb*))
               entity-type (fetch-in @*tdb* [:eid-type teid])
               >>          (when (nil? entity-type)
                             (throw (ex-info "entity not found" (vals->map teid))))
               ea-triples  (index/prefix-match->seq [teid attr-in] idx-eav)
               >>          (when (not-empty? ea-triples)
                             (throw (ex-info "pre-existing element found" (vals->map teid attr-in ea-triples))))]
           ;(newline)
           ;(spyx-pretty entity-type)
           ;(spyx-pretty edn-in)
           (when-not (= :map entity-type)
             (throw (ex-info "non map type found" (vals->map teid entity-type))))
           (db-add-triple [teid (edn->encoded attr-in) (edn->encoded edn-in)])
           teid)))

     (s/defn entity-add-set-elem
       "Adds a new element to an existing set entity."
       [eid :- s/Int
        edn-in :- s/Any] ; #todo add db arg version
       (when-not (primitive? edn-in) ; #todo generalize?
         (throw (ex-info "Attribute must be primitive (non-collection) type" (vals->map edn-in))))
       (with-spy-indent
         (let [teid        (->Eid eid)
               idx-eav     (grab :idx-eav (deref *tdb*))
               entity-type (eid->type teid)
               ea-triples  (index/prefix-match->seq [teid edn-in] idx-eav)
               >>          (when (not-empty? ea-triples)
                             (throw (ex-info "pre-existing element found" (vals->map teid edn-in ea-triples))))
               edn-encoded (edn->encoded edn-in) ]
           ;(newline)
           ;(spyx-pretty entity-type)
           ;(spyx-pretty edn-in)
           (when-not (= :set entity-type)
             (throw (ex-info "non set type found" (vals->map teid entity-type))))
           (db-add-triple [teid  edn-encoded edn-encoded])
           teid)))


     (s/defn entity-add-array-elem
       "Adds a new idx-val pair to an existing map entity. "
       [eid  :- s/Int
        idx-in :- s/Int
        edn-in :- s/Any] ; #todo add db arg version
       (when-not (int? idx-in)
         (throw (ex-info "Index must be integer type" (vals->map eid idx-in ))))
       (with-spy-indent
         (let [teid        (->Eid eid)
               tidx        (tag-idx idx-in)
               idx-eav     (grab :idx-eav (deref *tdb*))
               entity-type (eid->type teid)
               ea-triples  (index/prefix-match->seq [teid tidx] idx-eav)
               >>          (when (not-empty? ea-triples)
                             (throw (ex-info "pre-existing element found" (vals->map teid idx-in ea-triples))))]
           ;(newline)
           ;(spyx-pretty entity-type)
           ;(spyx-pretty edn-in)
           (when-not (= :array entity-type)
             (throw (ex-info "non array type found" (vals->map teid entity-type))))
           (db-add-triple [teid tidx (edn->encoded edn-in)])
           (array-entity-rerack teid)
           teid)))

     ;-----------------------------------------------------------------------------
     ; #todo (defn remove-entity [eid] ...)
     ; #todo (defn update-entity [eid fn] ...)
     ; #todo (defn update-triple [triple-eav fn] ...)
     (declare remove-entity)

     (s/defn remove-triple
       "Recursively removes an EAV triple of data from the db."
       [triple-eav :- tsk/Triple] ; #todo add db arg version
       (let [[-e- -a- v] triple-eav]
         (when-not (db-contains-triple? triple-eav)
           (throw (ex-info "triple not found" (vals->map triple-eav))))
         (when (Eid? v)
           (remove-entity (<val v)))
         (db-remove-triple triple-eav)))

     (s/defn ^:no-doc remove-entity
       [eid-in :- EidType] ; #todo add db arg version
       (let [idx-eav     (grab :idx-eav (deref *tdb*))
             teid        (->Eid eid-in)
             eav-triples (index/prefix-match->seq [teid] idx-eav)]
         (when (empty? eav-triples)
           (throw (ex-info "entity not found" (vals->map eid-in))))
         (swap! *tdb* dissoc-in [:eid-type teid])
         (doseq [triple-eav eav-triples]
           (remove-triple triple-eav))))

     (s/defn remove-entity-elem
       "Recursively removes an element EAV triple of data from the db."
       [eid-in  :- EidType
        attr :- AttrTypeInput] ; #todo add db arg version
       (let [teid        (->Eid eid-in)
             tattr       (if (int? attr)
                           (tag-idx attr)
                           attr)
             idx-eav     (grab :idx-eav (deref *tdb*))
             eav-triples (index/prefix-match->seq [teid tattr] idx-eav)
             >>          (when (< 1 (count eav-triples))
                           (throw (ex-info "multiple elements found" (vals->map teid tattr eav-triples))))
             triple-eav  (only eav-triples)
             [-e- -a- v] triple-eav]
         (when (tagged-eid? v)
           (remove-entity (<val v)))
         (db-remove-triple triple-eav)
         (when (= :array (eid->type teid))
           (array-entity-rerack teid))))

     (s/defn remove-root-entity
       "Recursively removes an entity from the db."
       [eid-in :- EidType] ; #todo add db arg version
       (let [idx-vea     (grab :idx-vea (deref *tdb*))
             teid        (->Eid eid-in)
             vea-triples (index/prefix-match->seq [teid] idx-vea)]
         (when (not-empty? vea-triples)
           (throw (ex-info "entity not root" (vals->map eid-in))))
         (remove-entity eid-in)))

     ; #todo remove-array-elem
     ; #todo remove-map-entry
     ; #todo remove-set-elem

     ; #todo update-array-elem
     ; #todo update-map-entry
     ; #todo update-set-elem

     ;-----------------------------------------------------------------------------
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
             results-plain  (forv [result-tagged results-tagged]
                              (untag-query-result result-tagged))]
         results-plain))

     (s/defn eval-with-env-map :- s/Any
       [env-map :- tsk/KeyMap
        & forms] ; from tagged param => (possibly-tagged) value
       ;(spyx-pretty env-map)
       ;(spyx-pretty forms)
       (let [sym-val-pairs (apply glue
                             (forv [[kk vv] env-map]
                               [(kw->sym kk) vv]))]
         ; (spyx-pretty sym-val-pairs)
         (eval
           `(let ~sym-val-pairs
              ~@forms))))

     (s/defn tagged-params->env-map :- tsk/KeyMap
       [tagged-map :- {TagVal s/Any}] ; from tagged param => (possibly-tagged) value
       ;(spyx-pretty tagged-map)
       (let [env-map (apply glue
                       (forv [[kk vv] tagged-map]
                         {(untagged kk) (untagged vv)}))]
         env-map))

     (defn eval-with-tagged-params
       [tagged-map forms] ; from tagged param => (possibly-tagged) value
       (let [env-map (tagged-params->env-map tagged-map)]
         (apply eval-with-env-map env-map forms))) ; #todo unify rest params on forms!!!

     (s/defn query-triples+preds
       [qspec-list :- [tsk/Triple]
        pred-master :- tsk/List]
       ; (spyx :query-triples+preds pred-master)
       (let [query-results-tagged (query-triples->tagged qspec-list)
             ; >>                   (spyx-pretty query-results-tagged)
             query-results-kept   (keep-if (fn [query-result]
                                             ; (spyx-pretty query-result)
                                             (let [keep-result (eval-with-tagged-params query-result [pred-master])]
                                               ; (spyx keep-result)
                                               keep-result))
                                    query-results-tagged)]
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
                       (->Eid e))
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

     (s/defn index-find-leaf :- [Eid]
       [target :- Primitive]
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
           (let [kk-sym (kw->sym kk)]
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

     (defn ^:no-doc query->triples-impl
       [qmaps]
       (with-spy-indent
         ;(newline)
         ;(spyq :-----------------------------------------------------------------------------)
         ;(spyq :query->triples)
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
               ;(spyx [kk vv])
                (cond
                  (symbol? vv) ; #todo clean up conflict with (primitive-data? ...) below
                  (do
                    (let [sym-to-use (autosym-resolve kk vv)
                          triple     (search-triple-fn [eid-val kk sym-to-use])]
                      ;(spyx :symbol triple)
                      (cum-vector-append triple)))

                  (sequential? vv) ; (throw (ex-info "not implemented" {:type :sequential :value vv}))
                  (let [array-val       vv
                        tmp-eid         (gensym "tmp-eid-")
                        triple-modified (search-triple-fn [eid-val kk tmp-eid])
                        ; >>              (spyx triple-modified)
                        qmaps-modified  (forv [elem array-val]
                                          {:eid tmp-eid (gensym "tmp-attr-") elem})]
                    ; (spyx qmaps-modified)
                    (cum-vector-append triple-modified)
                    (query->triples-impl qmaps-modified))

                  (map? vv)
                  (let [tmp-eid         (gensym "tmp-eid-")
                        triple-modified (search-triple-fn [eid-val kk tmp-eid])
                        ; >>              (spyx triple-modified)
                        qmaps-modified  [(glue {:eid tmp-eid} vv)]]
                    ;(spyx qmaps-modified)
                    (cum-vector-append triple-modified)
                    (query->triples-impl qmaps-modified))

                  (primitive-data? vv)
                  (let [triple (search-triple-fn [eid-val kk vv])]
                    ; (spyx triple)
                    (cum-vector-append triple))

                  :else (throw (ex-info "unrecognized value" (vals->map kk vv map-remaining)))
                  ))))))

     (defn ^:no-doc query->triples
       [qmaps]
       (binding [*autosyms-seen* (atom #{})]
         (with-cum-vector
           (query->triples-impl qmaps))))

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
       ; (spyx :fn-form?--enter arg)
       ; (spy :fn-form?--result)
       (and (list? arg)
         (let [elem0 (xfirst arg)]
           (and (symbol? elem0)
             (let [eval-result (eval elem0)]
               ; (spyxx eval-result)
               (fn? eval-result))))))

     (defn ^:no-doc query->tagged
       [query-specs]
       ; (spyx-pretty :query->wrapped-fn-enter query-specs)
       (exclude-reserved-identifiers query-specs)
       (let [maps-in      (keep-if map? query-specs)
             triple-forms (keep-if search-triple-form? query-specs)
             pred-forms   (keep-if #(and (not (search-triple-form? %))
                                      (fn-form? %)) query-specs)
             ;>>           (spyx-pretty maps-in)
             ;>>           (spyx triple-forms)
             ;>>           (spyx pred-forms)
             pred-master   `(clojure.core/every? tupelo.core/truthy?
                                    [true ; sentinal in case of empty list
                                     ~@pred-forms])
             ;>> (spyx pred-master)

             triples-proc (forv [triple-form triple-forms]
                            ; (spyx triple-form)
                            (let [form-to-eval (cons
                                                 (quote tupelo.data/search-triple)
                                                 (rest triple-form))
                                  ;>>           (spyx form-to-eval)
                                  form-result  (eval form-to-eval)]
                              ;(spyx form-result)
                              form-result))]

         (let [map-triples    (query->triples maps-in)
               search-triples (glue map-triples triples-proc)]
           ;(spyx-pretty map-triples)
           (let [unfiltered-results (query-triples+preds
                                      search-triples
                                      pred-master)
                 ;>>                 (spyx unfiltered-results)
                 filtered-results   (query-results-filter-tmp-attr-mapentry
                                      (query-results-filter-tmp-eid-mapentry
                                        unfiltered-results))]
             ;(spyx-pretty :query->wrapped-fn-leave filtered-results)
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
                   ]
               {param-raw val-raw})))))

     (defn query-fn
       [args]
       ; #todo need a linter to catch nonsensical qspecs (attr <> keyword for example)
       (let [unwrapped-query-results (untag-query-results
                                       (query->tagged args))]
         ; (spyx unwrapped-query-results)
         unwrapped-query-results))

     (defn query-impl
       [qspecs]
       `(query-fn (quote ~qspecs)))

     (defmacro query
       "Will evaluate embedded calls to `(search-triple ...)` "
       [qspecs]
       (query-impl qspecs))





     ))







