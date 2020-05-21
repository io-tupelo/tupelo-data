;   Copyright (c) Alan Thompson. All rights reserved.
;   The use and distribution terms for this software are covered by the Eclipse Public License 1.0
;   (http://opensource.org/licenses/eclipse-1.0.php) which can be found in the file epl-v10.html at
;   the root of this distribution.  By using this software in any fashion, you are agreeing to be
;   bound by the terms of this license.  You must not remove this notice, or any other, from this
;   software.
(ns tst.tupelo.data.index
  ;---------------------------------------------------------------------------------------------------
  ;   https://code.thheller.com/blog/shadow-cljs/2019/10/12/clojurescript-macros.html
  ;   http://blog.fikesfarm.com/posts/2015-12-18-clojurescript-macro-tower-and-loop.html
  #?(:cljs (:require-macros
             [tupelo.core]
             [tupelo.testy]
             ))
  (:require
    [clojure.test] ; sometimes this is required - not sure why
    [clojure.data.avl :as avl]
    [clojure.string :as str]
    [tupelo.data.index :as index]
    [tupelo.lexical :as lex]
    [tupelo.testy :refer [deftest testing is dotest isnt is= isnt= is-set= is-nonblank=
                          throws? throws-not? define-fixture]]
    [tupelo.core :as t :refer [spy spyx spyxx spy-pretty spyx-pretty spyq unlazy let-spy with-spy-indent
                               only only2 forv glue grab nl keep-if drop-if ->sym xfirst xsecond xthird not-nil?
                               it-> fetch-in with-map-vals map-plain?]]
    [tupelo.tag :as tt :refer [IVal ITag ITagMap ->tagmap <tag <val]] ))

; #todo fix dotest-focus so it works again!

#?(:cljs (enable-console-print!))

(dotest
  (let [index   (t/it-> (index/empty-index)
                  (index/add-entry it [2 :b])
                  (index/add-entry it [2 :c])
                  (index/add-entry it [2 :a])
                  (index/add-entry it [1]))
        index-2 (t/it-> index
                  (index/remove-entry it [2 :c])
                  (index/remove-entry it [1]))]
    (is= (vec index) [[1] [2 :a] [2 :b] [2 :c]])
    (is= (vec index-2) [[2 :a] [2 :b]])
    (throws? (index/remove-entry index-2 [2 :c]))))


          (dotest
  (is= (vec (avl/sorted-set-by lex/compare-lex [1 :a] [1] [2]))
    [[1] [1 :a] [2]])
  (is= (vec (avl/sorted-set-by lex/compare-lex [1 :a] [1 nil] [1] [2]))
    [[1] [1 nil] [1 :a] [2]])
  (let [expected-vec [[1]
                      [1 nil]
                      [1 :a]
                      [1 :b]
                      [1 :b nil]
                      [1 :b nil 9]
                      [1 :b 3]
                      [2]
                      [2 0]
                      [3]
                      [3 :y]]
        expected-set (index/->index expected-vec)
        data         (reverse expected-vec)
        result-set   (apply avl/sorted-set-by lex/compare-lex data)
        result-vec   (vec result-set)]
    (is= result-vec expected-vec)
    (is= result-set expected-set) )
  (let [expected   [[1]
                    [1 nil]
                    [1 nil nil]
                    [1 nil 9]
                    [1 2]
                    [1 2 nil]
                    [1 2 3]]
        data       (reverse expected)
        result-vec (vec (index/->index data))]
    (is= result-vec expected)))

(dotest
  (let [lex-set (avl/sorted-set 1 2 3)
        lex-map (avl/sorted-map :a 1 :b 2 :c 3)]
    (is= #{1 2 3} lex-set)
    (is= {:a 1 :b 2 :c 3} lex-map))

  (let [data-raw (index/->index #{[:b 1] [:b 2] [:b 3]
                                         [:f 1] [:f 2] [:f 3]
                                         [:h 1] [:h 2]})]
    ; test with prefix-key
    (is= (index/split-key-prefix (index/bound-lower [:a 2]) data-raw)
      {:smaller #{},
       :matches #{},
       :larger  #{[:b 1] [:b 2] [:b 3] [:f 1] [:f 2] [:f 3] [:h 1] [:h 2]}})
    (is= (index/split-key-prefix (index/bound-lower [:b 2]) data-raw)
      {:smaller #{}
       :matches #{[:b 1] [:b 2] [:b 3]},
       :larger  #{[:f 1] [:f 2] [:f 3] [:h 1] [:h 2]}})
    (is= (index/split-key-prefix (index/bound-lower [:c 2]) data-raw)
      {:smaller #{[:b 1] [:b 2] [:b 3]},
       :matches #{}
       :larger  #{[:f 1] [:f 2] [:f 3] [:h 1] [:h 2]}})
    (is= (index/split-key-prefix (index/bound-lower [:f 2]) data-raw)
      {:smaller #{[:b 1] [:b 2] [:b 3]},
       :matches #{[:f 1] [:f 2] [:f 3]},
       :larger  #{[:h 1] [:h 2]}})
    (is= (index/split-key-prefix (index/bound-lower [:g 2]) data-raw)
      {:smaller #{[:b 1] [:b 2] [:b 3] [:f 1] [:f 2] [:f 3]},
       :matches #{},
       :larger  #{[:h 1] [:h 2]}})
    (is= (index/split-key-prefix (index/bound-lower [:h 2]) data-raw)
      {:smaller #{[:b 1] [:b 2] [:b 3] [:f 1] [:f 2] [:f 3]},
       :matches #{[:h 1] [:h 2]}
       :larger  #{}})
    (is= (index/split-key-prefix (index/bound-lower [:joker 2]) data-raw)
      {:smaller #{[:b 1] [:b 2] [:b 3] [:f 1] [:f 2] [:f 3] [:h 1] [:h 2]},
       :matches #{}
       :larger  #{}}))

  ; test with full-key
  (let [data-raw (index/->index #{[:b 1] [:b 2] [:b 3]
                                         [:f 1] [:f 2] [:f 3]
                                         [:h 1] [:h 2]})]
    (is= (index/split-key-prefix [:a 2] data-raw)
      {:smaller #{},
       :matches #{},
       :larger  #{[:b 1] [:b 2] [:b 3] [:f 1] [:f 2] [:f 3] [:h 1] [:h 2]}})
    (is= (index/split-key-prefix [:b 2] data-raw)
      {:smaller #{[:b 1]}
       :matches #{ [:b 2] },
       :larger  #{[:b 3] [:f 1] [:f 2] [:f 3] [:h 1] [:h 2]}})
    (is= (index/split-key-prefix [:c 2] data-raw)
      {:smaller #{[:b 1] [:b 2] [:b 3]},
       :matches #{}
       :larger  #{[:f 1] [:f 2] [:f 3] [:h 1] [:h 2]}})
    (is= (index/split-key-prefix [:f 2] data-raw)
      {:smaller #{[:b 1] [:b 2] [:b 3] [:f 1]},
       :matches #{[:f 2]},
       :larger  #{[:f 3] [:h 1] [:h 2]}})
    (is= (index/split-key-prefix [:g 2] data-raw)
      {:smaller #{[:b 1] [:b 2] [:b 3] [:f 1] [:f 2] [:f 3]},
       :matches #{},
       :larger  #{[:h 1] [:h 2]}})
    (is= (index/split-key-prefix [:h 2] data-raw)
      {:smaller #{[:b 1] [:b 2] [:b 3] [:f 1] [:f 2] [:f 3] [:h 1]},
       :matches #{ [:h 2]}
       :larger  #{}})
    (is= (index/split-key-prefix [:joker 2] data-raw)
      {:smaller #{[:b 1] [:b 2] [:b 3] [:f 1] [:f 2] [:f 3] [:h 1] [:h 2]},
       :matches #{}
       :larger  #{}})) )



