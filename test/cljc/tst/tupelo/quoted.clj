;   Copyright (c) Alan Thompson. All rights reserved.
;   The use and distribution terms for this software are covered by the Eclipse Public License 1.0
;   (http://opensource.org/licenses/eclipse-1.0.php) which can be found in the file epl-v10.html at
;   the root of this distribution.  By using this software in any fashion, you are agreeing to be
;   bound by the terms of this license.  You must not remove this notice, or any other, from this
;   software.
(ns tst.tupelo.quoted
  (:use tupelo.core tupelo.test)
  (:require
    [clojure.walk :as walk]
    ))

(comment
  (defn unquoted-form?
    [form]
    (and (list? form)
      (= (quote unquoted) (first form))))

  (defn quoted-impl
    [form]
    (spyx form)
    (let [env-pairs   (atom [])
          form-walked (walk/prewalk (fn [item]
                                      (t/with-nil-default item
                                        (when (unquoted-form? item)
                                          (let ; -spy
                                            [payload (only (rest item))
                                             gsym    (gensym "unq-val-")
                                             pair    [gsym payload]]
                                            (swap! env-pairs t/append pair)
                                            gsym))))
                        form)
          >>          (spyx-pretty env-pairs)
          >>          (spyx-pretty form-walked)
          let-form    (list
                        (quote let)
                        (apply glue @env-pairs)
                        form-walked)]
      let-form))

  (defmacro quoted
    [form] (quoted-impl form))

  (defmacro shower
    [& form]
    `(prn (quote ~form)))

  (dotest
    (is (unquoted-form? (quote (unquoted xxx))))
    (isnt (unquoted-form? (quote (something xxx))))

    (comment ; or do
      (spy-pretty :impl-4
        (quoted-impl (quote
                       (+
                         (unquoted (+ 1 2))
                         (unquoted (+ 3 4))
                         (unquoted (+ 5 6))
                         (unquoted (+ 7 8))))))
      (comment ; =>  (sample result)
        (let [unq-val-32909 (+ 1 2)
              unq-val-32910 (+ 3 4)
              unq-val-32911 (+ 5 6)
              unq-val-32912 (+ 7 8)]
          (+ unq-val-32909 unq-val-32910 unq-val-32911 unq-val-32912))))

    (newline)
    (println :-----------------------------------------------------------------------------)
    (spy-pretty :shower
      (quoted-impl (quote
                     (shower (+
                               (unquoted (+ 1 2))
                               (unquoted (+ 3 4))
                               (unquoted (+ 5 6))
                               (unquoted (+ 7 8))))))))

  )


