;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;
;;;; File: load.lisp
;;;; Author: Tomáš Kudělka
;;;;
;;;; Description:
;;;;   Loads:
;;;;     Bordeaux-Threads: https://github.com/sionescu/bordeaux-threads
;;;;     ./distributed-system-sim.lisp
;;;;     ./mutex-lamport.lisp
;;;;

(require "asdf")
(asdf:load-system :bordeaux-threads)

(defsystem distributed-system-sim ()
  :members ("distributed-system-sim" "mutex-lamport")
  :rules ((:in-order-to :compile :all (:requires (:load :previous)))))

(compile-system 'distributed-system-sim :load t)