;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;
;;;; File: helpers.lisp
;;;; Author: Tomáš Kudělka
;;;;
;;;; Description:
;;;;   Helper functions and macros
;;;;

(defun range (from to)
  "Returns a list of numbers (from from+1 ... to)."
  (if (> from to)
      nil
    (cons from (range (1+ from) to))))

(defun first-half (n)
  (range 1 (floor (/ n 2))))

(defun second-half (n)
  (range (1+ (floor (/ n 2))) n))

(defun replace-element (list el new-el &key (key (lambda (x) x)))
  (cond ((null list) '())
        ((eql el (funcall key (car list)))
         (cons new-el (replace-element (cdr list) el new-el :key key)))
        (t (cons (car list)
                 (replace-element (cdr list) el new-el :key key)))))

;; Source: https://lispcookbook.github.io/cl-cookbook/process.html
(defmacro until (condition &body body)
  "Loops around until the condition becomes true."
  (let ((block-name (gensym)))
    `(block ,block-name
       (loop
          (if ,condition
              (return-from ,block-name nil)
            (progn ,@body))))))