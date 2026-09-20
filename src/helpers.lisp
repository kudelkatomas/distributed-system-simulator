;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;
;;;; File: helpers.lisp
;;;; Author: Tomáš Kudělka
;;;;
;;;; Description:
;;;;   Helper functions
;;;;

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; Initialization
;;;

(defun init-n-nodes (n node-class program &key (network (make-instance 'network)) (first-index 1))
  (loop for i from first-index to (1- (+ first-index n))
        collect (make-instance node-class
                               :id i
                               :program program
                               :network network)))

(defun start-n-nodes (n node-class program &key (network (make-instance 'network)) (first-index 1))
  "Returns list of nodes."
  (let ((nodes (init-n-nodes n node-class program :network network :first-index first-index)))
    (dolist (node nodes)
      (start node))
    (format t "Nodes ~a-~a started.~%" first-index (1- (+ first-index n)))
    nodes))

(defun start-2n-nodes-in-two-groups (n node-class program &key (sleep-duration 0))
  "Returns a pair of lists of nodes (group1 . group2)."
  (let* ((network (make-instance 'network))
         (group1 (start-n-nodes n node-class program :network network)))
    (when (> sleep-duration 0)
      (format t "Waiting for ~a seconds...~%" sleep-duration)
      (sleep sleep-duration))
    (cons group1 (start-n-nodes n node-class program :network network :first-index (1+ n)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; General helper functions
;;;

(defun compose (f g)
  (lambda (x)
    (funcall f (funcall g x))))

(defun wait-for-all (nodes)
  "Waits until all nodes finish computing."
  (format t "Waiting for nodes to finish computing...~%")
  (mapcar (compose #'bt:join-thread #'thread) nodes)
  (format t "All nodes finished computing.~%"))