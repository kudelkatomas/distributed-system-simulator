;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;
;;;; File: demo.lisp
;;;; Author: Tomáš Kudělka
;;;;
;;;; Description:
;;;;   Couple of examples
;;;;

;;;
;;; Example 1
;;;
;;; Nodes introduce themselves.
;;; Nodes will most likely know each other at the end.
;;;

(defun run-example-1 (&optional (node-count 3))
  (let* ((program (node-program (this-node)
                    (send-message-as this-node
                                     *broadcast*
                                     ":connection-request")
                    (loop repeat node-count do
                            (sleep 0.01)
                            (process-next-message this-node))))
         (nodes (start-n-nodes node-count 'node program)))
    (wait-for-all nodes)
    (format t "Will return a list of node-known-ids.~%")
    (mapcar #'known-node-ids nodes)))

;;;
;;; Example 2
;;;
;;; Nodes introduce themselves, but are started in two groups.
;;; Nodes will most likely NOT know each other at the end.
;;;

(defun run-example-2 (&optional (group-node-count 2) (pause-between-starts-duration 0.5))
  (let* ((program (node-program (this-node)
                    (send-message-as this-node
                                     *broadcast*
                                     ":connection-request")
                    (loop repeat (* 2 group-node-count) do
                            (sleep 0.01)
                            (process-next-message this-node))))
         (groups (start-2n-nodes-in-two-groups group-node-count
                                               'node
                                               program
                                               :sleep-duration pause-between-starts-duration))
         (nodes (append (car groups) (cdr groups))))
    (wait-for-all nodes)
    (format t "Will return a list of node-known-ids.~%")
    (mapcar #'known-node-ids nodes)))

;;;
;;; Example 3
;;;
;;; Mutual exclusion.
;;; The resource is the ability to print to the output.
;;;
;;; In this example we give the nodes access to node-count to ensure they know each other
;;; before using the resource.
;;; See Assumptions in mutex-lamport.lisp.
;;;

;; No synchronization
(defun run-example-3-no-sync (&optional (node-count 10) (prints-per-node 5))
  (let* ((program (node-program (this-node)
                    ; Introduction    
                    (until (= (length (known-node-ids this-node))
                              (1- node-count))
                      (send-message-as this-node
                                       *broadcast*
                                       ":connection-request")
                      (sleep 0.01)
                      (process-next-message this-node))
                    ; Using the resource
                    (loop repeat prints-per-node do
                            (format t "[id: ~a; time:~a]~%" (id this-node) (clock this-node)))))
         (nodes (start-n-nodes node-count 'node program)))
    (wait-for-all nodes)
    (format t "Check the output.~%")))

;; Synchronized (lamport-node)
(defun run-example-3-sync (&optional (node-count 10) (prints-per-node 5))
  (let* ((program (node-program (this-node)
                    ; Introduction
                    (until (= (length (known-node-ids this-node))
                              (1- node-count))
                      (send-message-as this-node
                                       *broadcast*
                                       ":connection-request")
                      (sleep 0.01)
                      (process-next-message this-node))
                    ; Using the resource
                    (loop repeat prints-per-node do
                            (request-resource this-node)
                            (until (holds-resource-p this-node)
                              (process-next-message this-node))
                            (format t "[id: ~a; time:~a]~%" (id this-node) (clock this-node))
                            (release-resource this-node))
                    ; Sending acknowledgements to others
                    ;
                    ; Deadlock occurs if any of the nodes shuts down too soon.
                    ; Barrier should be used here to check that none of the nodes
                    ; wants the resource anymore. Loop is used for simplicity,
                    ; but does not prevent the deadlock in all cases.
                    (loop repeat (* 2 node-count prints-per-node) do
                            (sleep 0.01)
                            (process-next-message this-node))))
         (nodes (start-n-nodes node-count 'lamport-node program)))
    (wait-for-all nodes)
    (format t "Check the output.~%")))