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

#|
(let* ((node-count 3)
       (program (node-program (this-node)
                  (send-message-as this-node
                                   *broadcast*
                                   ":connection-request")
                  (loop repeat node-count do
                          (sleep 0.01)
                          (process-next-message this-node))))
       (nodes (start-n-nodes node-count 'node program)))
  (wait-for-all nodes)
  (mapcar #'known-node-ids nodes))
|#

;;;
;;; Example 2
;;;
;;; Nodes introduce themselves, but are started in two groups.
;;; Nodes will most likely NOT know each other at the end.
;;;

#|
(let* ((group-node-count 2)
       (pause-between-starts-duration 0.5)
       (program (node-program (this-node)
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
  (mapcar #'known-node-ids nodes))
|#

;;;
;;; Example 3
;;;
;;; TO BE COMPLETED
;;;
;;; Mutual exclusion.
;;; The resource is the ability to print to the output.
;;;

#| No synchronization |#

;; Vzajemne vylouceni

; Bez synchronizace
(setf *program-not-synced*
      (node-program
        (send-message-as this-node *broadcast* ":connection-request")
        (loop repeat 100 do
              (sleep 0.01)
              (handle-message this-node))

        (format t "[id: ~a; time:~a]~%" (id this-node) (clock this-node))))

; Se synchronizací
(setf *program-synced*
      (node-program
        (send-message-as this-node *broadcast* ":connection-request")
        (loop repeat 100 do
                (sleep 0.01)
                (handle-message this-node))

        (loop repeat 5 do
                (request-resource this-node)
                (until (holds-resource-p this-node)
                       (handle-message this-node))
                (format t "[id: ~a; request-time:~a]~%"
                        (id this-node) (request-timestamp this-node))
                (release-resource this-node))))
     

;;
;; Spuštění naráz
;;

(defun run-program (program)
  (progn
    (setf *network* (make-instance 'network)
          *nodes*   (mapcar (lambda (i)
                              (make-instance 'node
                                             :id i
                                             :program program
                                             :network *network*))
                            (range 1 *node-count*))
          *threads* (mapcar #'start *nodes*))
    (pprint *threads*)))




;;
;; Spuštění po dvou skupinach
;;

(defun run-2-programs (program-1 program-2)
  (progn
    (setf *network*                (make-instance 'network)
          *nodes*                  (mapcar (lambda (i)
                                             (make-instance 'node
                                                            :id i
                                                            :program program-1
                                                            :network *network*))
                                           (first-half *node-count*))
          *threads*                (mapcar #'start *nodes*))
    (format t "First group started...~%")
    (sleep 0.5)
    (setf *nodes-2*                (mapcar (lambda (i)
                                             (make-instance 'node
                                                            :id i
                                                            :program program-2
                                                            :network *network*))
                                           (second-half *node-count*))
          (cdr (last *threads*))   (mapcar #'start *nodes-2*)
          (cdr (last *nodes*))     *nodes-2*)
    (format t "Both groups started.~%")))

;;
;; Pomocná funkce pro získání hodnot hodin uzlů (nepouziva lock)
;;

(defun clocks ()
  (mapcar #'clock *nodes*))