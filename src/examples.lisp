;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;
;;;; File: helpers.lisp
;;;; Author: Tomáš Kudělka
;;;;
;;;; Description:
;;;;   Helper functions and macros
;;;;


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Ukázka
;;
;; Funkce "run-program" a "run-2-programs" prepisují "network", "nodes" a "threads".
;; Funkce "run-2-programs" přepisuje také "nodes-2".
;;

(defparameter *node-count* 20)

(defparameter *program* nil)
(defparameter *program-not-synced* nil)
(defparameter *program-synced* nil)

(defparameter *program-1* nil)
(defparameter *program-2* nil)

(defparameter *network* nil)

(defparameter *nodes* nil)
(defparameter *nodes-2* nil)

(defparameter *threads* nil)

;;
;; Programy
;;

#|
;; Jedna skupina
(setf *program*
      (node-program
        (send-message-as this-node *broadcast* ":connection-request")
        (loop repeat 100 do
                (sleep 0.01)
                (handle-message this-node))))


;; Dve skupiny
(setf *program-1*
      (node-program
        (send-message-as this-node *broadcast* ":connection-request")
        (loop repeat 100 do
                (sleep 0.01)
                (handle-message this-node)))
      
      *program-2*
      (node-program
        (send-message-as this-node *broadcast* ":connection-request")
        (loop repeat 100 do
                (sleep 0.01)
                (handle-message this-node))))
|#

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
;; Pomocná funkce pro počkání než všechny uzly dopočítají
;;

(defun wait-for-all ()
  (format t "Waiting for threads to finish computing...~%")
  (mapcar #'bt:join-thread *threads*)
  (format t "All threads finished computing.~%"))

;;
;; Pomocná funkce pro získání hodnot hodin uzlů (nepouziva lock)
;;

(defun clocks ()
  (mapcar #'clock *nodes*))