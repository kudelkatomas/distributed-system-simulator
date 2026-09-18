;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;
;;;; File: mutex-lamport.lisp
;;;; Author: Tomáš Kudělka
;;;;
;;;; Description:
;;;;   Mutual exclusion for shared resource access using
;;;;   Lamport's system of logical clocks: https://doi.org/10.1145/359545.359563
;;;;
;;;; Assumptions:
;;;;   For mutual exclusion to work, each node must be connected to every other node.
;;;;   Precisely, in HOLDS-RESOURCE-P, it is assumed that the given node has received
;;;;   at least one message from every other node.
;;;;

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; Lamport node
;;;
;;; Subclass of NODE implementing mutual exclusion.
;;;
;;; Possible resource states are :requested, :granted, :released.
;;; Default state is :released.
;;;

(defclass lamport-node (node)
  ((resource-state :initform :released
                   :reader resource-state
                   :documentation "Possible states are :requested, :granted, :released. Default state is :released.")
   (request-timestamp :initform 0
                      :reader request-timestamp)
   (request-queue :initform nil
                  :reader request-queue)
   (known-node-clocks :initform nil
                      :reader known-node-clocks
                      :documentation "List of pairs (id . id's clock value)")))

;;;
;;; Resource
;;;

;; Local
;; Notice that (clock nd) after sending the request message
;; is equal to the timestamp sent by send-message-as,
;; since clock value can only be changed locally,
;; and no local operation can interfere with request-resource.
(defmethod request-resource ((nd lamport-node))
  (setf (slot-value nd 'resource-state) :requested)
  (send-message-as nd *broadcast* ":resource-request")
  (let ((timestamp (clock nd)))
    (setf (slot-value nd 'request-timestamp) timestamp)
    (enqueue-request nd (id nd) timestamp))
  nd)

;; Local
(defmethod release-resource ((nd lamport-node))
  (setf (slot-value nd 'resource-state) :released)
  (request-queue-remove nd (id nd))
  (send-message-as nd *broadcast* ":resource-release")
  nd)

;; Local
(defmethod holds-resource-p ((nd lamport-node))
  "Check if ND holds the resource according to Lamport's conditions 5.(i) and 5.(ii). Assumes ND has received at least one message from every other node in the network."
  (or (eql (resource-state nd) :granted)
      (let ((request-queue-head (request-queue-head nd)))
        (when (and request-queue-head
                   ; 5.(i) from Lamport's paper
                   (= (id nd) (car request-queue-head))
                   ; 5.(ii) from Lamport's paper
                   (all-other-clocks-greater-than-request-timestamp-p nd))
          (setf (slot-value nd 'resource-state) :granted)
          t))))

;; Local
(defmethod all-other-clocks-greater-than-request-timestamp-p ((nd lamport-node))
  "Check Lamport's condition 5.(ii). Assumes ND has received at least one message from every other node in the network."
  (let ((known-node-clocks (known-node-clocks nd)))
    (or (null known-node-clocks) ; ND is alone in the network.
        (< (request-timestamp nd)
           (reduce #'min known-node-clocks :key #'cdr)))))

;;;
;;; Request queue operations
;;;

;; Local
(defmethod enqueue-request ((nd lamport-node) requester-id timestamp)
  (push (cons requester-id timestamp) (slot-value nd 'request-queue))
  nd)

;; Local
(defmethod request-queue-head ((nd lamport-node))
  (when (request-queue nd)
    (reduce (lambda (min-pair pair)
              (cond ((< (cdr pair) (cdr min-pair)) pair)
                    ((and (= (cdr pair) (cdr min-pair))
                          (< (car pair) (car min-pair))) pair)
                    (t min-pair)))
            (request-queue nd))))

;; Local
(defmethod request-queue-remove ((nd lamport-node) releaser-id)
  (setf (slot-value nd 'request-queue)
        (remove-if (lambda (node-id) (eql node-id releaser-id))
                   (request-queue nd)
                   :key #'car))
  nd)

;;;
;;; Known nodes clocks
;;;

;; Local
(defmethod update-known-node-clocks ((nd lamport-node) sender-id timestamp)
  (unless (= sender-id (id nd))
    (let ((pair (assoc sender-id (known-node-clocks nd))))
      (if pair
          (setf (cdr pair) (max (cdr pair) timestamp))
        (push (cons sender-id timestamp) (slot-value nd 'known-node-clocks)))))
  nd)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; Message handling extension
;;;
;;; New message types: :resource-request, :resource-request-ack, :resource-release
;;;

;; Local
(defmethod handle-message :after ((nd lamport-node) msg-type (msg message))
  (update-known-node-clocks nd (sender-id msg) (timestamp msg))
  nd)

;; Local
(defmethod handle-message ((nd lamport-node) (msg-type (eql :resource-request)) (msg message))
  (let ((requester-id (sender-id msg)))
    (enqueue-request nd requester-id (timestamp msg))
    (send-message-as nd requester-id ":resource-request-ack"))
  nd)

;; Local
(defmethod handle-message ((nd lamport-node) (msg-type (eql :resource-release)) (msg message))
  (request-queue-remove nd (sender-id msg))
  nd)
