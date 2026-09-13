;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;
;;;; File: mutex-lamport.lisp
;;;; Author: Tomáš Kudělka
;;;;
;;;; Description:
;;;;   Mutual exlusion using Lamport's system of logical clocks
;;;;   Lamport's paper: https://doi.org/10.1145/359545.359563
;;;;

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; Lamport node
;;;
;;; The node class extension enabling mutual exclusion
;;;

(defclass lamport-node (node)
  ((resource-state :initform :released
                   :accessor resource-state)
   (request-timestamp :initform 0
                      :accessor request-timestamp)
   (request-queue :initform nil
                  :accessor request-queue)
   (known-node-ids-clocks :initform nil
                          :reader known-node-ids-clocks
                          :documentation "List of pairs (id . id's clock value)")))

;;;
;;; Resource
;;;

;; Local
(defmethod request-resource ((nd node))
  (setf (resource-state nd) :requested)
  (send-message-as nd *broadcast* ":resource-request")
  (setf (request-timestamp nd) (clock nd))
  (enqueue-request nd (id nd) (clock nd)))

;; Local
(defmethod release-resource ((nd node))
  (setf (resource-state nd) :released)
  (request-queue-remove nd (id nd))
  (send-message-as nd *broadcast* ":resource-release"))

;; Local
(defmethod holds-resource-p ((nd node))
  (or (eql (resource-state nd) :granted)
      (and (= (id nd)                    ; 5.(i) from Lamport's paper
              (car (request-queue-head nd)))
           (< (request-timestamp nd)     ; 5.(ii) from Lamport's paper
              (oldest-latest-received-message-timestamp nd))
           (setf (resource-state nd) :granted))))

;; Local
(defmethod oldest-latest-received-message-timestamp ((nd node))
  (reduce #'min (slot-value nd 'node-ids) :key #'cdr))

;;;
;;; Request queue
;;;

;; Local
(defmethod enqueue-request ((nd node) requester-id timestamp)
  (push (cons requester-id timestamp) (request-queue nd)))

;; Local
(defmethod request-queue-head ((nd node))
  (reduce (lambda (min-pair pair)
            (cond ((< (cdr pair) (cdr min-pair)) pair)
                  ((and (= (cdr pair) (cdr min-pair))
                        (< (car pair) (car min-pair))) pair)
                  (t min-pair)))
          (request-queue nd)))

;; Local
(defmethod request-queue-remove ((nd node) releaser-id)
  (setf (request-queue nd)
        (remove-if (lambda (node-id) (eql node-id releaser-id))
                   (request-queue nd)
                   :key #'car)))

;;;
;;; Known node ids clocks
;;;

;; Local
(defmethod update-known-node-ids-clocks ((nd node) sender-id timestamp)
  (let ((pair (find sender-id (known-node-ids-clocks nd) :key #'car)))
    (cond ((and pair (> timestamp (cdr pair)))
           (setf (slot-value nd 'known-node-ids-clocks)
                 (replace-element (slot-value nd 'known-node-ids-clocks)
                                  sender-id
                                  (cons sender-id timestamp)
                                  :key #'car)))
          ((not pair)
           (push (cons sender-id timestamp) (slot-value nd 'known-node-ids-clocks))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; Message handling extension
;;;
;;; New message types: :resource-request, :resource-request-ack, :resource-release
;;;

;;;
;;; To be completed
;;;

;; Local
(defmethod handle-message :after ((nd node) msg-type (msg message))
  (update-known-node-ids-clocks nd (sender-id msg) (timestamp msg)))

;; Local
(defmethod handle-message ((nd node) (msg-type (eql :resource-request)) (msg message))
  (let ((requester-id (sender-id msg)))
    (enqueue-request nd requester-id (timestamp msg))
    (send-message-as nd requester-id ":resource-request-ack")))

;; Local
(defmethod handle-message ((nd node) (msg-type (eql :resource-release)) (msg message))
  (request-queue-remove nd (sender-id nd)))
