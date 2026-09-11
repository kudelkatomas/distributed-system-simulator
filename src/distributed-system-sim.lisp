;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;
;;;; File: distributed-system-sim.lisp
;;;; Author: Tomáš Kudělka
;;;;
;;;; Description:
;;;;   Definitions of base classes: Message, Node, Network
;;;;

(require "asdf")
(asdf:load-system :bordeaux-threads)

(defparameter *broadcast* 0)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; Message
;;;

(defclass message ()
  ((sender-id :initarg :sender-id
              :initform (error "Message must have a sender-id.")
              :reader sender-id)
   (receiver-id :initarg :receiver-id
                :initform (error "Message must have a receiver-id.")
                :reader receiver-id)
   (timestamp :initarg :timestamp
              :initform (error "Message must have a timestamp.")
              :reader timestamp)
   (content :initarg :content
            :initform (error "Message must have content.")
            :reader content)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; Node
;;;

(defmacro node-program (&body body)
  "THIS-NODE is bound to the node in BODY."
  `(lambda (this-node)
     ,@body))

(defclass node ()
  ((id :initarg :id
       :initform (error "Node must have an ID.")
       :reader id)
   (running-p :initform nil
              :reader running-p)
   (running-p-lock :initform (bt:make-lock))
   (message-queue :initform nil)
   (message-queue-lock :initform (bt:make-lock))
   (program :initarg :program
            :initform (error "Node must have a program."))
   (thread :initform nil)
   (network :initarg :network
            :initform (error "Node must be connected to some network.")
            :reader network)
   (known-node-ids :initform nil)
   (clock :initform 0
          :reader clock)))

; Not local
(defmethod start ((nd node))
  (bt:with-lock-held ((slot-value nd 'running-p-lock))
    (unless (running-p nd)
      (setf (slot-value nd 'running-p) t)
      (setf (slot-value nd 'thread)
            (bt:make-thread
             (lambda ()
               (unwind-protect
                   (progn
                     (join (network nd) nd)
                     (funcall (slot-value nd 'program) nd))
                 (unwind-protect
                     (leave (network nd) nd)
                   (bt:with-lock-held ((slot-value nd 'running-p-lock))
                     (setf (slot-value nd 'running-p) nil))))
               :name (format nil "node-~a" (id nd))))))))

;;;
;;; Message queue
;;;

; Not local
(defmethod enqueue-message ((nd node) (msg message))
  (bt:with-lock-held ((slot-value nd 'running-p-lock))
    (when (running-p nd)
      (bt:with-lock-held ((slot-value nd 'message-queue-lock))
        (setf (slot-value nd 'message-queue)
              (append (slot-value nd 'message-queue)
                      (list msg)))))))

; Local
(defmethod dequeue-message ((nd node))
  (bt:with-lock-held ((slot-value nd 'message-queue-lock))
    (when (slot-value nd 'message-queue)
      (pop (slot-value nd 'message-queue)))))

; Local
(defmethod process-next-message ((nd node))
  (let ((msg (dequeue-message nd)))
    (when msg
      (let ((msg-type (read-from-string (content msg))))
        (handle-message nd msg-type msg)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; Message handling
;;;
;;; Message types:
;;;   :connection-request |
;;;   :introduction |
;;;   :resource-request |
;;;   :resource-request-ack |
;;;   :resource-release
;;;
;;; Broadcast: (= receiver-id *broadcast*)
;;;

(defgeneric handle-message (node message-type msg)
  (:documentation "Handle MSG directed to NODE based on its MESSAGE-TYPE."))

; Local
(defmethod handle-message ((nd node) (type (eql :connection-request)) msg)
  (let ((requester-id (sender-id msg)))
    (connect nd requester-id)
    (send-message-as nd requester-id ":introduction")))

; Local
(defmethod handle-message ((nd node) (type (eql :introduction)) msg)
  (connect nd (sender-id msg)))

;;;
;;; Helper methods
;;;

; Local
(defmethod connect ((nd node) sender-id)
  (pushnew sender-id (slot-value nd 'known-node-ids)))

; Local
(defmethod send-message-as ((nd node) receiver-id content)
  (send-message (network nd)
                (make-instance 'message
                               :sender-id (id nd)
                               :receiver-id receiver-id
                               :content content
                               :timestamp (clock nd))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; Network
;;;
;;; Stores a list of connected nodes and distributes messages.
;;;
;;; Broadcast: (= receiver-id *broadcast*)
;;;

(defclass network ()
  ((nodes :initform nil)
   (nodes-lock :initform (bt:make-lock))))

(defmethod nodes ((nw network))
  (bt:with-lock-held ((slot-value nw 'nodes-lock))
    (copy-list (slot-value nw 'nodes))))

(defmethod join ((nw network) (nd node))
  (bt:with-lock-held ((slot-value nw 'nodes-lock))
    (push nd (slot-value nw 'nodes))))

(defmethod leave ((nw network) (nd node))
  (bt:with-lock-held ((slot-value nw 'nodes-lock))
    (setf (slot-value nw 'nodes)
          (remove nd (slot-value nw 'nodes)))))

(defmethod send-message ((nw network) (msg message))
  (labels ((broadcast-message ()
             (mapc (lambda (node)
                     (unless (= (sender-id msg) (id node))
                       (enqueue-message node msg)))
                   (nodes nw)))

           (direct-message ()
             (let ((receiver (find (receiver-id msg)
                                   (nodes nw)
                                   :key #'id)))
               (when receiver
                 (enqueue-message receiver msg)))))

    (if (= (receiver-id msg) *broadcast*)
        (broadcast-message)
      (direct-message))))