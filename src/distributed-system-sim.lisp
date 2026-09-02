;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; Simulátor distribuovaného systému
;;;
;;; Tomáš Kudělka
;;;

(require "asdf")
(asdf:load-system :bordeaux-threads)

(defparameter *broadcast* 0)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Pomocné funkce a makra
;;

; (n n+1 ... m)
(defun range (n m)
  (if (> n m)
      nil
    (cons n (range (1+ n) m))))

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

; Převzato z: https://lispcookbook.github.io/cl-cookbook/process.html
(defmacro until (condition &body body)
  "Loops around until the condition becomes true."
  (let ((block-name (gensym)))
    `(block ,block-name
       (loop
          (if ,condition
              (return-from ,block-name nil)
            (progn ,@body))))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Message
;; (sender-id receiver-id timestamp content)
;;
;; Pokud (= receiver-id *broadcast*) pak jde o broadcast
;;
;; Protokol
;; :connection-request | :introduction | :resource-request | :resource-request-ack |
;; | :resource-release
;;

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


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Node
;; (id running-p message-queue program lambda-thread network node-ids clock
;;  resource-state request-timestamp request-queue)
;;
;; V "program" je "this-node" navázano na dany uzel.
;;
;; node-ids = ((node-id . latest-time) ...)
;;
;; resource-state = :requested | :granted | :released
;; "released" je výchozí hodnota
;;
;; request-queue = ((requester-id . timestamp) ...)
;;

(defmacro node-program (&body body)
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
   (lambda-thread :initform nil)
   (network :initarg :network
            :initform (error "Node must be connected to some network.")
            :reader network)
   (node-ids :initform nil)
   (clock :initform 0
          :reader clock)
   (resource-state :initform :released
                   :accessor resource-state)
   (request-timestamp :initform 0
                      :accessor request-timestamp)
   (request-queue :initform nil
                  :accessor request-queue)))

;; Before concurrency
(defmethod initialize-instance ((nd node) &key)
  (call-next-method)
  (initialize-lambda-thread nd)
  (join (slot-value nd 'network) nd))

;; Before concurrency
(defmethod initialize-lambda-thread ((nd node))
  (setf (slot-value nd 'lambda-thread)
        (lambda ()
          (bt:make-thread
           (lambda ()
             (progn
               (funcall (slot-value nd 'program) nd)
               (bt:with-lock-held ((slot-value nd 'running-p-lock))
                 (setf (slot-value nd 'running-p) nil))))))))

;; Before concurrency
(defmethod start ((nd node))
  (bt:with-lock-held ((slot-value nd 'running-p-lock))
    (unless (running-p nd)
      (setf (slot-value nd 'running-p) t)
      (funcall (slot-value nd 'lambda-thread)))))

; Not local
(defmethod enqueue-message ((nd node) (msg message))
  (bt:with-lock-held ((slot-value nd 'running-p-lock))
    (when (running-p nd)
      (bt:with-lock-held ((slot-value nd 'message-queue-lock))
        (setf (slot-value nd 'message-queue)
              (append (slot-value nd 'message-queue)
                      (list msg)))))))

; Local
(defmethod increment-clock ((nd node))
  (incf (slot-value nd 'clock)))

; Local
(defmethod dequeue-message ((nd node))
  (bt:with-lock-held ((slot-value nd 'message-queue-lock))
    (when (slot-value nd 'message-queue)
      (pop (slot-value nd 'message-queue)))))

; Local
(defmethod handle-message ((nd node))
  (let ((msg (dequeue-message nd)))
    (when msg
      (let ((sender-id (sender-id msg))
            (timestamp (timestamp msg))
            (content   (content msg)))
        (lamport-update-clock nd timestamp)
        (case (read-from-string content)
          (:connection-request (handle-connection-request nd sender-id timestamp))
          (:introduction (connect nd sender-id timestamp))
          (:resource-request (handle-resource-request nd sender-id timestamp))
          (:resource-release (request-queue-remove nd sender-id)))
        (update-node-ids-clocks nd sender-id timestamp)))))

; Local - Inkrementace hodin po
(defmethod lamport-update-clock ((nd node) timestamp)
  (setf (slot-value nd 'clock)
        (1+ (max (clock nd) timestamp))))

; Local
(defmethod handle-connection-request ((nd node) requester-id timestamp)
  (connect nd requester-id timestamp)
  (send-message-as nd requester-id ":introduction"))

; Local
(defmethod handle-resource-request ((nd node) requester-id timestamp)
  (enqueue-request nd requester-id timestamp)
  (send-message-as nd requester-id ":resource-request-ack"))  

; Local
(defmethod connect ((nd node) sender-id timestamp)
  (unless (member sender-id (slot-value nd 'node-ids) :key #'car)
    (push (cons sender-id timestamp) (slot-value nd 'node-ids))))

; Local - Inkrementace hodin před
(defmethod send-message-as ((nd node) receiver-id content)
  (increment-clock nd)
  (send-message (network nd)
                (make-instance 'message
                               :sender-id (id nd)
                               :receiver-id receiver-id
                               :content content
                               :timestamp (clock nd))))

; Local
(defmethod update-node-ids-clocks ((nd node) node-id timestamp)
  (let ((pair (find node-id (slot-value nd 'node-ids) :key #'car)))
    (when (and pair
               (> timestamp (cdr pair)))
      (setf (slot-value nd 'node-ids)
            (replace-element (slot-value nd 'node-ids)
                             node-id
                             (cons node-id timestamp)
                             :key #'car)))))

; Local
(defmethod request-resource ((nd node))
  (setf (resource-state nd) :requested)
  (send-message-as nd *broadcast* ":resource-request")
  (setf (request-timestamp nd) (clock nd))
  (enqueue-request nd (id nd) (clock nd)))

; Local
(defmethod release-resource ((nd node))
  (setf (resource-state nd) :released)
  (request-queue-remove nd (id nd))
  (send-message-as nd *broadcast* ":resource-release"))

; Local
(defmethod enqueue-request ((nd node) requester-id timestamp)
  (push (cons requester-id timestamp) (request-queue nd)))

; Local
(defmethod request-queue-head ((nd node))
  (reduce (lambda (min-pair pair)
            (cond ((< (cdr pair) (cdr min-pair)) pair)
                  ((and (= (cdr pair) (cdr min-pair))
                        (< (car pair) (car min-pair))) pair)
                  (t min-pair)))
          (request-queue nd)))

; Local
(defmethod request-queue-remove ((nd node) releaser-id)
  (setf (request-queue nd)
        (remove-if (lambda (node-id) (eql node-id releaser-id))
                   (request-queue nd)
                   :key #'car)))

; Local
(defmethod holds-resource-p ((nd node))
  (or (eql (resource-state nd) :granted)
      (and (= (id nd)                    ; 5.(i) z Lamportova textu
              (car (request-queue-head nd)))
           (< (request-timestamp nd)     ; 5.(ii) z Lamportova textu
              (oldest-latest-received-message-timestamp nd))
           (setf (resource-state nd) :granted))))

; Local
(defmethod oldest-latest-received-message-timestamp ((nd node))
  (reduce #'min (slot-value nd 'node-ids) :key #'cdr))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Network
;; (nodes)
;;
;; Uchovává si seznam uzlů, které se připojily, a rozesílá zprávy.
;; Pokud (= receiver-id *broadcast*) pak jde o broadcast
;;

(defclass network ()
  ((nodes :initform nil)
   (nodes-lock :initform (bt:make-lock))
   (sending-lock :initform (bt:make-lock))))

(defmethod nodes ((nw network))
  (bt:with-lock-held ((slot-value nw 'nodes-lock))
    (copy-list (slot-value nw 'nodes))))

(defmethod join ((nw network) (nd node))
  (bt:with-lock-held ((slot-value nw 'nodes-lock))
    (push nd (slot-value nw 'nodes))))

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