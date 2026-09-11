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

(defmacro node-program (&body body)
  "THIS-NODE is bound to the node in BODY."
  `(lambda (this-node)
     ,@body))

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