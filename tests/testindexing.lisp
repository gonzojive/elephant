
(in-package :ele-tests)

(in-suite* testindexing :in elephant-tests)

(defun setup-testing ()
  #-use-fiveam
  (progn
    (setf regression-test::*debug* t)
    (setf regression-test::*catch-errors* nil))
;;  (trace elephant::indexed-slot-writer)
  (trace ((method initialize-instance :before (persistent))))
  (trace ((method initialize-instance (persistent-object))))
;;  (trace ((method shared-initialize :around (persistent-object t))))
;;  (trace ((method shared-initialize :around (persistent-metaclass t))))
;;  (trace elephant::find-class-index)
;;  (trace get-instances-by-class)
;;  (trace get-instances-by-value)  
  (trace enable-class-indexing)
  (trace get-instances-by-range)
  (trace elephant::cache-instance)
  (trace elephant::get-cached-instance)
  (trace elephant::get-cache)
  (trace elephant::db-transaction-commit)
  )

(defvar inst1)
(defvar inst2)
(defvar inst3)

(test disable-class-indexing-test 
  (5am:finishes
    (when (find-class 'idx-one nil)
      (disable-class-indexing 'idx-one  :errorp nil)
      (setf (find-class 'idx-one) nil))
      
    (defclass idx-one ()
      ((slot1 :initarg :slot1 :initform 1 :accessor slot1 :index t))
      (:metaclass persistent-metaclass))

    (disable-class-indexing 'idx-one  :errorp nil)
    (disable-class-indexing 'idx-one  :errorp nil)
    (setf (find-class 'idx-one) nil)))

(defun wipe-class (name)
  (when (find-class name nil)
    (disable-class-indexing name :errorp nil)
    (setf (find-class name nil) nil)))

(defun wipe-all ()
  (mapcar #'wipe-class
	  '(idx-one-a idx-one-b idx-one-c idx-one-d idx-one-e idx-one-f
	    idx-two idx-cslot idx-three idx-four idx-unbound-del
	    idx-five-del idx-five idx-six idx-seven idx-eight)))

(deftest index-reset
    (listp (wipe-all))
  t)

;;   (defmethod print-object ((obj idx-one) stream)
;;     (print-unreadable-object (obj stream)
;;       (format stream "idx-one slot1 = ~A"
;;               (if (slot-boundp obj 'slot1)
;;                   (slot1 obj)
;;                   "unbound slot")))))

(deftest (indexing-basic-trivial :depends-on index-reset)
    (progn
      (defclass idx-one-a ()
	((slot1 :initarg :slot1 :accessor slot1 :index t))
	(:metaclass persistent-metaclass))
  
      (wipe-class 'idx-one-a)

      (defclass idx-one-a ()
	((slot1 :initarg :slot1 :accessor slot1 :index t))
	(:metaclass persistent-metaclass))

      (with-transaction (:store-controller *store-controller*)
	(setq inst1 (make-instance 'idx-one-a :slot1 101 :sc *store-controller*))
	(setq inst1 (make-instance 'idx-one-a :slot1 101 :sc *store-controller*)))
      
      ;; The real problem is that this call doesn't seem to see it, and the make-instance
      ;; doesn't seem to think it needs to write anything!
      (length (get-instances-by-class 'idx-one-a))
      (wipe-class 'idx-one-a)
      (signals-error (get-instances-by-class 'idx-one-a))
      )
  t)

;; put list of objects, retrieve on value, range and by class
(test (indexing-basic :depends-on index-reset)
  (defclass idx-one-f ()
    ((slot1 :initarg :slot1 :accessor slot1 :index t))
    (:metaclass persistent-metaclass))
  
  (wipe-class 'idx-one-f)

  (defclass idx-one-f ()
    ((slot1 :initarg :slot1 :accessor slot1 :index t))
    (:metaclass persistent-metaclass))

    (let ((n 105))
      (with-transaction (:store-controller *store-controller*)
	(setq inst1 (make-instance 'idx-one-f :slot1 n :sc *store-controller*))
	(setq inst2 (make-instance 'idx-one-f :slot1 n :sc *store-controller*))
	(setq inst3 (make-instance 'idx-one-f :slot1 (+ 1 n) :sc *store-controller*)))
    
      (is (= 3 (length (get-instances-by-class 'idx-one-f))))
      (is (= 2 (length (get-instances-by-value 'idx-one-f 'slot1 n))))
      (is (= 1 (length (get-instances-by-value 'idx-one-f 'slot1 (+ 1 n)))))
      (is (equal (first (get-instances-by-value 'idx-one-f 'slot1 (+ 1 n))) inst3))
      (is (= 3 (length (get-instances-by-range 'idx-one-f 'slot1 n (+ 1 n)))))))

(test (indexing-basic-with-string :depends-on index-reset)
    (defclass idx-one-b ()
      ((slot1 :initarg :slot1 :accessor slot1 :index t))
      (:metaclass persistent-metaclass))
  
    (wipe-class 'idx-one-b)

    (defclass idx-one-b ()
      ((slot1 :initarg :slot1 :accessor slot1 :index t))
      (:metaclass persistent-metaclass))

  (with-transaction (:store-controller *store-controller*)
    (setq inst1 (make-instance 'idx-one-b :slot1 "one" :sc *store-controller*))
    (setq inst2 (make-instance 'idx-one-b :slot1 "two" :sc *store-controller*))
    (setq inst3 (make-instance 'idx-one-b :slot1 "one" :sc *store-controller*))
    (setq inst4 (make-instance 'idx-one-b :slot1 "onethousand" :sc *store-controller*))
    (setq inst5 (make-instance 'idx-one-b :slot1 "only" :sc *store-controller*))
    (setq inst6 (make-instance 'idx-one-b :slot1 "twothousand" :sc *store-controller*)))
  (is (= 6 (length (get-instances-by-class 'idx-one-b))))
  (is (= 2 (length (get-instances-by-value 'idx-one-b 'slot1 "one"))))
  (is (equal (get-instances-by-value 'idx-one-b 'slot1 "two")
             (list inst2))))

(test (larger-indexing :depends-on index-reset)
  (defclass idx-one-c ()
    ((slot1 :initarg :slot1 :accessor slot1 :index t))
    (:metaclass persistent-metaclass))
  
  (wipe-class 'idx-one-c)

  (defclass idx-one-c ()
    ((slot1 :initarg :slot1 :accessor slot1 :index t))
    (:metaclass persistent-metaclass))

  (let ((nr 100)
        instances)
    (flet ((last-in-string (str)
             (subseq str (1- (length str)))))
      (with-transaction (:store-controller *store-controller*)
        (dotimes (i nr)
          (push (make-instance 'idx-one-c
                               :slot1 (read-from-string (last-in-string (princ-to-string i)))
                               :sc *store-controller*)
                instances)))
      (setf instances (nreverse instances))
      (is (= nr (length (get-instances-by-class 'idx-one-c))))
      (is (= 10 (length (get-instances-by-value 'idx-one-c 'slot1 2))))
      (is (= 10 (length (get-instances-by-value 'idx-one-c 'slot1 8))))
      (is (= 10 (length (get-instances-by-value 'idx-one-c 'slot1 0))))
      (is (equal (first (get-instances-by-value 'idx-one-c 'slot1 0))
                 (first instances)))
      (is (equal (second (get-instances-by-value 'idx-one-c 'slot1 0))
                 (elt instances 10))))))

(test (larger-indexing-with-string :depends-on index-reset)
  (defclass idx-one-d ()
    ((slot1 :initarg :slot1 :accessor slot1 :index t))
    (:metaclass persistent-metaclass))
  
  (wipe-class 'idx-one-d)

  (defclass idx-one-d ()
    ((slot1 :initarg :slot1 :accessor slot1 :index t))
    (:metaclass persistent-metaclass))

  (let ((nr 100)
        instances)
    (flet ((last-in-string (str)
             (subseq str (1- (length str)))))
      (with-transaction (:store-controller *store-controller*)
        (dotimes (i nr)
          (push (make-instance 'idx-one-d
                               :slot1 (last-in-string (princ-to-string i))
                               :sc *store-controller*)
                instances)))
      (setf instances (nreverse instances))
      (is (= nr (length (get-instances-by-class 'idx-one-d))))
      (is (= 10 (length (get-instances-by-value 'idx-one-d 'slot1 "2"))))
      (is (= 10 (length (get-instances-by-value 'idx-one-d 'slot1 "8"))))
      (is (= 10 (length (get-instances-by-value 'idx-one-d 'slot1 "0"))))
      (is (equal (first (get-instances-by-value 'idx-one-d 'slot1 "0"))
                 (first instances)))
      (is (equal (second (get-instances-by-value 'idx-one-d 'slot1 "0"))
                 (elt instances 10))))))

(test (indexing-basic-with-symbol :depends-on index-reset)
  (defclass idx-one-e ()
    ((slot1 :initarg :slot1 :accessor slot1 :index t))
    (:metaclass persistent-metaclass))
  
  (wipe-class 'idx-one-e)

  (defclass idx-one-e ()
    ((slot1 :initarg :slot1 :accessor slot1 :index t))
    (:metaclass persistent-metaclass))

  (with-transaction (:store-controller *store-controller*)
    (setq inst1 (make-instance 'idx-one-e :slot1 'one :sc *store-controller*))
    (setq inst2 (make-instance 'idx-one-e :slot1 'two :sc *store-controller*))
    (setq inst3 (make-instance 'idx-one-e :slot1 'one :sc *store-controller*)))
  (is (= 3 (length (get-instances-by-class 'idx-one-e))))
  (is (= 2 (length (get-instances-by-value 'idx-one-e 'slot1 'one))))
  (is (= 1 (length (get-instances-by-value 'idx-one-e 'slot1 'two))))
  (is (equal (get-instances-by-value 'idx-one-e 'slot1 'two)
             (list inst2))))

(deftest (indexing-class-opt :depends-on index-reset)
    (progn
      (defclass idx-cslot ()
	((slot1 :initarg :slot1 :initform 0 :accessor slot1))
	(:metaclass persistent-metaclass) 
	(:index t))

      (wipe-class 'idx-cslot)
      
      (defclass idx-cslot ()
	((slot1 :initarg :slot1 :initform 0 :accessor slot1))
	(:metaclass persistent-metaclass) 
	(:index t))

      (make-instance 'idx-cslot)

      (values (if (class-indexedp-by-name 'idx-cslot) t nil)))
  t)


;; test inherited slots
(deftest (indexing-inherit :depends-on index-reset)
    (progn 
;;      (format t "inherit store: ~A  ~A~%" *store-controller* (controller-path *store-controller*))

      (defclass idx-two ()
	((slot1 :initarg :slot1 :initform 1 :accessor slot1 :index t)
	 (slot2 :initarg :slot2 :initform 2 :accessor slot2 :index t)
	 (slot3 :initarg :slot3 :initform 3 :accessor slot3)
	 (slot4 :initarg :slot4 :initform 4 :accessor slot4 :transient t))
	(:metaclass persistent-metaclass))

      (defclass idx-three (idx-two)
	((slot2 :initarg :slot2 :initform 20 :accessor slot2)
	 (slot3 :initarg :slot3 :initform 30 :accessor slot3 :index t)
	 (slot4 :initarg :slot4 :initform 40 :accessor slot4 :index t))
	(:metaclass persistent-metaclass))

      (wipe-class 'idx-two)
      (wipe-class 'idx-three)

      (defclass idx-two ()
	((slot1 :initarg :slot1 :initform 1 :accessor slot1 :index t)
	 (slot2 :initarg :slot2 :initform 2 :accessor slot2 :index t)
	 (slot3 :initarg :slot3 :initform 3 :accessor slot3)
	 (slot4 :initarg :slot4 :initform 4 :accessor slot4 :transient t))
	(:metaclass persistent-metaclass))

      (defclass idx-three (idx-two)
	((slot2 :initarg :slot2 :initform 20 :accessor slot2)
	 (slot3 :initarg :slot3 :initform 30 :accessor slot3 :index t)
	 (slot4 :initarg :slot4 :initform 40 :accessor slot4 :index t))
	(:metaclass persistent-metaclass))

      (progn
	(with-transaction ()
	  (setq inst1 (make-instance 'idx-two :sc *store-controller*))
	  (setq inst2 (make-instance 'idx-three :sc *store-controller*)))

	(values (slot1 inst1)
		(slot2 inst1)
		(slot3 inst1)
		(slot4 inst1)
		(slot1 inst2)
		(slot2 inst2)
		(slot3 inst2)
		(slot4 inst2)
		(equal (elephant::indexing-record-slots (elephant::indexed-record (find-class 'idx-two)))
		       '(slot1 slot2))
		(equal (elephant::indexing-record-slots (elephant::indexed-record (find-class 'idx-three)))
		       '(slot1 slot3 slot4)))))
  1 2 3 4 1 20 30 40 t t)

(deftest (indexing-range :depends-on index-reset)
    (progn
;;       (format t "range store: ~A  ~A~%" *store-controller* (elephant::controller-spec *store-controller*))

      (defclass idx-four ()
	((slot1 :initarg :slot1 :initform 1 :accessor slot1 :index t))
	(:metaclass persistent-metaclass))

      (wipe-class 'idx-four)

      (defclass idx-four ()
	((slot1 :initarg :slot1 :initform 1 :accessor slot1 :index t))
	(:metaclass persistent-metaclass))

      (defun make-idx-four (val)
	(make-instance 'idx-four :slot1 val))
      
      (with-transaction ()
	(mapc #'make-idx-four '(1 1 1 2 2 4 5 5 5 6 10)))

      (let ((x1 (get-instances-by-range 'idx-four 'slot1 2 6))
	    (x2 (get-instances-by-range 'idx-four 'slot1 0 2))
	    (x3 (get-instances-by-range 'idx-four 'slot1 6 15))
	    )
	;;	(format t " x1 = ~A~%" (mapcar #'slot1 x1))
	;;	(format t " x2 = ~A~%" (mapcar #'slot1 x2))
	;;	(format t " x3 = ~A~%" (mapcar #'slot1 x3))
	(values (equal (mapcar #'slot1 x1)
		       '(2 2 4 5 5 5 6)) ;; interior range
		(equal (mapcar #'slot1 x2)
		       '(1 1 1 2 2))
		(equal (mapcar #'slot1 x3)
		       '(6 10))
		))
      )
  t t t)

(deftest (indexing-slot-makunbound :depends-on index-reset)
    (progn

      (defclass idx-unbound-del ()
	((slot1 :initarg :slot1 :initform 1 :accessor slot1 :index t))
	(:metaclass persistent-metaclass))

      (wipe-class 'idx-unbound-del)

      (defclass idx-unbound-del ()
	((slot1 :initarg :slot1 :initform 1 :accessor slot1 :index t))
	(:metaclass persistent-metaclass))

      (with-transaction (:store-controller *store-controller*)
	(make-instance 'idx-unbound-del :slot1 10))

      (let ((orig-len (length (get-instances-by-class 'idx-unbound-del)))
	    (orig-obj (get-instance-by-value 'idx-unbound-del 'slot1 10)))
	(slot-makunbound orig-obj 'slot1)
	(let ((new-len (length (get-instances-by-class 'idx-unbound-del))) 
	      (index-obj (get-instance-by-value 'idx-unbound-del 'slot1 10)))
	  (values orig-len new-len index-obj))))
  1 1 nil)
      

(deftest (indexing-wipe-index :depends-on index-reset)
    (progn 

      (defclass idx-five-del ()
	((slot1 :initarg :slot1 :initform 1 :accessor slot1 :index t))
	(:metaclass persistent-metaclass))

      (wipe-class 'idx-five-del)

      (defclass idx-five-del ()
	((slot1 :initarg :slot1 :initform 1 :accessor slot1 :index t))
	(:metaclass persistent-metaclass))

      (with-transaction (:store-controller *store-controller*)
	(drop-instances (get-instances-by-class 'idx-five-del))
	(make-instance 'idx-five-del))
      
      (let ((r1 (get-instances-by-value 'idx-five-del 'slot1 1)))

	(defclass idx-five-del ()
	  ((slot1 :initarg :slot1 :initform 1 :accessor slot1))
	  (:metaclass persistent-metaclass))
	(values 
	 (eq (length r1) 1)
	 (signals-error (get-instances-by-value 'idx-five-del 'slot1 1))
	 (null (get-index (get-value 'idx-five-del (elephant::controller-class-root *store-controller*))
			  'slot1)))))
  t t t)

(deftest (indexing-reconnect-db :depends-on index-reset)
    (progn 

      (defclass idx-five ()
	((slot1 :initarg :slot1 :initform 1 :accessor slot1 :index t)
	 (slot2 :initarg :slot2 :initform 2 :accessor slot2)
	 (slot3 :initarg :slot3 :initform 3 :accessor slot3 :index t))
	(:metaclass persistent-metaclass))

      (wipe-class 'idx-five)

      (defclass idx-five ()
	((slot1 :initarg :slot1 :initform 1 :accessor slot1 :index t)
	 (slot2 :initarg :slot2 :initform 2 :accessor slot2)
	 (slot3 :initarg :slot3 :initform 3 :accessor slot3 :index t))
	(:metaclass persistent-metaclass))

      (let ((*default-indexed-class-synch-policy* :db))
	(with-transaction (:store-controller *store-controller*)
	  (make-instance 'idx-five))
	
	;; Wipe out the class so it's not a redefinition
	(setf (find-class 'idx-five) nil)

	;; Assume our db is out of synch with our class def
	(defclass idx-five ()
	  ((slot1 :initarg :slot1 :initform 1 :accessor slot1)
	   (slot2 :initarg :slot2 :initform 2 :accessor slot2 :index t)
	   (slot3 :initarg :slot3 :initform 3 :accessor slot3 :index t))
	  (:metaclass persistent-metaclass))
	
	;; Add an instance of the new class
	(with-transaction ()
	  (make-instance 'idx-five))

	;; DB should dominate (if set as default)
	(values (length (get-instances-by-value 'idx-five 'slot3 3))
		(length (get-instances-by-value 'idx-five 'slot1 1))
		(signals-error (length (get-instances-by-value 'idx-five 'slot2 2))))))
  2 2 t)

(test (indexing-change-class :depends-on index-reset)

      (defclass idx-six ()
	((slot1 :initarg :slot1 :initform 1 :accessor slot1 :index t)
	 (slot2 :initarg :slot2 :initform 2 :accessor slot2 :index t))
	(:metaclass persistent-metaclass))

      (wipe-class 'idx-six)

      (defclass idx-seven ()
	((slot1 :initarg :slot1 :initform 10 :accessor slot1 :index nil)
	 (slot3 :initarg :slot3 :initform 30 :accessor slot3 :index t)
	 (slot4 :initarg :slot4 :initform 40 :accessor slot4 :index t))
	(:metaclass persistent-metaclass))

      (wipe-class 'idx-seven)

      (defclass idx-six ()
	((slot1 :initarg :slot1 :initform 1 :accessor slot1 :index t)
	 (slot2 :initarg :slot2 :initform 2 :accessor slot2 :index t))
	(:metaclass persistent-metaclass))

      (defclass idx-seven ()
	((slot1 :initarg :slot1 :initform 10 :accessor slot1 :index nil)
	 (slot3 :initarg :slot3 :initform 30 :accessor slot3 :index t)
	 (slot4 :initarg :slot4 :initform 40 :accessor slot4 :index t))
	(:metaclass persistent-metaclass))

      (defmethod update-instance-for-different-class :before ((old idx-six)
							      (new idx-seven)
							      &key)
	(setf (slot3 new) (slot2 old)))

      (let ((foo (make-instance 'idx-six)))
	(change-class foo 'idx-seven)
	 ;; shared data from original slot
	(is (= (slot1 foo) 1))
	;; verify old instance access fails
	#+allegro
	(signals program-error (slot2 foo))
	#-allegro
	(signals simple-error (slot2 foo))
	;; verify new instance is there
	(is (= (slot3 foo) 2))
	(is (= (slot4 foo) 40))
	;; verify proper indexing changes (none should lookup a value)
	(is-false (get-instances-by-class 'idx-six))
	(is-false (get-instances-by-value 'idx-six 'slot1 1))
	(is-false (get-instances-by-value 'idx-six 'slot2 2))
	;; new indexes
	(is (= (length (get-instances-by-class 'idx-seven)) 1))
	(is (= (length (get-instances-by-value 'idx-seven 'slot3 2)) 1))))

(deftest (indexing-redef-class :depends-on index-reset)
    (progn

      (defclass idx-eight ()
	((slot1 :accessor slot1 :initarg :slot1 :index t)
	 (slot2 :accessor slot2 :initarg :slot2)
	 (slot3 :accessor slot3 :initarg :slot3 :transient t)
	 (slot4 :accessor slot4 :initarg :slot4 :index t)
	 (slot5 :accessor slot5 :initarg :slot5))
	(:metaclass persistent-metaclass))
      
      (wipe-class 'idx-eight)

      ;;      (format t "sc: ~A  ct: ~A~%" *store-controller* *current-transaction*)
      (defclass idx-eight ()
	((slot1 :accessor slot1 :initarg :slot1 :index t)
	 (slot2 :accessor slot2 :initarg :slot2)
	 (slot3 :accessor slot3 :initarg :slot3 :transient t)
	 (slot4 :accessor slot4 :initarg :slot4 :index t)
	 (slot5 :accessor slot5 :initarg :slot5))
	(:metaclass persistent-metaclass))

      (let ((o1 nil)
	    (o2 nil))
	(with-transaction ()
	  (setf o1 (make-instance 'idx-eight :slot1 1 :slot2 2 :slot3 3 :slot4 4 :slot5 5))
	  (setf o2 (make-instance 'idx-eight :slot1 10 :slot2 20 :slot3 30 :slot4 40 :slot5 50)))

	(defclass idx-eight ()
	  ((slot1 :accessor slot1 :initarg :slot1 :initform 11)
	   (slot2 :accessor slot2 :initarg :slot2 :initform 12 :index t)
	   (slot3 :accessor slot3 :initarg :slot3 :initform 13)
	   (slot6 :accessor slot6 :initarg :slot6 :initform 14 :index t)
	   (slot7 :accessor slot7 :initarg :slot7))
	  (:metaclass persistent-metaclass))
	;;      (format t "indexing redef-class d~%")
	(let ((
	       v1
	       (and (eq (slot1 o1) 1)
		    (signals-error (get-instances-by-value 'idx-eight 'slot1 1))))
	      ;;	      (v1x       (format t "indexing redef-class v1x~%"))
	      (v2 (and (eq (slot2 o1) 2)
		       (eq (length (get-instances-by-value 'idx-eight 'slot2 2)) 1)))
	      ;;	      (v2x       (format t "indexing redef-class v2x~%"))
	      (v3 (eq (slot3 o1) 13)) ;; transient values not preserved (would be inconsistent)
	      ;;	      (v3x       (format t "indexing redef-class v3x~%"))
	      (v4 (and (not (slot-exists-p o1 'slot4))
		       (not (slot-exists-p o1 'slot5))
		       (signals-error (get-instances-by-value 'idx-eight 'slot4 4))))
	      ;;	      (v4x       (format t "indexing redef-class v4x~%"))
	      (v5 (eq (slot6 o1) 14))
	      ;;	      (v5x       (format t "indexing redef-class v5x~%"))
	      (v6 (eq (length (get-instances-by-value 'idx-eight 'slot6 14)) 2))
	      ;;	      (v6x       (format t "indexing redef-class v6x~%"))
	      (v7 (and ;;(slot-exists-p o1 'slot7)
		   (not (slot-boundp o1 'slot7))))
	      ;;	      (v7x       (format t "indexing redef-class v7x~%"))
	      (v8 (and ;;(slot-exists-p o2 'slot7)
		   (not (slot-boundp o2 'slot7))))
	      ;;	      (v8x       (format t "indexing redef-class v8x~%")))
	      )
	      (values 
	       v1 v2 v3 v4 v5 v6 v7 v8))))
      t t t t t t t t)

;; create 500 objects, write each object's slots 

(defvar normal-index nil)

(defgeneric stress1 (obj))

(defun make-stress-classes ()
  (defclass stress-normal ()
    ((stress1 :accessor stress1 :initarg :stress1 :initform nil :index nil)
     (stress2 :accessor stress2 :initarg :stress2 :initform nil :index nil))
    (:metaclass persistent-metaclass))

  (defclass stress-index ()
    ((stress1 :accessor stress1 :initarg :stress1 :initform nil :index t)
     (stress2 :accessor stress2 :initarg :stress2 :initform 2 :index t)
     (stress3 :accessor stress3 :initarg :stress3 :initform 3 :index nil))
    (:metaclass persistent-metaclass)))

(defparameter *stress-count* 700)
(defparameter *range-size* 10)

(defun non-monotonic-stress-def (i)
  (- *stress-count* i)
)

(defun normal-stress-setup (count class-name &rest inst-args)
  (setf normal-index (make-btree))
  (dotimes (i count)
    (setf (get-value i normal-index) (apply #'make-instance class-name :stress1 (non-monotonic-stress-def i) inst-args))))

(defun indexed-stress-setup (count class-name &rest inst-args)  
  (dotimes (i count)
    (progn
    (apply #'make-instance class-name :stress1 (non-monotonic-stress-def i) inst-args))))

(defun normal-range-lookup (count size)
  "Given stress1 slot has values between 1 and count, extract a range of size size that starts
   at (/ count 2)"
  (let* ((objects nil)
	 (start (/ count 2))
	 (end (1- (+ start size))))
    (with-btree-cursor (cur normal-index)
      (loop
	 (multiple-value-bind (value? key val) (cursor-next cur)
	   (declare (ignore key))
	   (cond ((or (not value?)
;; I think these lines were in correctly assuming a particular order.
;;		      (and value?
;;			   (>= (stress1 val) end)
;;			   )
		      )
		  (return-from normal-range-lookup objects))
		 ((and value?
		       (>= (stress1 val) start)
		       (<= (stress1 val) end))
		  (push val objects)))))
      objects)))

(defun normal-lookup ()
  (let ((normal-check nil))
    (dotimes (i *range-size*)
      (push (length (normal-range-lookup *stress-count* *range-size*))
	    normal-check))
    normal-check))

(defun indexed-range-lookup (class count size)
  (let* ((start (/ count 2))
	 (end (1- (+ start size)))
	 (res
    (get-instances-by-range class 'stress1 start end)))
    res
    ))

(defun index-lookup ()
  (let ((index-check nil))
    (dotimes (i *range-size*)
      (push (length (indexed-range-lookup 'stress-index *stress-count* *range-size*))
	    index-check))
    index-check))
  
(deftest (indexing-timing :depends-on index-reset)
    (progn
      (make-stress-classes)
      (let ((insts (get-instances-by-class 'stress-index))
	    (start nil)
	    (end nil)
	    (normal-check nil)
	    (index-check nil)
	    (normal-time 0)
	    (index-time 0))
	(when insts
	  (drop-instances insts :sc *store-controller*))

	(with-transaction ()
	  (normal-stress-setup *stress-count* 'stress-normal :stress2 10))
	
	(with-transaction ()
	  (indexed-stress-setup *stress-count* 'stress-index :stress2 10))

	(setf start (get-internal-run-time))
	(setf normal-check (normal-lookup))
	(setf end (get-internal-run-time))
	(setf normal-time (/ (- end start 0.0) internal-time-units-per-second))

	(setf start (get-internal-run-time))
	(setf index-check (index-lookup))
	(setf end (get-internal-run-time))
	(setf index-time (/ (- end start 0.0) internal-time-units-per-second))
	(format t "~%Ranged get of ~A/~A objects = Linear: ~A sec Indexed: ~A sec~%"
		*range-size* *stress-count* normal-time index-time)
	(and (equal normal-check index-check) (> normal-time index-time)))
      )
  t)
  

      

