;;; mop-tests.lisp
;;;
;;; part of
;;;
;;; Elephant: an object-oriented database for Common Lisp
;;;
;;; Copyright (c) 2004 by Andrew Blumberg and Ben Lee
;;; <ablumberg@common-lisp.net> <blee@common-lisp.net>
;;;
;;; Elephant users are granted the rights to distribute and use this software
;;; as governed by the terms of the Lisp Lesser GNU Public License
;;; (http://opensource.franz.com/preamble.html), also known as the LLGPL.

(in-package :ele-tests)

;; For ccl; ensure the class is in the appropriate place
(defclass pineapple ()() (:metaclass persistent-metaclass))
(defclass p-initform-test ()() (:metaclass persistent-metaclass))
(defclass p-initform-test-2 ()() (:metaclass persistent-metaclass))
(defclass no-eval-initform ()() (:metaclass persistent-metaclass))
(defclass class-one ()() (:metaclass persistent-metaclass))
(defclass p-class ()() (:metaclass persistent-metaclass))
(defclass make-persistent2 ()() (:metaclass persistent-metaclass))
(defclass mix-1 ()() (:metaclass persistent-metaclass))
(defclass mix-2 ()() (:metaclass persistent-metaclass))
(defclass mix-3 ()() (:metaclass persistent-metaclass))
(defclass mix-4 ()() (:metaclass persistent-metaclass))
(defclass mix-5 ()() (:metaclass persistent-metaclass))
(defclass mix-6 ()() (:metaclass persistent-metaclass))
(defclass update-class ()() (:metaclass persistent-metaclass))
(defclass class-two ()() (:metaclass persistent-metaclass))
(defclass redef ()() (:metaclass persistent-metaclass))
(defclass minus-p-class ()() (:metaclass persistent-metaclass))
(defclass nonp-class ()())
(defclass switch-transient ()() (:metaclass persistent-metaclass))
(defclass make-persistent ()() (:metaclass persistent-metaclass))


(deftest non-transient-class-slot-1
    (signals-condition
      ;; This should fail (principle of least surprise)
      (defclass non-transient-class-slot-1 ()
	((slot3 :accessor slot3 :allocation :class))
	(:metaclass persistent-metaclass)))
  t)

(deftest non-transient-class-slot-2
    (signals-condition
      ;; as should this
      (defclass non-transient-class-slot-2 ()
	((slot3 :accessor slot3 :allocation :class :transient nil))
	(:metaclass persistent-metaclass)))
  t)

(deftest transient-class-slot
    (finishes
     ;; but this should be fine
     (defclass transient-class-slot ()
       ((slot3 :accessor slot3 :allocation :class :transient t))
       (:metaclass persistent-metaclass)))
  t)

(deftest class-definers
    (progn
;;    (finishes
     (defclass p-class ()
       ((slot1 :accessor slot1)
	(slot2 :accessor slot2 :transient t)
	(slot3 :accessor slot3 :allocation :class :transient t))
       (:metaclass persistent-metaclass))
     (finalize-inheritance (find-class 'p-class))
     (defclass nonp-class ()
       ((slot1 :accessor slot1)
	(slot2 :accessor slot2)
	(slot3 :accessor slot3 :allocation :class)))
     (finalize-inheritance (find-class 'nonp-class))
     (defclass minus-p-class ()
       ((slot1 :accessor slot1 :transient t)
	(slot2 :accessor slot2)
	(slot3 :accessor slot3))
       (:metaclass persistent-metaclass))
     (finalize-inheritance (find-class 'minus-p-class))
     (defclass switch-transient ()
       ((slot1 :accessor slot1 :transient t)
	(slot2 :accessor slot2))
       (:metaclass persistent-metaclass))
     (finalize-inheritance (find-class 'switch-transient))
     (defclass make-persistent ()
       ((slot2 :accessor slot2))
       (:metaclass persistent-metaclass))
     (finalize-inheritance (find-class 'make-persistent))
     t)
  t)

(deftest (bad-inheritence :depends-on class-definers)
    (signals-condition
     ;; This should fail
     (defclass bad-inheritence (p-class) ()))
  t)

(deftest (mixes :depends-on class-definers)
    (progn
;;    (finishes
     ;; but this should be fine
     (defclass mix-1 (p-class nonp-class) ()
       (:metaclass persistent-metaclass))
     (finalize-inheritance (find-class 'mix-1))
     ;; This should be ok
     (defclass mix-2 (p-class minus-p-class) ()
       (:metaclass persistent-metaclass))
     (finalize-inheritance (find-class 'mix-2))
     ;; This should be ok
     (defclass mix-3 (minus-p-class p-class) ()
       (:metaclass persistent-metaclass))
     (finalize-inheritance (find-class 'mix-3))
     ;; This should be ok 
     (defclass mix-4 (switch-transient p-class) ()
       (:metaclass persistent-metaclass))
     (finalize-inheritance (find-class 'mix-4))
     ;; This should be ok
     (defclass mix-5 (p-class switch-transient) ()
       (:metaclass persistent-metaclass))
     (finalize-inheritance (find-class 'mix-5))
     ;; should work
     (defclass mix-6 (make-persistent p-class) ()
       (:metaclass persistent-metaclass))
     (finalize-inheritance (find-class 'mix-6))
     t)
  t)

(deftest (mixes-right-slots :depends-on mixes)
    (are-not-null
     (typep (find-slot-def 'mix-1 'slot1) 'ele::persistent-slot-definition)
     (typep (find-slot-def 'mix-1 'slot2) 'ele::transient-slot-definition)
     (typep (find-slot-def 'mix-1 'slot3) 'ele::transient-slot-definition)
     (typep (find-slot-def 'mix-2 'slot1) 'ele::persistent-slot-definition)
     (typep (find-slot-def 'mix-2 'slot2) 'ele::persistent-slot-definition)
     (typep (find-slot-def 'mix-2 'slot3) 'ele::persistent-slot-definition)
     (typep (find-slot-def 'mix-3 'slot1) 'ele::persistent-slot-definition)
     (typep (find-slot-def 'mix-3 'slot2) 'ele::persistent-slot-definition)
     (typep (find-slot-def 'mix-3 'slot3) 'ele::persistent-slot-definition)
     (typep (find-slot-def 'mix-4 'slot1) 'ele::persistent-slot-definition)
     (typep (find-slot-def 'mix-4 'slot2) 'ele::persistent-slot-definition)
     (typep (find-slot-def 'mix-4 'slot3) 'ele::transient-slot-definition)
     (typep (find-slot-def 'mix-5 'slot1) 'ele::persistent-slot-definition)
     (typep (find-slot-def 'mix-5 'slot2) 'ele::persistent-slot-definition)
     (typep (find-slot-def 'mix-5 'slot3) 'ele::transient-slot-definition)
     (typep (find-slot-def 'mix-6 'slot1) 'ele::persistent-slot-definition)
     (typep (find-slot-def 'mix-6 'slot2) 'ele::persistent-slot-definition)
     (typep (find-slot-def 'mix-6 'slot3) 'ele::transient-slot-definition))
  t t t t t t t t t t t t t t t t t t)

(deftest (inherit :depends-on class-definers)
;;    (finishes
    (progn
     (defclass make-persistent2 (p-class)
       ((slot2 :accessor slot2)
	(slot4 :accessor slot4 :transient t))
       (:metaclass persistent-metaclass))
     (finalize-inheritance (find-class 'make-persistent2))
     t)
  t)

(deftest (inherit-right-slots :depends-on inherit)
    (are-not-null
     (typep (find-slot-def 'make-persistent2 'slot1) 
	    'ele::persistent-slot-definition)
     (typep (find-slot-def 'make-persistent2 'slot2) 
	    'ele::persistent-slot-definition)
     (typep (find-slot-def 'make-persistent2 'slot3) 
	    'ele::transient-slot-definition)
     (typep (find-slot-def 'make-persistent2 'slot4) 
	    'ele::transient-slot-definition))
  t t t t)

(deftest initform-classes
    (progn
;;    (finishes
      (defclass p-initform-test () 
	((slot1 :initform 10)) 
	(:metaclass persistent-metaclass))
      (finalize-inheritance (find-class 'p-initform-test))
      (defclass p-initform-test-2 () 
	((slot1 :initarg :slot1 :initform 10)) 
	(:metaclass persistent-metaclass))
      (finalize-inheritance (find-class 'p-initform-test-2))
      t)
  t)

(deftest redefinition-initform
    (progn
      (defclass pineapple () ()
        (:metaclass persistent-metaclass))
      (let ((fruit (make-instance 'pineapple)))
        (defclass pineapple ()
          ((tree :initform 1))
            (:metaclass persistent-metaclass))
        (values (slot-value fruit 'tree))))
    1)

(deftest (initform-test :depends-on initform-classes)
    (slot-value (make-instance 'p-initform-test :sc *store-controller*) 'slot1)
  10)

(deftest (initarg-test :depends-on initform-test)
    (values
     (slot-value (make-instance 'p-initform-test-2 :sc *store-controller*) 'slot1)
     (slot-value (make-instance 'p-initform-test-2 :slot1 20 :sc *store-controller*) 'slot1))
  10 20)

(deftest no-eval-initform
;;    (finishes
    (progn
      (defclass no-eval-initform ()
	((slot1 :initarg :slot1 :initform (error "Shouldn't be called")))
	(:metaclass persistent-metaclass))
      (make-instance 'no-eval-initform :slot1 "something" :sc *store-controller* )
      t)
  t)

(deftest redefclass
    (progn
      (defclass redef () () (:metaclass persistent-metaclass))
      (finalize-inheritance (find-class 'redef))
      (defclass redef () () (:metaclass persistent-metaclass))
      (is-not-null (subtypep 'redef 'persistent-object)))
  t)

(deftest slot-unbound
    (progn
      (defclass class-one ()
       (slot1 slot2)
       (:metaclass persistent-metaclass))
      (defmethod slot-unbound (class (instance class-one)
                              (slot-name (eql 'slot2)))
       t)
      (let ((inst (make-instance 'class-one)))
       (values
        (signals-specific-condition (unbound-slot)
          (slot-value inst 'slot1))
        (is-not-null (eq t (slot-value inst 'slot2))))))
  t t)

(deftest (makunbound :depends-on class-definers)
    (let ((p (make-instance 'p-class :sc *store-controller*)))
      (with-transaction (:store-controller *store-controller*)
	(setf (slot1 p) t)
	(slot-makunbound p 'slot1))
      (signals-condition (slot1 p)))
  t)

(deftest update-class
    (progn
      (defclass update-class () 
	((slot1 :initform 1 :accessor slot1))
	(:metaclass persistent-metaclass))
      (let* ((foo (make-instance 'update-class :sc *store-controller*)))
	(defclass update-class ()
	  ((slot2 :initform 2 :accessor slot2))
	  (:metaclass persistent-metaclass))
	(values
	 (slot2 foo)
	 (signals-condition (slot1 foo)))))
  2 t)

;; Bound values are retained
(deftest change-class
    (progn
      (defclass class-one ()
	((slot1 :initform 1 :accessor slot1))
	(:metaclass persistent-metaclass))

      (defclass class-two ()
	((slot1 :initform 0 :accessor slot1)
	 (slot2 :initform 2 :accessor slot2))
	(:metaclass persistent-metaclass))

	(let* ((foo (make-instance 'class-one :sc *store-controller*)))
	  (change-class foo (find-class 'class-two))
	  (values
	   (slot1 foo)
	   (slot2 foo))))
  1 2)

;; Unbound values are also retained!
(deftest change-class2
    (progn
      (defclass class-one ()
	((slot1 :accessor slot1))
	(:metaclass persistent-metaclass))
      
      (defclass class-two ()
	((slot1 :initform 0 :accessor slot1)
	 (slot2 :initform 2 :accessor slot2))
	(:metaclass persistent-metaclass))

      	(let* ((foo (make-instance 'class-one :sc *store-controller*)))
	  (change-class foo (find-class 'class-two))
	  (values
	   (slot-boundp foo 'slot1)
	   (slot2 foo))))
  nil 2)


      
;; does CHANGE-INSTANCE-SLOT handle unbound values properly?
(deftest change-instance-slot-unbound-value
  (progn
    (defpclass cisuv () ((s)))
    (let ((obj (make-instance 'cisuv)))
      (format t "persistent -> indexed~%")
      (defpclass cisuv2 () ((s :index t)))
      (change-class obj 'cisuv2)

      (format t "indexed -> indexed~%")
      (defpclass cisuv3 () ((s :index t)))
      (change-class obj 'cisuv3)

      (format t "indexed -> persistent~%")
      (change-class obj 'cisuv))
    t)
  t)

