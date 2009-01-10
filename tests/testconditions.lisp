;;; testconditions.lisp
;;;
;;; part of
;;;
;;; Elephant: an object-oriented database for Common Lisp
;;;
;;; Copyright (c) 2007 by Ian Eslick
;;; <ieslick common-lisp net>
;;;
;;; Elephant users are granted the rights to distribute and use this software
;;; as governed by the terms of the Lisp Lesser GNU Public License
;;; (http://opensource.franz.com/preamble.html), also known as the LLGPL.

(in-package :ele-tests)

(deftest cross-store-reference-condition
  (if (or (not (boundp '*test-spec-secondary*))
	  (null *test-spec-secondary*))
      (progn 
	(format t "~%Second store spec missing: ignoring")
	(values t t t t))
      (let ((sc2 (open-store *test-spec-secondary* :recover t :deadlock-detect nil))
	    (*store-controller* *store-controller*)
	    (sc1 *store-controller*))
	(unwind-protect
	     (let ((inst1 (make-instance 'pfoo :slot1 100 :sc sc1))
		   (inst2 (make-instance 'pfoo :slot1 200 :sc sc2)))
	       (values 
		(is-not-null (add-to-root 'inst1 inst1 :sc sc1))
		(is-not-null (add-to-root 'inst2 inst2 :sc sc2))
		(signals-specific-condition (cross-reference-error)
		  (add-to-root 'inst1 inst1 :sc sc2))
		(signals-specific-condition (cross-reference-error)
		  (add-to-root 'inst2 inst2 :sc sc1))))
	  (close-store sc2))))
  t t t t)

(define-condition inhibit-rollback-test () ())

(defun inhibit-rollback-p (c)
  (subtypep (type-of c)
	    'inhibit-rollback-test))

(defpclass rollback-test ()
  ((slot1 :accessor slot1 :initarg :slot1)))

(deftest inhibit-rollback
    (if (eq (first (elephant::controller-spec *store-controller*)) :BDB)
	(let ((test (make-instance 'rollback-test :slot1 1))
	      test1 test2 errorp)
	  ;; Side effect works, we exit without error; save results
	  (finishes
	    (with-transaction (:inhibit-rollback-fn 'inhibit-rollback-p)
	      (setf (slot1 test) 2)
	      (signal 'inhibit-rollback-test)))
	  (setf test1 (slot1 test))
	  (setf (slot1 test) 2)
	  ;; Side effect fails (stays 2) and error is signaled
	  (setf errorp
	    (signals-error
	      (with-transaction (:inhibit-rollback-fn 'inhibit-rollback-p)
		(setf (slot1 test) 3)
		(error))))
	  (setf test2 (slot1 test))
	  (values 
	   test1 test2 errorp
	   ;; works plain too
	   (with-transaction ()
	     (setf (slot1 test) 10))))
	(values 2 2 t 10))
  2 2 t 10)
						    
	    

