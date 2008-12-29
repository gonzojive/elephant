;;; testcaching.lisp
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
;;;

(in-package :ele-tests)


(in-suite* testcaching :in elephant-tests)

(defpclass cache-test ()
  ((slot1 :accessor slot1 :initarg :slot1)
   (slot2 :accessor slot2 :initarg :slot2 :cached t))
  (:cache-style :checkout))

(defpclass cache-test2 ()
  ((slot1 :accessor slot1 :initarg :slot1)
   (slot2 :accessor slot2 :initarg :slot2 :cached t))
  (:cache-style :none))
  

(test caching-basic
  (let ((test (make-instance 'cache-test :slot1 1 :slot2 2)))
    (is (= (slot1 test) 1))
    (is (= (slot2 test) 2))
;    (persistent-checkout test)
;;    (is-true (persistent-checked-out-p test))
;    (is-true (= (slot1 test) 1))
;;    (is-true (= (slot2 test) 2))
;;    (setf (slot1 test) 10)
;;    (setf (slot2 test) 20)
;;    (is-true (= (elephant::persistent-slot-reader *store-controller* test 'slot1)
;;		10))
;;    (is-true (= (elephant::persistent-slot-reader *store-controller* test 'slot2)
;;		2))
;;    (persistent-checkin test)
;;    (is-true (= (elephant::persistent-slot-reader *store-controller* test 'slot2)
;;		20))
;;    (is-true (= (slot1 test) 10))
;;    (is-true (= (slot2 test) 20))
  ))

(test (caching-cancel-checkout :depends-on caching-basic)
  (let ((test (make-instance 'cache-test :slot1 1 :slot2 2)))
    (persistent-checkout test)
    (setf (slot2 test) 20)
    (persistent-checkout-cancel test)
    (is (= (slot2 test) 2))))

(test (caching-with-checkouts :depends-on caching-basic)
  (let ((test (make-instance 'cache-test :slot1 1 :slot2 2)))
    (with-persistent-checkouts (test)
      (is-true (persistent-checked-out-p test))
      (is-true (slot-boundp test 'slot2))
      (is (= (slot2 test) 2))
      (setf (slot2 test) 20)
      (is (= (elephant::persistent-slot-reader *store-controller* test 'slot2)
	     2))
      (is (= (slot2 test) 20)))))

(test caching-inhibited
  (let ((test (make-instance 'cache-test2 :slot1 1 :slot2 2)))
    (is (= (slot1 test) 1))
    (is (= (slot2 test) 2))
    (5am:signals error 
      (persistent-checkout test))
    (setf (slot2 test) 3)
    (is (= (slot2 test) 3))))

(test caching-enable-disable
  (let ((test (make-instance 'cache-test2 :slot1 1 :slot2 2)))
    (is (= (slot1 test) 1))
    (is (= (slot2 test) 2))
    ;; Mode is :none - inhibited
    (5am:signals error 
      (persistent-checkout test))
    (unwind-protect
	 (progn (setf (caching-style (class-of test)) :checkout)
		;; We can checkout now; verify caching works
		(5am:finishes (persistent-checkout test))
		(setf (slot2 test) 20)
		(is (= (elephant::persistent-slot-reader *store-controller* test 'slot2)
		       2))
		(is (= (slot2 test) 20))
		(5am:finishes (persistent-checkin test))
		;; We can checkout now; verify checkin succeeded
		(is (= (slot2 test) 20))
		(is (= (elephant::persistent-slot-reader *store-controller* test 'slot2)
		       20))
		;; Verify reset works
		(setf (caching-style (class-of test)) :none)
		(5am:signals error 
		  (persistent-checkout test)))
      (setf (caching-style (class-of test)) :none))))

(test caching-change-mode
  ;; What happens to checked out instances when we change modes?
  (let ((test (make-instance 'cache-test2 :slot1 1 :slot2 20)))
    (unwind-protect
	 (progn
	   (setf (caching-style (class-of test)) :checkout)
	   (5am:finishes (persistent-checkout test))
	   (setf (slot2 test) 2)
	   (is (= (elephant::persistent-slot-reader *store-controller* test 'slot2)
		  20))
	   (setf (caching-style (class-of test)) :none)
	   ;; Can we still checkin?
	   (5am:signals error 
	     (persistent-checkin test))
	   ;; Did it work?
	   (is (= 20 (slot2 test)))
	   (is (= 20 (elephant::persistent-slot-reader *store-controller* test 'slot2))))
    (setf (caching-style (class-of test)) :none))))

(test caching-style-required
  (5am:signals error
    (defpclass cache-test-no-arg-bad ()
      ((slot1 :cached t))))
  (5am:finishes
    (defpclass cached-test-inherit-ok (cache-test)
      ((slot3 :cached t)))))


