;;; -*- Mode: Lisp; Syntax: ANSI-Common-Lisp; Base: 10 -*-
;;;
;;; Initial version 9/02/2004 by Ben Lee
;;; <blee@common-lisp.net>
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

(defpclass performance-test ()
  ((slot1-is-a-test-slot :accessor perfslot1 :initarg :s1 :initform 1)
   (slot2-is-a-really-long-symbol :accessor perfslot2 :initarg :s2 :initform 2)
   (slot3-is-so-long-we-shouldnt-even-talk-about-it-lest-we-die :accessor perfslot3 :initarg :s3 :initform 3)))

(defun performance-test ()
  (let ((a (make-array 500))
	(b (make-array 500 :element-type 'fixnum)))

    (loop for j from 0 to 20 do
	 (with-transaction ()
	   (loop for i from 0 below 500 do
		(setf (aref a i) (make-instance 'performance-test :s1 10 :s2 20 :s3 30))))
	 (with-transaction ()
	   (loop for i from 0 below 500 do
		(setf (perfslot2 (aref a i)) 30)
		(setf (aref b i) (+ (* (perfslot2 (aref a i)) 
				       (perfslot3 (aref a i)))
				    (perfslot1 (aref a i))))))
	 (every (lambda (elt) (= elt 910)) b))))
	   

(defun serializer-performance-test ()
  (elephant-memutil::with-buffer-streams (key val)
    (loop for i from 0 upto 1000000 do
	 (serialize 'persistent-symbol-test key *store-controller*)
	 (deserialize key *store-controller*)
	 (elephant-memutil::reset-buffer-stream key))))

(defun slot-access-test ()
  (let ((pt (make-instance 'performance-test))
	(var 0))
    (loop for i from 0 upto 1000000 do
	 (setq var (perfslot1 pt)))))

(defclass simple-class ()
  ((slot1 :accessor slot1 :initform 20)
   (slot-number-two :accessor slot2 :initform "This is a test")
   (slot3 :accessor slot3 :initform 'state-is-idle)
   (slot4 :accessor slot4 :initform 'test)))

(defun regular-class-test (sc)
  (let ((src (make-array 500))
	(targ (make-array 500))
	(bt (make-btree sc)))
    (loop for i from 0 below 500 do
	 (setf (aref src i) 
	       (make-instance 'simple-class)))
    (time
     (loop for j from 0 upto 20 do
	  (with-transaction (:store-controller sc)
	    (loop for elt across src 
	       for i from 0 do
		 (setf (get-value i bt) elt)))
	  (with-transaction (:store-controller sc)
	    (loop for elt across src
	       for i from 0 do
		 (setf (aref targ i) (get-value i bt))))))))

(defun serializer-stdclass-test ()
  (let ((inst (make-instance 'simple-class)))
    (elephant-memutil::with-buffer-streams (key val)
      (loop for i from 0 upto 100000 do
	   (serialize inst key *store-controller*)
	   (deserialize key *store-controller*)
	   (elephant-memutil::reset-buffer-stream key)))))
