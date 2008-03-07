;;; -*- Mode: Lisp; Syntax: ANSI-Common-Lisp; Base: 10 -*-
;;;
;;; cached-slots.lisp -- cached persistent slot accesses
;;; 
;;; part of
;;;
;;; Elephant: an object-oriented database for Common Lisp
;;;
;;; Original Copyright (c) 2004 by Andrew Blumberg and Ben Lee
;;; <ablumberg@common-lisp.net> <blee@common-lisp.net>
;;;
;;; Portions Copyright (c) 2005-2007 by Robert Read and Ian Eslick
;;; <rread common-lisp net> <ieslick common-lisp net>
;;;
;;; Elephant users are granted the rights to distribute and use this software
;;; as governed by the terms of the Lisp Lesser GNU Public License
;;; (http://opensource.franz.com/preamble.html), also known as the LLGPL.
;;;

(in-package :elephant)

;;(defmethod slot-value-using-class
;;    ((class persistent-metaclass) (instance persistent-object) (slot-def cached-slot-definition))
;;  (call-next-method))

(defmethod (setf slot-value-using-class)
    (new-value (class persistent-metaclass) (instance persistent-object) (slot-def cached-slot-definition))
  "Write through caching of slot value"
  (persistent-slot-writer (get-con instance) new-value instance (slot-definition-name slot-def))
  (call-next-method))

(defmethod slot-boundp-using-class 
    ((class persistent-metaclass) (instance persistent-object) (slot-def cached-slot-definition))
  "Checks if the slot exists in the database."
  (unless (call-next-method)
    (persistent-slot-boundp (get-con instance) instance (slot-definition-name slot-def))))

(defmethod slot-makunbound-using-class 
    ((class persistent-metaclass) (instance persistent-object) (slot-def cached-slot-definition))
  "Removes the slot value from the database."
  (persistent-slot-makunbound (get-con instance) instance (slot-definition-name slot-def))
  (call-next-method))

(defun initialize-cached-slots (instance slots)
  (let ((sc (get-con instance)))
    (loop for slot in slots do
	 (when (persistent-slot-boundp sc instance slot)
	   (let ((value (persistent-slot-reader sc instance slot)))
	     (setf (slot-value instance slot) value))))))


				