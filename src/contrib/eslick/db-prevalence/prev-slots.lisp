;;; -*- Mode: Lisp; Syntax: ANSI-Common-Lisp; Base: 10 -*-
;;;
;;; prev-collections.lisp -- Prevalence collections
;;; 
;;; Initial version 5/1/2007 by Ian Eslick
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
;;;

(in-package :db-prevalence)

;;
;; Slot api
;;

(defmethod persistent-slot-reader ((sc prev-store-controller) instance name)
  (multiple-value-bind (value exists?)
      (read-controller-slot sc (oid instance) name)
    (if exists? value
	(error 'unbound-slot :instance instance :name name))))

(defmethod persistent-slot-writer ((sc prev-store-controller) value instance name)
  (write-controller-slot sc (oid instance) name value))

(defmethod persistent-slot-boundp ((sc prev-store-controller) instance name)
  (multiple-value-bind (value found?) 
      (read-controller-slot sc (oid instance) name)
    (declare (ignore value))
    (when found? t)))

(defmethod persistent-slot-makunbound ((sc prev-store-controller) instance name)
  (unbind-controller-slot sc (oid instance) name))

;;
;; Slot storage manpulation
;;

(defun build-controller-slots (hint)
  (make-array (max (floor (* 1.5 hint)) 1000) :initial-element nil :adjustable t))

(defun maybe-extend-slots (sc oid)
  (when (>= oid (length (controller-slots sc)))
    (setf (controller-slots sc) 
	  (adjust-array (controller-slots sc) (ceiling (* (length (controller-slots sc)) 1.5))))))

(defun read-controller-slot (sc oid slotname)
;;  (maybe-extend-slots sc oid)
  (let* ((list (aref (controller-slots sc) oid))
	 (pair (when (listp list) (assoc slotname list))))
    (if (or (null pair) (not (consp pair)))
	(values nil nil)
	(values (cdr pair) t))))

(defun write-controller-slot (sc oid slotname value)
;;  (maybe-extend-slots sc oid)
  (let* ((list (aref (controller-slots sc) oid))
	 (pair (when list (assoc slotname list))))
    (if (null pair)
	(setf (aref (controller-slots sc) oid)
	      (acons slotname value (aref (controller-slots sc) oid)))
	(setf (cdr pair) value))))

(defun unbind-controller-slot (sc oid slotname)
  (maybe-extend-slots sc oid)
  (let* ((list (aref (controller-slots sc) oid)))
    (when (assoc slotname list)
      (setf (aref (controller-slots sc) oid)
	    (remove slotname list :key #'car)))
    t))

