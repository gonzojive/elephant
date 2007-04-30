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
  (read-controller-slot sc (oid instance) name))

(defmethod persistent-slot-writer ((sc prev-store-controller) value instance name)
  (write-controller-slot sc (oid instance) name value))

(defmethod persistent-slot-boundp ((sc prev-store-controller) instance name)
  (multiple-value-bind (value found?) 
      (read-controller-slot sc (oid instance) name)
    (when found? t)))    

;;
;; Slot storage
;;

(defun build-controller-slots (hint)
  (make-array (floor (* 1.5 hint)) :initial-element nil :adjustable t))

(defun read-controller-slot (sc oid slotname)
  (let ((pair (assoc slotname (aref (controller-slots sc) oid))))
    (if (null pair)
	(values nil nil)
	(values (cdr pair) t))))

(defun write-controller-slot (sc oid slotname value)
  (let ((pair (assoc slotname (aref (controller-slots sc) oid))))
    (if (null pair)
	(setf (aref (controller-slots sc) oid)
	      (acons slotname value (aref (controller-slots sc) oid)))
	(setf (cdr pair) value))))

	      

reclaim oids (to save array space) on restore?

store slot values with objects in snapshot; 
overload serializer to reconstruct persistent metaclass objects