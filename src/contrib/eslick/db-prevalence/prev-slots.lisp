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
      (read-controller-slot (oid instance) name sc)
    (if exists? value
	(error 'unbound-slot :instance instance :name name))))

(defmethod persistent-slot-writer ((sc prev-store-controller) value instance name)
  (write-controller-slot value (oid instance) name sc))

(defmethod persistent-slot-boundp ((sc prev-store-controller) instance name)
  (multiple-value-bind (value found?) 
      (read-controller-slot (oid instance) name sc)
    (declare (ignore value))
    (when found? t)))

(defmethod persistent-slot-makunbound ((sc prev-store-controller) instance name)
  (unbind-controller-slot (oid instance) name sc))

;;
;; Internal slot operations
;;

(defun read-controller-slot (oid slot sc)
  (let* ((list (object-record-slots (get-object-record oid sc)))
	 (pair (when (listp list) (assoc slot list))))
    (if (or (null pair) (not (consp pair)))
	(values nil nil)
	(values (cdr pair) t))))

(defun write-controller-slot (value oid slot sc)
  (let* ((record (get-object-record oid sc))
         (list (object-record-slots record))
	 (pair (when (listp list) (assoc slot list))))
    (if (or (null pair) (not (consp pair)))
	(push (cons slot value) (object-record-slots record))
	(setf (cdr pair) value)))
  value)

(defun unbind-controller-slot (oid slot sc)
  (let* ((record (get-object-record oid sc))
	 (list (object-record-slots record)))
    (when (assoc slot list)
      (setf (object-record-slots record)
	    (remove slot list :key #'car))))
  t)

