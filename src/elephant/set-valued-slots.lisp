;;; -*- Mode: Lisp; Syntax: ANSI-Common-Lisp; Base: 10 -*-
;;;
;;; set-valued-slots.lisp -- persistent slot accesses
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

;; ============================================
;;  Set-valued Slot Access Override
;; ============================================

(defmethod slot-value-using-class ((class persistent-metaclass) (instance persistent-object) (slot-def set-valued-slot-definition))
  "Ensure that there is a slot-set in the slot (lazy instantiation)"
  (handler-case
      (call-next-method)
    (unbound-slot ()
      (setf (slot-value-using-class class instance slot-def)
	    (build-slot-set (get-con instance))))))


(defmethod (setf slot-value-using-class) 
    (new-value (class persistent-metaclass) (instance persistent-object) (slot-def set-valued-slot-definition))
  "Setting a value adds it to the slot set"
  (if (or (null new-value)
	  (subtypep (type-of new-value) 'slot-set))
      (call-next-method)
      (insert-item new-value (slot-value-using-class class instance slot-def))))

(defmethod slot-makunbound-using-class ((class persistent-metaclass) (instance persistent-object) (slot-def set-valued-slot-definition))
  "Make sure we reclaim the pset storage"
  (awhen (and (slot-boundp-using-class class instance slot-def)
	      (slot-value-using-class class instance slot-def))
    (drop-slot-set it))
  (call-next-method))

;; ===========================================
;;  Slot set API
;; ===========================================


(defgeneric build-slot-set (sc)
  (:documentation "Construct an empty default pset or backend specific pset.
                   This is an internal function used by make-pset"))

(defgeneric insert-item (item slot-set)
  (:documentation "Insert a new item into the pset"))

(defgeneric remove-item (item slot-set)
  (:documentation "Remove specified item from pset"))

(defgeneric find-item (item slot-set &key key test)
  (:documentation "Find a an item in the pset using key and test"))

(defgeneric slot-set-list (slot-set)
  (:documentation "Convert items of pset into a list for processing"))

(defgeneric map-slot-set (fn slot-set)
  (:documentation "Map operator for psets"))

(defgeneric drop-slot-set (pset)
  (:documentation "Release pset storage to database for reuse"))


;; ============================================
;;  A generic slot set implementation
;; ============================================

(defclass slot-set () ()
  (:documentation "A proxy object for a set stored in a slot."))

(defpclass default-slot-set (slot-set default-pset) ()
  (:documentation "A default slot-set implementation"))

(defmethod build-slot-set ((sc store-controller))
  "Slot-set creation API"
  (let ((btree (make-btree sc)))
    (make-instance 'default-slot-set :btree btree :sc sc)))

(defmethod map-slot-set (fn (set default-slot-set))
  (map-pset fn set))

(defmethod slot-set-list ((set default-slot-set))
  (pset-list set))

(defmethod drop-slot-set ((set default-slot-set))
  (drop-pset set))
