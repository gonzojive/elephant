;;; -*- Mode: Lisp; Syntax: ANSI-Common-Lisp; Base: 10 -*-
;;;
;;; associations.lisp -- persistent slot accesses
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

;; ===============================
;;  Association Slots
;; ===============================

(defmethod slot-value-using-class 
    ((class persistent-metaclass) (instance persistent-object) (slot-def association-slot-definition))
  (case (association-type slot-def)
    (:end (call-next-method))
    (:m21 (get-associated instance slot-def))
    (:m2m (get-associated instance slot-def))))

(defmethod (setf slot-value-using-class) 
    (new-value (class persistent-metaclass) (instance persistent-object) (slot-def association-slot-definition))
  (when (type-check-association instance slot-def new-value)
    (let ((sc (get-con instance)))
      (ensure-transaction (:store-controller sc)
	(case (association-type slot-def)
	  (:end (update-association-end class instance slot-def new-value)
		(persistent-slot-writer sc new-value instance (slot-definition-name slot-def)))
	  (:m21 (update-other-association-end class instance slot-def new-value))
	  (:m2m (update-association-end class instance slot-def new-value)
		(update-other-association-end class instance slot-def new-value))))))
  new-value)

(defmethod slot-boundp-using-class 
    ((class persistent-metaclass) (instance persistent-object) (slot-def association-slot-definition))
  (when (association-end-p slot-def)
    (call-next-method)))
  
(defmethod slot-makunbound-using-class 
    ((class persistent-metaclass) (instance persistent-object) (slot-def association-slot-definition))
  (when (eq (association-type slot-def) :end)
    (call-next-method) ;; remove storage
    (remove-association class instance slot-def)))


;; =========================
;; Handling reads
;; =========================

(defun type-check-association (instance slot-def other-instance)
  (when (null other-instance)
    (return-from type-check-association t))
  (unless (subtypep (type-of other-instance) (foreign-classname slot-def))
    (cerror "Ignore and return"
	    "Value ~A written to association slot ~A of instance ~A 
             of class ~A must be a subtype of ~A"
	    other-instance (foreign-slotname slot-def) instance
	    (type-of instance) (foreign-classname slot-def))
    (return-from type-check-association nil))
  (unless (equal (db-spec instance) (db-spec other-instance))
    (cerror "Ignore and return"
	    "Cannot association objects from different controllers:
             ~A is in ~A and ~A is in ~A"
	    instance (get-con instance)
	    other-instance (get-con other-instance))
    (return-from type-check-association nil))
  t)

(defun get-associated (instance slot-def)
  (let* ((fclass (get-foreign-class slot-def))
	 (fslot (get-foreign-slot fclass slot-def))
	 (sc (get-con instance))
	 (index (get-association-index fslot sc)))
    (flet ((map-obj (value oid)
	     (declare (ignore value))
	     (controller-recreate-instance sc oid)))
      (declare (dynamic-extent map-obj))
      (map-btree #'map-obj index :value (oid instance) :collect t))))


;; ==========================
;;  Handling updates
;; ==========================

(defun update-association-end (class instance slot-def target)
  "Get the association index and add the target object as a key that
   refers back to this instance so we can get the set of referrers to target"
  (let ((index (get-association-index slot-def (get-con instance))))
    (when (and (association-end-p slot-def)
	       (slot-boundp-using-class class instance slot-def))
      (remove-kv-pair (slot-value-using-class class instance slot-def) (oid target) index))
    (setf (get-value (oid target) index) (oid instance))))

(defun update-other-association-end (class instance slot-def other-instance)
  "Update the association index for the other object so that it maps from
   us to it.  Also add error handling."
  (declare (ignore class))
  (let* ((fclass (class-of other-instance))
	 (fslot (get-foreign-slot fclass slot-def))
	 (sc (get-con other-instance)))
    (update-association-end fclass other-instance fslot instance)
    (persistent-slot-writer sc instance other-instance (slot-definition-name fslot))))


(defun get-foreign-class (slot-def)
  (find-class (foreign-classname slot-def)))

(defun get-foreign-slot (fclass slot-def)
  (find-slot-def-by-name fclass (foreign-slotname slot-def)))

;; =============================
;;  Late-binding Initialization
;; =============================

(defun get-association-index (slot-def sc)
  (ifret (get-association-slot-index slot-def sc)
    (aif (get-controller-association-index slot-def sc)
	 (progn (add-association-slot-index it slot-def sc) it)
	 (let ((new-idx (make-dup-btree sc)))
	   (add-slot-index sc new-idx (foreign-classname slot-def) (foreign-slotname slot-def))
	   (add-association-slot-index new-idx slot-def sc)
	   new-idx))))

(defun get-controller-association-index (slot-def sc)
  (let* ((master (controller-index-table sc))
	 (base (association-slot-base slot-def))
	 (slotname (slot-definition-name slot-def)))
    (get-value (cons base slotname) master)))

