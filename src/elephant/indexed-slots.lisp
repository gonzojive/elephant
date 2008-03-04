;;; -*- Mode: Lisp; Syntax: ANSI-Common-Lisp; Base: 10 -*-
;;;
;;; slots.lisp -- persistent slot accesses
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

;; ======================================================
;; Indexed slot accesses
;; ======================================================

(defmethod (setf slot-value-using-class) :around
    (new-value (class persistent-metaclass) (instance persistent-object) (slot-def indexed-slot-definition))
  "Update indices when writing an indexed slot.  Make around method to ensure a single transaction
   for write + index update"
  (let ((sc (get-con instance))
	(oid (oid instance)))
    (ensure-transaction (:store-controller sc)
      (let ((idx (get-slot-def-index slot-def sc))
	    (old-value (when (slot-boundp-using-class class instance slot-def)
			 (slot-value-using-class class instance slot-def))))
	(unless idx
	  (setf idx (initialize-slot-def-index slot-def sc)))
	(when old-value 
	  (remove-kv-pair old-value oid idx))
	(setf (get-value new-value idx) oid))
      (call-next-method))))

    
(defun initialize-slot-def-index (slot-def sc)
  (let* ((master (controller-index-table sc))
	 (idx-ref (cons (indexed-slot-base slot-def) (slot-definition-name slot-def)) ))
    (aif (get-value idx-ref master)
	 (progn (add-slot-def-index it slot-def sc) it)
	 (let ((new-idx (make-dup-btree sc)))
	   (setf (get-value idx-ref master) new-idx)
	   (add-slot-def-index new-idx slot-def sc)
	   new-idx))))

(defmethod remove-kv-pair (key value (dbt dup-btree))
  "Yuck!  Is there a more efficient way to update an index than:
   Read, delete, write?  At least the read caches the delete page?"
  (let ((sc (get-con dbt)))
    (ensure-transaction (:store-controller sc)
      (with-btree-cursor (cur dbt)
	(multiple-value-bind (exists? k v)
	    (cursor-set-range cur key)
	  (declare (ignore k))
  	  (if (and exists? (eq v value))
	      (cursor-delete cur)
	      (loop do
		 (multiple-value-bind (exists? k v)
		     (cursor-next cur)
		   (declare (ignore k))
		   (when (not exists?)
		     (error "Cannot remove key/value ~A/~A from dup-btree ~A: not found"
			    key value dbt))
		   (when (eq v value)
		     (cursor-delete cur)
		     (return value))))))))))

;; ======================================================
;; Set-valued Slot Accesses
;; ======================================================

;; ======================================================
;; Lisp-specific overrides of normal slot operation
;; ======================================================

;;
;; ALLEGRO 
;;

#+allegro
(defmethod slot-makunbound-using-class ((class persistent-metaclass) (instance persistent-object) (slot-name symbol))
  (loop for slot in (class-slots class)
     until (eq (slot-definition-name slot) slot-name)
     finally (return (if (typep slot 'persistent-slot-definition)
;;			 (if (indexed-p class)
;;			     (indexed-slot-makunbound class instance slot)
			 (slot-makunbound-using-class class instance slot)
			 (call-next-method)))))


#+allegro
(defun make-persistent-reader (name slot-definition class class-name)
  (eval `(defmethod ,name ((instance ,class-name))
	  (slot-value-using-class ,class instance ,slot-definition))))

#+allegro
(defun make-persistent-writer (name slot-definition class class-name)
  (let ((name (if (and (consp name)
		       (eq (car name) 'setf))
		  name
		  `(setf ,name))))
    (eval `(defmethod ,name ((instance ,class-name) value)
	     (setf (slot-value-using-class ,class instance ,slot-definition)
		   value)))))

#+allegro
(defmethod initialize-accessors ((slot-definition persistent-slot-definition) class)
  (let ((readers (slot-definition-readers slot-definition))
	(writers (slot-definition-writers slot-definition))
	(class-name (class-name class)))
    (loop for reader in readers
	  do (make-persistent-reader reader slot-definition class class-name))
    (loop for writer in writers
	  do (make-persistent-writer writer slot-definition class class-name))))

;;
;; CMU / SBCL
;;

#+(or cmu sbcl)
(defun make-persistent-reader (name)
  (lambda (instance)
    (declare (type persistent-object instance))
    (persistent-slot-reader (get-con instance) instance name)))

#+(or cmu sbcl)
(defun make-persistent-writer (name)
  (lambda (new-value instance)
    (declare (optimize (speed 3))
	     (type persistent-object instance))
    (persistent-slot-writer (get-con instance) new-value instance name)))

#+(or cmu sbcl)
(defun make-persistent-slot-boundp (name)
  (lambda (instance)
    (declare (type persistent-object instance))
    (persistent-slot-boundp (get-con instance) instance name)))

#+sbcl ;; CMU also?  Old code follows...
(defmethod initialize-internal-slot-functions ((slot-def persistent-slot-definition))
  (let ((name (slot-definition-name slot-def)))
    (setf (slot-definition-reader-function slot-def)
	  (make-persistent-reader name))
    (setf (slot-definition-writer-function slot-def)
	  (make-persistent-writer name))
    (setf (slot-definition-boundp-function slot-def)
	  (make-persistent-slot-boundp name)))
  (call-next-method)) ;;  slot-def)

#+cmu
(defmethod initialize-internal-slot-functions ((slot-def persistent-slot-definition))
  (let ((name (slot-definition-name slot-def)))
    (setf (slot-definition-reader-function slot-def)
	  (make-persistent-reader name))
    (setf (slot-definition-writer-function slot-def)
	  (make-persistent-writer name))
    (setf (slot-definition-boundp-function slot-def)
	  (make-persistent-slot-boundp name)))
  slot-def)

;;
;; LISPWORKS
;;

#+lispworks
(defmethod slot-value-using-class ((class persistent-metaclass) (instance persistent-object) slot)
  (let ((slot-def (or (find slot (class-slots class) :key 'slot-definition-name)
		      (find slot (class-slots class)))))
    (if (typep slot-def 'persistent-slot-definition)
	(persistent-slot-reader (get-con instance) instance (slot-definition-name slot-def))
	(call-next-method class instance (slot-definition-name slot-def)))))

#+lispworks
(defmethod (setf slot-value-using-class) (new-value (class persistent-metaclass) (instance persistent-object) slot)
  "Set the slot value in the database."
  (let ((slot-def (or (find slot (class-slots class) :key 'slot-definition-name)
		      (find slot (class-slots class)))))
    (if (typep slot-def 'persistent-slot-definition)
	(if (indexed class)
	    (indexed-slot-writer class instance slot-def new-value)
	    (persistent-slot-writer (get-con instance) new-value instance (slot-definition-name slot-def)))
	(call-next-method new-value class instance (slot-definition-name slot-def)))))

#+lispworks
(defmethod slot-makunbound-using-class ((class persistent-metaclass) (instance persistent-object) slot)
  "Removes the slot value from the database."
  (let ((slot-def (or (find slot (class-slots class) :key 'slot-definition-name)
		      (find slot (class-slots class)))))
    (if (typep slot-def 'persistent-slot-definition)
	(if (indexed class)
	    (indexed-slot-makunbound class instance slot-def)
	    (persistent-slot-makunbound (get-con instance) instance (slot-definition-name slot-def)))
	(call-next-method class instance (slot-definition-name slot-def)))))

