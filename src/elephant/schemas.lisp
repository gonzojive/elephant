;;; -*- Mode: Lisp; Syntax: ANSI-Common-Lisp; Base: 10 -*-
;;;
;;; metaclasses.lisp -- persistent objects via metaobjects
;;; 
;;; Initial version 8/26/2004 by Andrew Blumberg
;;; <ablumberg@common-lisp.net>
;;; 
;;; part of
;;;
;;; Elephant: an object-oriented database for Common Lisp
;;;
;;; Copyright (c) 2004 by Andrew Blumberg and Ben Lee
;;; <ablumberg@common-lisp.net> <blee@common-lisp.net>
;;;
;;; Portions Copyright (c) 2005-2007 by Robert Read and Ian Eslick
;;; <rread common-lisp net> <ieslick common-lisp net>
;;;
;;; Elephant users are granted the rights to distribute and use this software
;;; as governed by the terms of the Lisp Lesser GNU Public License
;;; (http://opensource.franz.com/preamble.html), also known as the LLGPL.
;;;
(in-package "ELEPHANT")

(declaim #-elephant-without-optimize (optimize (speed 3) (safety 1)))

(defclass schema ()
  ((classname :accessor schema-classname :initarg :name)
   (prior :accessor schema-prior :initarg :prior
	  :documentation "Schema evolution support; maintains a list of
                          older schemas.  Can hold oid or object reference")))

(defclass standard-schema (schema)
  ((slot-list :accessor schema-ordered-slot-list :initarg :slot-order)))

(defclass persistent-schema (schema)
  ((pslots :accessor schema-persistent-slots :initarg :pslots
	   :documentation "Persistent slots")
   (islots :accessor schema-indexed-slots :initarg :islots
	   :documentation "Indexed slots")
   (sslots :accessor schema-set-slots :initarg :sslots
	   :documentation "List of set slot names and ids")
   (aslots :accessor schema-assoc-slots :initarg :aslots
	   :documentation "List of slotname/association ids")))

;;
;; Compute a schema from a definition
;;

(defmethod compute-schema ((class-obj standard-class))
  (make-instance 'standard-schema
		 :name (class-name class-obj)
		 :prior nil
		 :slot-order (mapcar #'slot-definition-name (class-slots class-obj))))

;;
;; For schemas stored in a database
;;

(defclass db-schema ()
  ((id :accessor schema-id :initarg :id)
;;   (prior-id :accessor prior-schema-id :initarg :prior-id)
   (upgrade-fn :accessor schema-upgrade-fn :initarg :upgrade-fn
	       :documentation "A form or functionname that is to be called
                               when upgrading from the prior version to the current")
   (upgraded :accessor schema-upgraded-p :initarg :upgraded-p)
   (version :accessor schema-version :initarg :version :initform 1
	    :documentation "Keep track of changes to schemas classes without having
                            a recursive schema problem so we can run an upgrade
                            over the schema DB when necessary")))

(defclass db-persistent-schema (persistent-schema db-schema)
  ())

(defclass db-standard-schema (standard-schema db-schema)
  ())

(defun make-db-schema (cid class-schema)
  (let ((db-schema (copy-schema 'db-persistent-schema class-schema)))
    (setf (schema-id db-schema) cid)
    db-schema))

(defun copy-schema (type schema)
  (assert (subtypep type 'persistent-schema))
  (let ((new 
	 (make-instance type
			:name (schema-classname schema)
			:prior (schema-prior schema)
			:pslots (copy-list (schema-persistent-slots schema))
			:islots (copy-list (schema-indexed-slots schema))
			:sslots (copy-list (schema-set-slots schema))
			:aslots (copy-list (schema-assoc-slots schema)))))
    (when (subtypep (type-of schema) 'db-schema)
      (setf (schema-id new) (schema-id schema))
      (setf (schema-upgrade-fn new) (schema-upgrade-fn schema))
      (setf (schema-upgraded-p new) (schema-upgraded-p schema))
      (setf (schema-version new) (schema-version schema)))
    new))

		 


;;
;; Schema upgrades
;;

(defmethod upgrade-instance-from-schemas (oid old-schema new-schema)
  ;; deserialize oid with proxy class 
  ;; use change class w/ hook into update-instance-for-redefined-class
  )

;;
;; Schema matching - has the schema changed?
;;

(defmethod match-schemas ((sch1 db-persistent-schema) sch2)
  "Are the two schemas equivalent?"
  (and (eq (class-of sch1) (class-of sch2))
       (equal (schema-classname sch1) (schema-classname sch2))
       (equal (sorted-slots 'pslots sch1)
	      (sorted-slots 'pslots sch2))
       (equal (sorted-slots 'islots sch1)
	      (sorted-slots 'islots sch2))
       (equal (sorted-slots 'sslots sch1)
	      (sorted-slots 'sslots sch2))
       (equal (sorted-slots 'aslots sch1)
	      (sorted-slots 'aslots sch2))))

(defun sorted-slots (slotname schema)
  (let ((list (slot-value schema slotname)))
    (sort (mapcar #'car list) #'string<)))

;;
;; Schema diffs
;;

;; added slots
;; removed slots
;;   by type

