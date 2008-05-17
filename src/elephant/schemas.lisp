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
   (successor :accessor schema-successor :initarg :successor :initform nil)
   (predecessor :accessor schema-predecessor :initarg :predecessor :initform nil)
   (slot-recs :accessor schema-slot-recs :initarg :slot-recs :initform nil))
  (:documentation "Keep a doubly linked list of schemas in the db"))

(defmethod print-object ((schema schema) stream)
  (format stream "#<CLASS-SCHEMA ~A>" (schema-classname schema)))

(defstruct slot-rec type name args)

(defun slot-rec-eq (rec1 rec2)
  (and (eq (slot-rec-name rec1) (slot-rec-name rec2))
       (eq (slot-rec-type rec1) (slot-rec-type rec2))))

(defun get-slot-recs-by-type (type schema)
  (remove-if-not (lambda (rec)
		   (eq (slot-rec-type rec) type))
		 (schema-slot-recs schema)))

;; =========================================
;;  Compute a schema from a slot definition
;; =========================================

(defun compute-schema (class-obj)
  "Compute a schema representation from an instance of persistent-metaclass"
  (make-instance 'schema
		 :name (class-name class-obj)
		 :slot-recs (compute-slot-recs class-obj)))

(defun compute-transient-schema (class-obj)
  (make-instance 'schema
		 :name (class-name class-obj)
		 :slot-recs (append (compute-slot-recs class-obj)
				    (compute-transient-slot-recs class-obj))))

(defparameter *slot-def-type-tags*
  '((:persistent   persistent-effective-slot-definition)
    (:indexed      indexed-effective-slot-definition)
    (:derived      derived-index-effective-slot-definition)
    (:cached       cached-effective-slot-definition)
    (:set-valued   set-valued-effective-slot-definition)
    (:association  association-effective-slot-definition)))

(defun compute-slot-recs (class-obj &optional (slot-tag-map *slot-def-type-tags*))
  "For each slot, compute a serializable record of the important info 
   in that slot"
  (mapcan (lambda (tagrec)
	    (destructuring-bind (type slot-def-type) tagrec
	      (compute-slot-recs-by-type type slot-def-type class-obj)))
	  slot-tag-map))

(defun compute-transient-slot-recs (class-obj)
  (compute-slot-recs class-obj '((:transient transient-effective-slot-definition))))

(defmethod compute-slot-recs-by-type (type slot-def-type class-obj)
  "Default slot computation.  Capture the name and type tag for the definition"
  (mapcar (lambda (slotname)
	    (make-slot-rec :type type :name slotname :args nil))
	  (find-slot-def-names-by-type class-obj slot-def-type nil)))

(defmethod compute-slot-recs-by-type ((type (eql :indexed)) slot-def-type class-obj)
  "Special handling for hierarchical indexing, capture the base class name of the index"
  (mapcar (lambda (slot-def)
	    (make-slot-rec :type type :name (slot-definition-name slot-def) 
			   :args `(:base ,(indexed-slot-base slot-def))))
	  (find-slot-defs-by-type class-obj slot-def-type nil)))


;;
;; For schemas stored in a database
;;

(defclass db-schema (schema)
  ((id :accessor schema-id :initarg :id)
   (upgrade-fn :accessor schema-upgrade-fn :initarg :upgrade-fn :initform nil
	       :documentation "A form or functionname that is to be called
                               when upgrading from the prior version to the current")
   (upgraded :accessor schema-upgraded-p :initarg :upgraded-p :initform nil)
   (schema-version :accessor schema-version :initarg :version :initform 1
		   :documentation "Keep track of changes to schemas classes without having
                            a recursive schema problem so we can run an upgrade
                            over the schema DB when necessary")))

(defmethod print-object ((schema db-schema) stream)
  (format stream "#<DB-SCHEMA ~A ~A (s: ~A p: ~A)>" 
	  (schema-id schema) (schema-classname schema)
	  (schema-successor schema) (schema-predecessor schema)))

(defun make-db-schema (cid class-schema)
  (let ((db-schema (logical-copy-schema 'db-schema class-schema)))
    (setf (schema-id db-schema) cid)
    db-schema))

(defun logical-copy-schema (type schema)
  (assert (subtypep type 'schema))
  (make-instance type
		 :name (schema-classname schema)
		 :slot-recs (copy-list (schema-slot-recs schema))))

(defun copy-schema (type schema)    
  (assert (subtypep type 'schema))
  (let ((new 
	 (make-instance type
			:name (schema-classname schema)
			:successor (schema-successor schema)
			:predecessor (schema-predecessor schema)
			:slot-recs (copy-list (schema-slot-recs schema)))))
    (when (subtypep (type-of schema) 'db-schema)
      (setf (schema-id new) (schema-id schema))
      (setf (schema-upgrade-fn new) (schema-upgrade-fn schema))
      (setf (schema-upgraded-p new) (schema-upgraded-p schema))
      (setf (schema-version new) (schema-version schema)))
    new))

;;
;; Schema matching - has the schema changed?
;;

(defmethod match-schemas ((sch1 schema) (sch2 schema))
  "Are the two schemas functionally equivalent?"
  (and (equal (schema-classname sch1) (schema-classname sch2))
       (equal (merge 'list 
		     (sorted-slots :persistent sch1)
		     (sorted-slots :cached sch1)
		     #'symbol<)
	      (merge 'list
		     (sorted-slots :persistent sch2)
		     (sorted-slots :cached sch2)
		     #'symbol<))
       (equal (sorted-slots :indexed sch1)
	      (sorted-slots :indexed sch2))
       (equal (sorted-slots :derived sch1)
	      (sorted-slots :derived sch2))
       (equal (sorted-slots :set-valued sch1)
	      (sorted-slots :set-valued sch2))
       (equal (sorted-slots :association sch1)
	      (sorted-slots :association sch2))))

(defun symbol< (sym1 sym2) 
  (string< (symbol-name sym1) (symbol-name sym2)))

(defun sorted-slots (type schema)
  (let ((list (mapcar #'slot-rec-name (get-slot-recs-by-type type schema))))
    (sort list #'symbol<)))

;;
;; Schema diffs
;;

(defmethod schema-diff (new old)
  "Returns a list of lists :add, :rem, :change with one or two slot-recs"
  (let ((new-recs (schema-slot-recs new))
	(old-recs (schema-slot-recs old)))
    (labels ((find-old-rec (new-rec) 
	       (find (slot-rec-name new-rec) old-recs :key #'slot-rec-name))
	     (diff-add-change () 
	       (loop for new-rec in new-recs collect
		    (aif (find-old-rec new-rec)
			 (unless (slot-rec-eq new-rec it)
			   `(:change ,it ,new-rec))
			 `(:add ,new-rec))))
	     (diff-rem () 
	       (mapcar #'(lambda (rec) `(:rem ,rec))
		       (set-difference old-recs new-recs :key #'slot-rec-name))))
      (remove-if #'null (append (diff-add-change) (diff-rem))))))


(defun diff-type (diff-entry) (car diff-entry))
(defun diff-recs (diff-entry) (cdr diff-entry))

;;
;; Construct classes from schemas
;;

(defmethod default-class-constructor ((schema db-schema) &rest args
				      &key superclasses &allow-other-keys)
  "Given a schema, construct a class overriding information as necessary
   :subclasses - a list of subclasses for this schema"
  (let ((classname (schema-classname schema)))
    (ensure-class-using-class (find-class classname :errorp nil) classname
		:direct-superclasses superclasses
		:direct-slots (slot-defs-from-schema schema args)
		:metaclass 'persistent-metaclass)))

(defun slot-defs-from-schema (schema args)
  "Need to handle default-initargs and other options to defclass"
  (destructuring-bind (&key (accessor-template #'default-template)
			    accessor-override &allow-other-keys) args
    (loop for rec in (schema-slot-recs schema) do
	  (list :name (slot-rec-name rec)
		:readers (compute-reader (schema-classname schema) (slot-rec-name rec)
					 accessor-override accessor-template)
		:writers (compute-writer (slot-rec-name rec)
					 (schema-classname schema) 
					 accessor-override accessor-template )))))
		
(defun compute-reader (classname name override-fn template-fn)
  (or (and override-fn
	   (funcall override-fn classname name :reader))
      (funcall template-fn classname name :reader)))

(defun compute-writer (classname name override-fn template-fn)	
  (or (and override-fn
	   (funcall override-fn classname name :writer))
      (funcall template-fn classname name :writer)))

(defun default-template (classname name type)
  (ecase type
    (:reader (list (intern (format nil "~A-~A" classname name))))
    (:writer `((setf ,(intern (format nil "~A-~A" classname name)))))))
	   

;;
;; Debugging tools
;;

(defun dump-schema (schema &optional (stream t))
  (assert (subtypep (type-of schema) 'schema))
  (format stream "Schema for ~A (~A)~%" (schema-classname schema) schema)
  (when (subtypep (type-of schema) 'db-schema)
    (format stream "id: ~A~%" (schema-id schema))
    (awhen (schema-upgrade-fn schema)
      (format stream "upgrade-fn:~%~A~%" it)))
  (format stream "Successor: ~A   Predecessor: ~A~%" 
	  (schema-successor schema) (schema-predecessor schema))
  (dump-slots schema))

(defun dump-slots (schema)
  (loop for rec in (schema-slot-recs schema) do
       (format t "  ~A ~A ~A~%" (slot-rec-name rec) (slot-rec-type rec) (slot-rec-args rec))))
       