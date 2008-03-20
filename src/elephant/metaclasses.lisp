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

(defclass persistent ()
  ((oid :accessor oid :initarg :from-oid
	 :documentation "All persistent objects have an oid")
   (spec :type (or list string) :accessor db-spec :initarg :db-spec
	 :documentation "Persistent objects use a spec pointer to identify which store
                         they are connected to"))
  (:documentation "Abstract superclass for all persistent classes (common
    to both user-defined classes and Elephant-defined objects such as collections.)"))

(defmethod print-object ((obj persistent) stream)
  "This is useful for debugging and being clear about what is persistent and what is not"
  (format stream "#<~A oid:~A>" (type-of obj) (when (slot-boundp obj 'oid) (oid obj))))

(defclass persistent-metaclass (standard-class)
  ((%class-schema :accessor %class-schema :initarg :schemas :initform nil
		   :documentation "The code master schema")
   (%store-schemas :accessor %store-schemas :initarg :store-schemas :initform nil))
  (:documentation 
   "Metaclass for persistent classes.  Use this metaclass to
    define persistent classes.  All slots are persistent by
    default; use the :transient flag otherwise.  Slots can also
    be indexed for by-value retrieval."))

(defmethod has-class-schema-p ((class persistent-metaclass))
  (and (%class-schema class)
       (eq (class-name (class-of (%class-schema class)))
	   'persistent-schema)))

(defmethod has-class-controller-schema-p (sc (class persistent-metaclass))
  (and (get-class-controller-schema sc class) t))

(defmethod get-class-controller-schema (sc (class persistent-metaclass))
  (awhen (assoc (controller-spec sc) (%store-schemas class))
    (cdr it)))

(defmethod add-class-controller-schema (sc (class persistent-metaclass) schema)
  (remove-class-controller-schema sc class)
  ;; NOTE: Needs to be lock protected
  (setf (%store-schemas class)
	(acons (controller-spec sc) schema (%store-schemas class))))

(defmethod remove-class-controller-schema (sc (class persistent-metaclass))
  ;; NOTE: Needs to be lock protected
  (setf (%store-schemas class)
	(remove (controller-spec sc) (%store-schemas class) :key #'car :test #'equalp)))

;;
;; Top level defclass form - hide metaclass option
;;

(defmacro defpclass (cname parents slot-defs &rest class-opts)
  "Shorthand for defining persistent objects.  Wraps the main
   class definition with persistent-metaclass"
  `(defclass ,cname ,parents
     ,slot-defs
     ,@(add-persistent-metaclass-argument class-opts)))

(defun add-persistent-metaclass-argument (class-opts)
  (when (assoc :metaclass class-opts)
    (error "User metaclass specification not allowed in defpclass"))
  (append class-opts (list (list :metaclass 'persistent-metaclass))))
	  
;;
;; Persistent slot maintenance
;;

(defclass persistent-slot-definition (standard-slot-definition)
  ())

(defclass persistent-direct-slot-definition (standard-direct-slot-definition persistent-slot-definition)
  ())

(defclass persistent-effective-slot-definition (standard-effective-slot-definition persistent-slot-definition)
  ())

(defgeneric persistent-p (class)
  (:method ((class t)) nil)
  (:method ((class persistent-metaclass)) t)
  (:method ((class persistent-slot-definition)) t))

(defun persistent-slot-defs (class)
  (find-slot-defs-by-type class 'persistent-effective-slot-definition nil))

(defun persistent-slot-names (class)
  (find-slot-def-names-by-type class 'persistent-effective-slot-definition nil))

(defun all-persistent-slot-names (class)
  (append (find-slot-def-names-by-type class 'persistent-effective-slot-definition t)
	  (find-slot-def-names-by-type class 'cached-effective-slot-definition t)))

;;
;; Cached slots (a placeholder for future development)
;;

(defclass cached-slot-definition (standard-slot-definition)
  ((cache :accessor cached-slot-p :initarg :cached)))

(defclass cached-direct-slot-definition (standard-direct-slot-definition cached-slot-definition)
  ())

(defclass cached-effective-slot-definition (standard-effective-slot-definition cached-slot-definition)
  ())

(defun cached-slot-defs (class)
  (find-slot-defs-by-type class 'cached-effective-slot-definition nil))

(defun cached-slot-names (class)
  (find-slot-def-names-by-type class 'cached-effective-slot-definition nil))

;;
;; Standard/transient slots
;;

(defclass transient-slot-definition (standard-slot-definition)
  ((transient :initform t :initarg :transient :allocation :class)))

(defclass transient-direct-slot-definition (standard-direct-slot-definition transient-slot-definition)
  ())

(defclass transient-effective-slot-definition (standard-effective-slot-definition transient-slot-definition)
  ())

(defgeneric transient-p (slot)
  (:method ((slot standard-slot-definition)) t)
  (:method ((slot transient-slot-definition)) t)
  (:method ((slot cached-slot-definition)) nil)
  (:method ((slot persistent-slot-definition)) nil))

(defun ensure-transient-chain (slot-definitions initargs)
  (declare (ignore initargs))
  (loop for slot-definition in slot-definitions
     always (transient-p slot-definition)))

(defun transient-slot-defs (class)
  (let ((slot-definitions (class-slots class)))
    (loop for slot-def in slot-definitions
       when (transient-p slot-def)
       collect slot-def)))

(defun transient-slot-names (class)
  (mapcar #'slot-definition-name (transient-slot-defs class)))

;;
;; Indexed slots
;;

(defclass indexed-slot-definition (persistent-slot-definition)
  ((indexed :accessor indexed-p :initarg :indexed :initarg :index :initform nil :allocation :instance)))

(defclass indexed-direct-slot-definition (persistent-direct-slot-definition indexed-slot-definition)
  ())

(defclass indexed-effective-slot-definition (persistent-effective-slot-definition indexed-slot-definition)
  ((indices :accessor indexed-slot-indices :initform nil :allocation :instance
	    :documentation "Alist of actual indices by store")
   (base-class :accessor indexed-slot-base :initarg :base-class :allocation :instance
	       :documentation "The base class to use as an index")))

(defmethod get-slot-def-index ((def indexed-effective-slot-definition) sc)
  (awhen (assoc sc (indexed-slot-indices def))
    (cdr it)))

(defmethod add-slot-def-index (idx (def indexed-effective-slot-definition) sc)
  (setf (indexed-slot-indices def)
	(acons sc idx (indexed-slot-indices def))))

(defun indexed-slot-defs (class)
  (find-slot-defs-by-type class 'indexed-effective-slot-definition nil))

(defun indexed-slot-names (class)
  (find-slot-def-names-by-type class 'indexed-effective-slot-definition nil))

;;
;; Set-valued slots
;;

(defclass set-valued-slot-definition (persistent-slot-definition) 
  ((set-valued-p :accessor set-valued-p :initarg :set-valued :allocation :instance)))

(defclass set-valued-direct-slot-definition (persistent-direct-slot-definition set-valued-slot-definition) 
  ())

(defclass set-valued-effective-slot-definition (persistent-effective-slot-definition set-valued-slot-definition) 
  ())

(defun set-valued-slot-defs (class)
  (find-slot-defs-by-type class 'set-valued-effective-slot-definition nil))

(defun set-valued-slot-names (class)
  (find-slot-def-names-by-type class 'set-valued-effective-slot-definition nil))

;;
;; Association slots
;;

(defclass association-slot-definition (persistent-slot-definition)
  ((assoc :accessor association :initarg :associate :allocation :instance)
   (m2m :accessor many-to-many-p :initarg :many-to-many :initform nil :allocation :instance)))

(defclass association-direct-slot-definition (persistent-direct-slot-definition association-slot-definition) 
  ())

(defclass association-effective-slot-definition (persistent-effective-slot-definition association-slot-definition) 
  ((type :accessor association-type :initarg :association-type)
   (base-class :accessor association-slot-base :initarg :base-class :allocation :instance
	       :documentation "The base class to use as an index")
   (indices :accessor association-slot-indices :initform nil 
	    :documentation "Alist of actual indices by store")
   (classname :accessor foreign-classname :initarg :foreign-classname)
   (slotname :accessor foreign-slotname :initarg :foreign-slotname)
   (class :accessor foreign-class :initarg :foreign-class :initform nil
	  :documentation "Direct pointer to foreign class; late binding")))

(defmethod initialize-instance :after ((slot-def association-effective-slot-definition) &rest args)
  (declare (ignore args))
  (let ((assoc (association slot-def)))
    (cond ((symbolp (association slot-def))
	   (when (many-to-many-p slot-def)
	     (error "Cannot specify ~A in a many-to-many association, must be of form (class slotname)"))
	   (setf (association-type slot-def) :end
		 (foreign-classname slot-def) assoc
		 (foreign-slotname slot-def) nil))
	  (t 
	   (destructuring-bind (classname slotname) (association slot-def)
	     (setf (foreign-classname slot-def) classname)
	     (setf (foreign-slotname slot-def) slotname)
	     (if (many-to-many-p slot-def)
		 (setf (association-type slot-def) :m2m)
		 (setf (association-type slot-def) :m21)))))))

(defun association-end-p (slot-def)
  (eq (association-type slot-def) :end))

(defun association-slot-defs (class)
  (find-slot-defs-by-type class 'association-effective-slot-definition nil))

(defun association-slot-names (class)
  (find-slot-def-names-by-type class 'association-effective-slot-definition nil))


(defun get-association-slot-index (slot-def sc)
  (awhen (assoc sc (association-slot-indices slot-def))
    (cdr it)))

(defun add-association-slot-index (idx slot-def sc)
  (setf (association-slot-indices slot-def)
	(acons sc idx (association-slot-indices slot-def))))

(defun remove-association-slot-index (slot-def sc)
  (setf (association-slot-indices slot-def)
	(delete sc (association-slot-indices slot-def) :key #'car)))

;;
;; Class MOP support:
;;


(defmethod validate-superclass ((class persistent-metaclass) (super standard-class))
  "Persistent classes may inherit from ordinary classes."
  t)

(defmethod validate-superclass ((class standard-class) (super persistent-metaclass))
  "Ordinary classes may NOT inherit from persistent classes."
  nil)

(defgeneric database-allocation-p (class)
  (:method ((class t)) nil)
  (:method ((class persistent-metaclass)) t)
  (:method ((class persistent-slot-definition)) t))

;;
;; Slot MOP support: compute slot definition types
;;

(defmethod slot-definition-allocation ((slot-definition persistent-slot-definition))
  :database)

(defmacro bind-standard-init-arguments ((initargs) &body body)
  `(let ((allocation-key (getf ,initargs :allocation))
	 (has-initarg-p (getf ,initargs :initargs))
	 (transient-p (getf ,initargs :transient))
	 (indexed-p (or (getf ,initargs :indexed)
			(getf ,initargs :index)))
	 (cached-p (getf ,initargs :cached))
	 (set-valued-p (getf ,initargs :set-valued))
	 (associate-p (getf ,initargs :associate)))
     (declare (ignorable allocation-key has-initarg-p))
     (when (consp transient-p) (setq transient-p (car transient-p)))
     (when (consp indexed-p) (setq indexed-p (car indexed-p)))
     (when (consp cached-p) (setq cached-p (car cached-p)))
     (when (consp set-valued-p) (setq set-valued-p (car set-valued-p)))
     (when (consp associate-p) (setq associate-p (car associate-p)))
     ,@body))

(defmethod direct-slot-definition-class ((class persistent-metaclass) &rest initargs)
  "Checks for the transient tag (and the allocation type)
   and chooses persistent or transient slot definitions."
  (bind-standard-init-arguments (initargs)
    (cond ((and (eq allocation-key :class) (not transient-p))
	   (error "Persistent class slots are not supported, try :transient t."))
	  ((> (count-true indexed-p transient-p set-valued-p associate-p) 1)
	   (error "Cannot declare a slot to be more than one of transient, indexed, 
                   set-valued and associated"))
	  ((and set-valued-p has-initarg-p)
	   (error "Cannot specify initargs for set-valued slots"))
	  ((and associate-p has-initarg-p (not (eq associate-p t)))
	   (error "Cannot specify initargs for association slots"))
	  (indexed-p 
	   (find-class 'indexed-direct-slot-definition))
	  (set-valued-p
	   (find-class 'set-valued-direct-slot-definition))
	  (cached-p
	   (find-class 'cached-direct-slot-definition))
	  (associate-p
	   (find-class 'association-direct-slot-definition))
  	  (transient-p
	   (find-class 'transient-direct-slot-definition))
	  (t
	   (find-class 'persistent-direct-slot-definition)))))

(defmethod effective-slot-definition-class ((class persistent-metaclass) &rest initargs)
  "Chooses the persistent or transient effective slot
definition class depending on the keyword."
  (bind-standard-init-arguments (initargs)
    (cond (indexed-p 
	   (find-class 'indexed-effective-slot-definition))
	  (set-valued-p
	   (find-class 'set-valued-effective-slot-definition))
	  (cached-p
	   (find-class 'cached-effective-slot-definition))
	  (associate-p
	   (find-class 'association-effective-slot-definition))
	  (transient-p
	   (find-class 'transient-effective-slot-definition))
	  (t
	   (find-class 'persistent-effective-slot-definition)))))

(defmethod compute-effective-slot-definition-initargs ((class persistent-metaclass) #+lispworks slot-name slot-definitions)
  #+lispworks (declare (ignore slot-name))
  (let ((initargs (call-next-method))
	(parent-direct-slot (first slot-definitions)))
    (cond ((ensure-transient-chain slot-definitions initargs)
	   (setf initargs (append initargs '(:transient t))))
	  ((not (eq (type-of parent-direct-slot) 'cached-direct-slot-definition))
	   (setf (getf initargs :allocation) :database)))
    (when (eq (type-of parent-direct-slot) 'set-valued-direct-slot-definition)
      (setf (getf initargs :set-valued) t))
    (when (eq (type-of parent-direct-slot) 'cached-direct-slot-definition)
      (setf (getf initargs :cached) t))
    (when (eq (type-of parent-direct-slot) 'association-direct-slot-definition)
      (setf (getf initargs :associate) (association parent-direct-slot))
      (setf (getf initargs :many-to-many) (many-to-many-p parent-direct-slot))
      (setf (getf initargs :base-class)
	    (find-class-for-direct-slot class (slot-definition-name (first slot-definitions)))))
    (when (eq (type-of parent-direct-slot) 'indexed-direct-slot-definition)
      (setf (getf initargs :indexed) t)
      (setf (getf initargs :base-class)
	    (find-class-for-direct-slot class (slot-definition-name (first slot-definitions)))))
    initargs))

(defun find-class-for-direct-slot (class name)
  (let ((list (compute-class-precedence-list class)))
    (labels ((rec (super)
	       (if (null super)
		   nil
		   (aif (find-direct-slot-def-by-name super name)
			(class-name super)
			(rec (pop list))))))
      (rec class))))

;;
;; General tools for accessing and manipulating slot definitions
;;

(defun find-direct-slot-def-by-name (class slot-name)
  (loop for slot-def in (class-direct-slots class)
	when (eq (slot-definition-name slot-def) slot-name)
	do (return slot-def)))

(defun find-slot-def-by-name (class slot-name)
  (loop for slot-def in (class-slots class)
	when (eq (slot-definition-name slot-def) slot-name)
	do (return slot-def)))

(defmethod find-slot-defs-by-type ((class persistent-metaclass) type &optional (by-subtype t))
  (let ((slot-defs (class-slots class)))
    (loop for slot-def in slot-defs
	 when (if by-subtype
		  (subtypep (type-of slot-def) type)
		  (eq (type-of slot-def) type))
	 collect slot-def)))

(defmethod find-slot-def-names-by-type ((class persistent-metaclass) type &optional (by-subtype t))
  (mapcar #'slot-definition-name 
	  (find-slot-defs-by-type class type by-subtype)))

(defun count-true (&rest args)
  (count t args :key #'(lambda (x) (not (null x)))))

;;
;; Special support for different MOP implementations
;;
;; To be superceded by Closer-to-MOP?  Doesn't seem to solve all these issues on
;; last check (ISE)
;;

#+allegro
(defmethod excl::valid-slot-allocation-list ((class persistent-metaclass))
  '(:instance :class :database))

#+lispworks
(defmethod (setf slot-definition-allocation) (allocation (slot-def persistent-slot-definition))
  (unless (eq allocation :database)
    (error "Invalid allocation type ~A for slot-definition-allocation" allocation))
  allocation)

#+openmcl
(defmethod compute-effective-slot-definition ((class persistent-metaclass) slot-name direct-slot-definitions)
  (declare (ignore slot-name))
  (apply #'make-effective-slot-definition class
	 (compute-effective-slot-definition-initargs 
	 class direct-slot-definitions)))

#+openmcl
(defmethod compute-effective-slot-definition-initargs ((class slots-class)
						       direct-slots)
  (let* ((name (loop for s in direct-slots
		  when s
		  do (return (slot-definition-name s))))
	 (initer (dolist (s direct-slots)
                   (when (%slot-definition-initfunction s)
                     (return s))))
         (documentor (dolist (s direct-slots)
                       (when (%slot-definition-documentation s)
                         (return s))))
         (first (car direct-slots))
         (initargs (let* ((initargs nil))
                     (dolist (dslot direct-slots initargs)
                       (dolist (dslot-arg (%slot-definition-initargs  dslot))
                         (pushnew dslot-arg initargs :test #'eq))))))
    (list
     :name name
     :allocation (%slot-definition-allocation first)
     :documentation (when documentor (nth-value
                                      1
                                      (%slot-definition-documentation
                                       documentor)))
     :class (%slot-definition-class first)
     :initargs initargs
     :initfunction (if initer (%slot-definition-initfunction initer))
     :initform (if initer (%slot-definition-initform initer))
     :type (or (%slot-definition-type first) t))))





