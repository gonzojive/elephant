;;; -*- Mode: Lisp; Syntax: ANSI-Common-Lisp; Base: 10 -*-
;;;
;;; classes.lisp -- persistent objects via metaobjects
;;; 
;;; Initial version 8/26/2004 by Andrew Blumberg
;;; <ablumberg@common-lisp.net>
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

(defvar *debug-si* nil)

(declaim #-elephant-without-optimize (optimize (speed 3)))

;; ================================================
;; SIMPLE PERSISTENT OBJECTS
;; ================================================

(defmethod initialize-instance :before  ((instance persistent)
					 &rest initargs
					 &key from-oid
					 (sc *store-controller*))
  "Each persistent instance has an oid and a home controller spec"
  (declare (ignore initargs))
  (initial-persistent-setup instance :from-oid from-oid :sc sc))

(defun initial-persistent-setup (instance &key from-oid sc)
  (check-valid-store-controller sc)
  (if from-oid
      (setf (oid instance) from-oid)
      (register-new-instance instance (class-of instance) sc))
  (setf (db-spec instance) (controller-spec sc))
  (cache-instance sc instance))

(defun register-new-instance (instance class sc)
  (setf (oid instance) (next-oid sc))
  (register-instance sc class instance))

(defun check-valid-store-controller (sc)
  (unless (subtypep (type-of sc) 'store-controller)
    (error "This function requires a valid store controller")))

(defclass persistent-object (persistent) ()
  (:metaclass persistent-metaclass)
  (:documentation 
   "Superclass for all user-defined persistent classes.  This is
    automatically inherited if you use the persistent-metaclass
    metaclass.  This allows specialization of functions for user
    objects that would not be appropriate for Elephant objects
    such as persistent collections"))

;; ================================================
;; METACLASS INITIALIZATION 
;; ================================================

(defmethod shared-initialize :around ((class persistent-metaclass) slot-names &rest args &key direct-superclasses index)
  "Ensures we inherit from persistent-object prior to initializing."
  (declare (ignorable index))
  (let* ((persistent-object (find-class 'persistent-object))
	 (has-persistent-object (superclass-member-p direct-superclasses persistent-object)))
    (if (not (or (eq class persistent-object)
		 has-persistent-object))
	(apply #'call-next-method class slot-names
	       :direct-superclasses (append direct-superclasses (list persistent-object)) args)
	(call-next-method))))

(defun superclass-member-p (superclasses class)
  "Searches superclass list for class"
  (some #'(lambda (superclass)
	    (eq (class-of superclass) class))
	superclasses))

(defmethod finalize-inheritance :after ((instance persistent-metaclass))
  "Constructs the metaclass schema when the class hierarchy is valid"
  (let* ((old-schema (%class-schema instance))
	 (new-schema (compute-schema instance)))
    (setf (schema-predecessor new-schema) old-schema)
    (setf (%class-schema instance) new-schema)
    (when (and old-schema (not (match-schemas new-schema old-schema)))
      (synchronize-stores-for-class instance))))



(defmethod reinitialize-instance :around ((instance persistent-metaclass) &rest initargs 
					  &key direct-slots &allow-other-keys)
  (declare (ignore direct-slots))
  ;; Warnings at class def time:
  ;; - set-valued/assoc (warn!)
  ;; - persistent/indexed/cached (warn?)
  (call-next-method))



;; ================================================
;; PERSISTENT OBJECT MAINTENANCE
;; ================================================

;; Some syntactic sugar

(defmacro bind-slot-defs (class slots bindings &body body)
  (with-gensyms (classref slotrefs)
    `(let* ((,classref ,class)
	    (,slotrefs ,slots)
	    ,@(compute-bindings classref slotrefs bindings))
     ,@body)))

(eval-when (:compile-toplevel :load-toplevel)
  (defun compute-bindings (class slots bindings)
    (loop for (name accessor) in bindings collect
	 `(,name (get-init-slotnames ,class #',accessor ,slots)))))

;;
;; CLASS INSTANCE INITIALIZATION
;;

(defmethod initialize-instance :around ((instance persistent-object) &rest initargs 
					&key (sc *store-controller*) &allow-other-keys)
  "Ensure instance creation is inside a transaction, huge (5x) performance impact per object"
  (ensure-transaction (:store-controller sc)
    (call-next-method)))
    

(defmethod shared-initialize :around ((instance persistent-object) slot-names &rest initargs &key from-oid &allow-other-keys)
  "Initializes the persistent slots via initargs or forms.
This seems to be necessary because it is typical for
implementations to optimize setting the slots via initforms
and initargs in such a way that slot-value-using-class et al
aren't used.  We also handle writing any indices after the 
class is fully initialized.  Calls the next method for the transient 
slots."
  (bind-slot-defs (class-of instance) slot-names
    ((transient-slots transient-slot-names)
     (cached-slots transient-slot-names)
     (indexed-slots indexed-slot-names)
     (persistent-slots persistent-slot-names))
    (let* ((class (class-of instance))
	   (persistent-initializable-slots 
	   (union persistent-slots indexed-slots))
	   (set-slots (union (get-init-slotnames class #'association-slot-names slot-names)
			     (get-init-slotnames class #'set-valued-slot-names slot-names))))
      (cond (from-oid ;; If re-starting, make sure we read the cached values
	     (initialize-cached-slots instance cached-slots))
	    (t        ;; If new instance, initialize all slots
	     (setq transient-slots (union transient-slots cached-slots))
	     (initialize-persistent-slots class instance persistent-initializable-slots initargs from-oid)))
      ;; Always initialize transients
      (apply #'call-next-method instance transient-slots initargs)
      ;; Initialize set slots after transient initialization
      (unless from-oid
	(initialize-set-slots class instance set-slots)))))

(defun initialize-persistent-slots (class instance persistent-slot-inits initargs object-exists)
  (dolist (slotname persistent-slot-inits)
    (let ((slot-def (find-slot-def-by-name class slotname)))
      (unless (or (initialize-from-initarg class instance slot-def 
					   (slot-definition-initargs slot-def) initargs)
		  object-exists
		  (slot-boundp-using-class class instance slot-def))
	(awhen (slot-definition-initfunction slot-def)
	  (setf (slot-value-using-class class instance slot-def)
		(funcall it))))))))

(defun initialize-set-slots (class instance set-slots)
  (dolist (slotname set-slots)
    (setf (slot-value-using-class class instance
				  (find-slot-def-by-name class slotname))
	  nil)))

(defun initialize-from-initarg (class instance slot-def slot-initargs initargs)
  (loop for slot-initarg in slot-initargs
     when (member slot-initarg initargs :test #'eq)
     do
       (setf (slot-value-using-class class instance slot-def)
	     (getf initargs slot-initarg))
       (return t)
     finally (return nil)))

(defun get-init-slotnames (class accessor slot-names)
  (let ((slotnames (funcall accessor class)))
    (if (not (eq slot-names t))
	(intersection slotnames slot-names :test #'equal)
	slotnames)))

(define-condition dropping-persistent-slot-data ()
  ((operation :initarg :operation :reader persistent-slot-drop-operation)
   (class :initarg :class :reader persistent-slot-drop-class)
   (slots :initarg :slotnames :reader persistent-slot-drop-names))
  (:report (lambda (cond stream)
	     (with-slots (class slots operation) cond
	       (format stream "Dropping slot(s) ~A for class ~A in ~A."
		       slots class operation)))))

(defun warn-about-dropped-slots (op class names)
  (when (and *warn-when-dropping-persistent-slots* names)
    (cerror "Drop the slots" 
	    'dropping-persistent-slot-data
	    :operation op
	    :class class
	    :slotnames names)))

(defun drop-slots (class instance slotnames)
  (when slotnames
    (loop for slot-def in (class-slots class)
       when (member (slot-definition-name slot-def) slotnames)
       do (slot-makunbound-using-class class instance slot-def))))

;;
;; RECREATING A PERSISTENT INSTANCE
;;

(defgeneric recreate-instance (instance &rest initargs &key &allow-other-keys)
  (:method ((instance persistent-object) &rest args &key from-oid schema (sc *store-controller*))
    ;; Initialize basic instance data
    (initial-persistent-setup instance :from-oid from-oid :sc sc)
    ;; Update db instance data
    (when schema
      (let ((official-schema (lookup-schema sc (class-of instance))))
	(unless (eq (schema-id schema) (schema-id official-schema))
	  (upgrade-db-instance instance official-schema schema))))
    ;; Load cached slots, set, assoc values, etc.
    (shared-initialize instance t :from-oid from-oid)))

(defmethod recreate-instance-using-class ((class standard-class) &rest initargs &key &allow-other-keys)
  "Simply allocate store, the state of the slots will be filled by the data from the 
   database.  We do not want to call initialize-instance and re-evaluate the initforms;
   we are just fetching the object & values from the store"
  (allocate-instance class))

(defmethod recreate-instance-using-class ((class persistent-metaclass) &rest initargs &key &allow-other-keys)
  "Persistent-objects bypass initialize-instance"
    (let ((instance (allocate-instance class)))
      (apply #'recreate-instance instance initargs)
      instance))

;;
;; CLASS REDEFINITION PROTOCOL
;;

(defmethod update-instance-for-redefined-class :around ((instance persistent-object) added-slots 
							discarded-slots property-list &rest initargs)
  (declare (ignore property-list discarded-slots added-slots initargs))
  (prog1
      (call-next-method)
    (let* ((sc (get-con instance))
	   (current-schema (get-current-db-schema sc (type-of instance)))
	   (prior-schema (get-controller-schema sc (schema-predecessor current-schema))))
      (assert (and current-schema prior-schema))
      (upgrade-db-instance instance current-schema prior-schema))))

;;
;; CLASS CHANGE PROTOCOL   
;;

(defmethod change-class :before ((previous persistent) (new-class standard-class) &rest initargs)
  (declare (ignorable initargs))
  (unless (subtypep (type-of new-class) 'persistent-metaclass)
    (error "Persistent instances cannot be changed to standard classes via change-class")))

(defmethod change-class :before ((previous standard-object) (new-class persistent-metaclass) &rest initargs)
  (declare (ignorable initargs))
  (unless (subtypep (type-of previous) 'persistent)
    (error "Standard classes cannot be changed to persistent classes in change-class")))

(defmethod update-instance-for-different-class :around ((previous persistent-object) (current persistent-object) 
							&rest initargs &key)
  (change-db-instance-class previous current)
;; call update-db-instance
;; - handles dropped, added and changed
;; for slots with same name: update new indexes, remove from prior indexes
;; update associations
;; update oid-table to point to new cid
  )


;;   (let* ((old-class (class-of previous))
;; 	 (new-class (class-of current))
;; 	 (old-pslots (persistent-slots old-class))
;; 	 (new-pslots (persistent-slots new-class))
;; 	 (new-persistent-slots (set-difference new-pslots old-pslots))
;; 	 (dropped-persistent-slots (set-difference old-pslots new-pslots))
;; 	 (raw-retained-persistent-slots (intersection new-pslots old-pslots))
;; 	 (retained-unbound-slots (loop for slot-name in raw-retained-persistent-slots
;; 				       when (not (persistent-slot-boundp (get-con previous) previous slot-name))
;; 				       collect slot-name))
;;  	 (retained-persistent-slots (set-difference raw-retained-persistent-slots retained-unbound-slots)))
;;     ;; Apply default values for unbound & new slots (updates class index)
;;     (apply #'shared-initialize current (append new-persistent-slots retained-unbound-slots) initargs)
;;     (ensure-transaction (:store-controller (get-con current))
;;       ;; Copy old slot values to new slot values
;;       (loop for slot-def in (class-slots new-class)
;; 	 when (member (slot-definition-name slot-def) retained-persistent-slots)
;; 	 do (setf (slot-value-using-class new-class
;; 					  current
;; 					  slot-def)
;; 		  (slot-value-using-class old-class
;; 					  previous
;; 					  (find-slot-def-by-name old-class (slot-definition-name slot-def))))))
;;     (call-next-method)
;;     (ensure-transaction (:store-controller (get-con current))
;;       ;; Reclaim space from dropped slots
;;       (drop-slots old-class previous dropped-persistent-slots)
;;       ;; Delete this instance from its old class index, if exists
;;       (when (indexed old-class)
;; 	(remove-kv (oid previous) (find-class-index old-class))))))


;; =============================================
;; SHARED SLOT ACCESS PROTOCOL DEFINITIONS
;; =============================================

(defmethod slot-value-using-class ((class persistent-metaclass) (instance persistent-object) (slot-def persistent-slot-definition))
  "Get the slot value from the database."
  (let ((name (slot-definition-name slot-def)))
    (persistent-slot-reader (get-con instance) instance name)))

(defmethod (setf slot-value-using-class) (new-value (class persistent-metaclass) (instance persistent-object) (slot-def persistent-slot-definition))
  "Set the slot value in the database."
  (let ((name (slot-definition-name slot-def)))
    (persistent-slot-writer (get-con instance) new-value instance name)))

(defmethod slot-boundp-using-class ((class persistent-metaclass) (instance persistent-object) (slot-def persistent-slot-definition))
  "Checks if the slot exists in the database."
  (when instance
    (let ((name (slot-definition-name slot-def)))
      (persistent-slot-boundp (get-con instance) instance name))))

(defmethod slot-boundp-using-class ((class persistent-metaclass) (instance persistent-object) (slot-name symbol))
  "Checks if the slot exists in the database."
  (loop for slot in (class-slots class)
     for matches-p = (eq (slot-definition-name slot) slot-name)
     until matches-p
     finally (return (if (and matches-p
			      (subtypep (type-of slot) 'persistent-slot-definition))
			 (persistent-slot-boundp (get-con instance) instance slot-name)
			 (call-next-method)))))

(defmethod slot-makunbound-using-class ((class persistent-metaclass) (instance persistent-object) (slot-def persistent-slot-definition))
  "Removes the slot value from the database."
  (persistent-slot-makunbound (get-con instance) instance (slot-definition-name slot-def)))

;; ===================================
;; Multi-store error checking
;; ===================================

(defun valid-persistent-reference-p (object sc)
  "Ensures that object can be written as a reference into store sc"
  (or (not (slot-boundp object 'spec))
      (eq (db-spec object) (controller-spec sc))))

(define-condition cross-reference-error (error)
  ((object :accessor cross-reference-error-object :initarg :object)
   (home-controller :accessor cross-reference-error-home-controller :initarg :home-ctrl)
   (foreign-controller :accessor cross-reference-error-foreign-controller :initarg :foreign-ctrl))
  (:documentation "An error condition raised when an object is being written into a data store other
                   than its home store")
  (:report (lambda (condition stream)
	     (format stream "Attempted to write object ~A with home store ~A into store ~A"
		     (cross-reference-error-object condition)
		     (cross-reference-error-home-controller condition)
		     (cross-reference-error-foreign-controller condition)))))

(defun signal-cross-reference-error (object sc)
  (cerror "Proceed to write incorrect reference"
	  'cross-reference-error
	  :object object
	  :home-ctrl (get-con object)
	  :foreign-ctrl sc))


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
     finally (return (if (or (typep slot 'persistent-slot-definition)
			     (typep slot 'cached-slot-definition))
			 (slot-makunbound-using-class class instance slot)
			 (call-next-method)))))

#+allegro
(defmethod reinitialize-instance :after ((class persistent-metaclass) &rest initargs)
;;  (ensure-finalized class)
;;  (loop with persistent-slots = (union (persistent-slot-names class)
;;				       (cached-slot-names class)
;;				       (indexed-slot-names class))
;;     for slot-def in (class-direct-slots instance)
;;     when (member (slot-definition-name slot-def) persistent-slots)
;;     do (initialize-accessors slot-def class))
;;  (make-instances-obsolete class))
  )

#+allegro
(defmethod initialize-accessors ((slot-definition persistent-slot-definition) class)
  (let ((readers (slot-definition-readers slot-definition))
	(writers (slot-definition-writers slot-definition))
	(class-name (class-name class)))
    (loop for reader in readers
	  do (make-persistent-reader reader slot-definition class class-name))
    (loop for writer in writers
	  do (make-persistent-writer writer slot-definition class class-name))))

#+allegro
(defmethod initialize-accessors ((slot-definition cached-slot-definition) class)
  (let ((writers (slot-definition-writers slot-definition))
	(class-name (class-name class)))
    (loop for writer in writers
	  do (make-persistent-writer writer slot-definition class class-name))))

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
	(persistent-slot-writer (get-con instance) new-value instance (slot-definition-name slot-def))
	(call-next-method new-value class instance (slot-definition-name slot-def)))))

#+lispworks
(defmethod slot-makunbound-using-class ((class persistent-metaclass) (instance persistent-object) slot)
  "Removes the slot value from the database."
  (let ((slot-def (or (find slot (class-slots class) :key 'slot-definition-name)
		      (find slot (class-slots class)))))
    (if (typep slot-def 'persistent-slot-definition)
	(persistent-slot-makunbound (get-con instance) instance (slot-definition-name slot-def))
	(call-next-method class instance (slot-definition-name slot-def)))))
