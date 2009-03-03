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

(declaim #-elephant-without-optimize (optimize (speed 3))
	 #+elephant-without-optimize (optimize (speed 1) (safety 3) (debug 3)))

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
  (assert sc)
  (if from-oid
      (setf (oid instance) from-oid)
      (register-new-instance instance (class-of instance) sc))
  (setf (db-spec instance) (controller-spec sc))
  (cache-instance sc instance))

(defun register-new-instance (instance class sc)
  (setf (oid instance) (next-oid sc))
  (register-instance sc class instance))

(defun check-valid-store-controller (sc)
  (ifret (subtypep (type-of sc) 'store-controller)
	 (error "This function requires a valid store controller")))

(defclass persistent-collection (persistent) ()
  (:documentation "Abstract superclass of all collection types."))

(defclass persistent-object (persistent) ()
  (:metaclass persistent-metaclass)
  (:documentation 
   "Superclass for all user-defined persistent classes.  This is
    automatically inherited if you use the persistent-metaclass
    metaclass.  This allows specialization of functions for user
    objects that would not be appropriate for Elephant objects
    such as persistent collections"))

(defclass cacheable-persistent-object (persistent-object)
  ((pchecked-out :accessor pchecked-out-p :initform nil)
   (checked-out :accessor checked-out-p :initform nil :transient t))
  (:metaclass persistent-metaclass)
  (:documentation 
   "Adds a special value slot to store checkout state"))


;; ================================================
;; METACLASS INITIALIZATION 
;; ================================================

(defmethod shared-initialize :around ((class persistent-metaclass) slot-names &rest args &key direct-superclasses direct-slots index cache-style)
  "Ensures we inherit from persistent-object prior to initializing."
  (declare (ignorable index))
  ;; When we declare slots and don't initialize the cache-style and don't
  ;; inherit cacheable-persistent-object...inform the user
  (when (and (not cache-style) (has-cached-slot-specification direct-slots)
	     (not (superclass-member-p 'cacheable-persistent-object 
				       (class-direct-superclasses class))))
    (error "Must specify the class caching style if you declare cached slots and don't~%inherit from a cached class.  Class option :cache-style must be one of~% :checkout, :txn or :none"))
  (let* ((new-direct-superclasses 
	  (if cache-style 
	      (ensure-class-inherits-from class '(cacheable-persistent-object) direct-superclasses)
	      (ensure-class-inherits-from class '(persistent-object) direct-superclasses))))
    ;; Call the next method
    (prog1
	(apply #'call-next-method class slot-names
	       :direct-superclasses new-direct-superclasses 
	       (remove-keywords '(:direct-superclasses :index) args))
      ;; Make sure we convert the cache argument so it can be used in accessors
      (when (consp (get-cache-style class))
	(setf (get-cache-style class) (first (get-cache-style class)))))))

(defun ensure-class-inherits-from (class from-classnames direct-superclasses)
  (let* ((from-classes (mapcar #'find-class from-classnames))
	 (has-persistent-objects 
	  (every #'(lambda (class) (superclass-member-p class direct-superclasses))
		 from-classes)))
    (if (not (or (member class from-classes) has-persistent-objects))
	(progn
	  (dolist (class from-classes)
	    (setf direct-superclasses (remove class direct-superclasses)))
	  (append direct-superclasses from-classes))
	direct-superclasses)))

(defun superclass-member-p (class superclasses)
  "Searches superclass list for class"
  (some #'(lambda (superclass)
	    (or (eq class superclass)
		(let ((supers (class-direct-superclasses superclass)))
		  (when supers
		    (superclass-member-p class supers)))))
	superclasses))

(defun has-cached-slot-specification (direct-slot-defs)
  (some #'(lambda (slot) (getf slot :cached)) direct-slot-defs))

(defun warn-on-reinitialization-data-loss (class)
  "Warnings at class def time:
   - set-valued/assoc (warn!)
   - persistent/indexed/cached (warn?)
   - derived hints?
   Be nice to be able to restore the slots rather than just
   avoid updating"
  (let* ((old-schema (get-class-schema class))
	 (new-schema (class-instance-schema class))
	 (diffs (schema-diff new-schema old-schema)))
    (dolist (diff diffs)
      (when (eq (diff-type diff) :rem)
	(warn-about-dropped-slots :rem class
				  (mapcar #'slot-rec-name (cdr diff)))))))

#-ccl
(defmethod finalize-inheritance :after ((class persistent-metaclass))
  (ensure-schemas class))

#+ccl
(defmethod ensure-class-using-class :around (instance name &rest args)
  (declare (ignore name args instance))
  (let ((class-obj (call-next-method)))
    (ensure-schemas class-obj)
    class-obj))

(defmethod ensure-schemas ((instance t)) ())
(defmethod ensure-schemas ((instance persistent-metaclass))
  "Constructs the metaclass schema when the class hierarchy is valid"
  (let* ((old-schema (get-class-schema instance))
	 (new-schema (class-instance-schema instance)))
    ;; Stop synchronization if necessary to allow for reversing the
    ;; interactive re-definition
    (when *warn-on-manual-class-finalization*
      (warn-on-reinitialization-data-loss instance))
    ;; Update schema chain
    (setf (schema-predecessor new-schema) old-schema)
    (setf (get-class-schema instance) new-schema)
    (and *store-controller* (not (subtypep (class-name instance) 'btree))
      (lookup-schema *store-controller* instance)) ; ensure db schema of user-defined classes
    ;; Cleanup some slot values
    (let ((idx-state (get-class-indexing instance)))
      (when (consp idx-state)
	(setf (get-class-indexing instance) (first idx-state))))
    ;; Compute derived index triggers
    (awhen (derived-index-slot-defs instance)
      (compute-derived-index-triggers instance it))
    ;; Synchronize instances to new schemas
    (when (and old-schema (not (match-schemas new-schema old-schema)))
      (synchronize-stores-for-class instance))
    (and *store-controller*
         (not (subtypep (class-name instance) 'btree))
         (not (match-schemas (lookup-schema *store-controller* instance) new-schema))
         (synchronize-stores-for-class instance))
    instance))

(defun compute-derived-index-triggers (class derived-slot-defs)
  (let* ((sdefs (all-single-valued-slot-defs class))
	 (sdef-names (mapcar #'slot-definition-name sdefs)))
    (dolist (ddef derived-slot-defs)
      (dolist (sname (ifret (derived-slot-deps ddef) sdef-names))
	(unless (member sname sdef-names)
	  (error "Finalization error: derived index dependency hint ~A not a valid slot name"
		 sname))
	(push ddef (derived-slot-triggers (find-slot-def-by-name class sname)))))))


;; ===============================================
;;  CLASS INSTANCE INITIALIZATION PROTOCOL
;; ===============================================

;; Some syntactic sugar

(defmacro bind-slot-defs (class slots bindings &body body)
  "Bindings contain name, accessor pairs.  Extract 
   slot-definitions into variable name using accessor and
   filter by the list of valid slots"
  (with-gensyms (classref slotrefs)
    `(let* ((,classref ,class)
	    (,slotrefs ,slots)
	    ,@(compute-bindings classref slotrefs bindings))
     ,@body)))

(eval-when (:compile-toplevel :load-toplevel)
  (defun compute-bindings (class slots bindings)
    "Helper function for bind-slot-defs"
    (loop for (name accessor) in bindings collect
	 `(,name (get-init-slotnames ,class #',accessor ,slots)))))

;; Main protocol

(defmethod initialize-instance :around ((instance persistent-object) &rest initargs 
					&key (sc *store-controller*) &allow-other-keys)
  "Ensure instance creation is inside a transaction, huge (5x) performance impact per object"
  (declare (ignore initargs))
  (assert sc nil "You must have an open store controller to create ~A" instance)
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
     (cached-slots cached-slot-names)
     (indexed-slots indexed-slot-names)
     (derived-slots derived-index-slot-names)
     (association-end-slots association-end-slot-names)
     (persistent-slots persistent-slot-names))
    ;; Slot initialization
    (let* ((class (class-of instance))
	   (persistent-initializable-slots 
	    (union (union persistent-slots indexed-slots) association-end-slots))
	   (set-slots (get-init-slotnames class #'set-valued-slot-names slot-names)))
;;      NOTE: backing store for cached slots is only initialized on checkout or txn
      (cond (from-oid ;; If re-starting, make sure we read the cached values
;;	     (refresh-cached-slots instance cached-slots)) ;; old model dependency
	     nil)
	    (t  ;; If new instance, initialize all slots
	     (setq transient-slots (union transient-slots cached-slots))
	     (initialize-persistent-slots class instance persistent-initializable-slots initargs from-oid)))
      ;; Always initialize transients
      (apply #'call-next-method instance transient-slots initargs)
      ;; Initialize set slots after transient initialization
      (unless from-oid
	(initialize-set-slots class instance set-slots))
      (loop for dslotname in derived-slots do
	   (derived-index-updater class instance (find-slot-def-by-name class dslotname))))))

(defmethod shared-initialize :around ((instance cacheable-persistent-object) slot-names &key make-cached-instance &allow-other-keys)
  ;; User asked us to start in cached mode?  Otherwise default to not.
  (setf (slot-value instance 'pchecked-out) make-cached-instance)
  (setf (slot-value instance 'checked-out) make-cached-instance)
  (call-next-method))

(defun initialize-persistent-slots (class instance persistent-slot-inits initargs object-exists)
  (dolist (slotname persistent-slot-inits)
    (let ((slot-def (find-slot-def-by-name class slotname)))
      (unless (or (initialize-from-initarg class instance slot-def 
					   (slot-definition-initargs slot-def) initargs)
		  object-exists
		  (slot-boundp-using-class class instance slot-def))
	(awhen (slot-definition-initfunction slot-def)
	  (setf (slot-value-using-class class instance slot-def)
		(funcall it)))))))

(defun initialize-set-slots (class instance set-slots)
  (declare (ignore class instance))
  (dolist (slotname set-slots)
    (declare (ignorable slotname))
;;    (setf (slot-value-using-class class instance
;;				  (find-slot-def-by-name class slotname))
;;	  nil)
    ))

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

(defun warn-about-dropped-slots (op class names)
  (when (and *warn-when-dropping-persistent-slots* names)
    (cerror "Drop the slots" 
	    'dropping-persistent-slot-data
	    :operation op
	    :class class
	    :slotnames names)))

(define-condition dropping-persistent-slot-data ()
  ((operation :initarg :operation :reader persistent-slot-drop-operation)
   (class :initarg :class :reader persistent-slot-drop-class)
   (slots :initarg :slotnames :reader persistent-slot-drop-names))
  (:report (lambda (cond stream)
	     (with-slots (class slots operation) cond
	       (format stream "Dropping slot(s) ~A for class ~A in ~A."
		       slots class operation)))))

;; ================================================
;;  RECREATING A PERSISTENT INSTANCE FROM THE DB
;; ================================================

(defmethod recreate-instance-using-class ((class t) &rest initargs &key &allow-other-keys)
  "Implement a subset of the make-instance functionality to avoid initialize-instance
   calls after the initial creation time"
  (apply #'recreate-instance (allocate-instance class) initargs))

(defgeneric recreate-instance (instance &rest initargs &key &allow-other-keys)
  (:method ((instance t) &rest args)
    (declare (ignore args))
    instance))

(defmethod recreate-instance ((instance persistent-object) &rest args &key from-oid schema (sc *store-controller*))
  (declare (ignore args))
  ;; Initialize basic instance data
  (initial-persistent-setup instance :from-oid from-oid :sc sc)
  ;; Update db instance data
  (when schema
    (let ((official-schema (lookup-schema sc (class-of instance))))
      (unless (eq (schema-id schema) (schema-id official-schema))
	(upgrade-db-instance instance official-schema schema nil))))
  ;; Load cached slots, set, assoc values, etc.
  (shared-initialize instance t :from-oid from-oid)
  instance)

(defmethod recreate-instance ((instance persistent-collection) &rest initargs &key from-oid (sc *store-controller*))
  (declare (ignore initargs))
  ;; Initialize basic instance data
  (initial-persistent-setup instance :from-oid from-oid :sc sc)
  ;; Load cached slots, set, assoc values, etc.
  (shared-initialize instance t :from-oid from-oid)
  instance)

;; ================================
;;  CLASS REDEFINITION PROTOCOL
;; ================================

(defmethod update-instance-for-redefined-class :around ((instance persistent-object) added-slots discarded-slots property-list &rest initargs)
  (declare (ignore discarded-slots added-slots initargs))
  (let* ((sc (get-con instance))
;;	 (class (class-of instance))
	 (current-schema (get-current-db-schema sc (type-of instance))))
;;    (unless (match-schemas (%class-schema class) current-schema))
      (prog1 
	  (call-next-method)
	(let ((prior-schema (aif (schema-predecessor current-schema)
				 (get-controller-schema sc it)
				 (error "If the schemas mismatch, a derived controller schema should have been computed"))))
	  (assert (and current-schema prior-schema))
	  (upgrade-db-instance instance current-schema prior-schema property-list)))))


;; =================================
;;  CLASS CHANGE PROTOCOL   
;; =================================

;; Persistent objects

(defmethod change-class :before ((previous persistent) (new-class standard-class) &rest initargs)
  (declare (ignorable initargs))
  (unless (subtypep (type-of new-class) 'persistent-metaclass)
    (error "Persistent instances cannot be changed to standard classes via change-class")))

(defmethod update-instance-for-different-class :after ((previous persistent-object) (current persistent-object) 
							&rest initargs &key)
  ;; Update db to new class configuration
  ;; - handle indices, removals, associations and additions
  (let* ((sc (get-con current))
	 (current-schema (lookup-schema sc (class-of current)))
	 (previous-schema (lookup-schema sc (class-of previous))))
    (assert (eq sc (get-con previous)))
    (change-db-instance current previous current-schema previous-schema)
    ;; Deal with new persistent slot, cached and transient initialization
    (let* ((diff-entries (schema-diff current-schema previous-schema))
	   (add-entries (remove-if-not (lambda (entry) (eq :add (diff-type entry))) diff-entries))
	   (add-names (when add-entries (mapcar #'slot-rec-name (mapcan #'diff-recs add-entries)))))
      (apply #'shared-initialize current add-names initargs))))

;; Standard objects

(defmethod change-class :before ((previous standard-object) (new-class persistent-metaclass) &rest initargs)
  (declare (ignorable initargs)) 
  (unless (subtypep (type-of previous) 'persistent)
    (error "Cannot convert standard objects to persistent objects")))
;;  (warn "We have not thought through persistent to standard class conversions, so you get what you deserve converting standard class ~A to persistent class ~A" (class-of previous) new-class)
;;  nil)
;;  (unless (subtypep (type-of previous) 'persistent)
;;    (error "Standard classes cannot be changed to persistent classes in change-class")))

#+nil
(defmethod update-instance-for-different-class :after ((previous standard-object) (current persistent-object) 
							&rest initargs &key (sc *store-controller*) 
						       &allow-other-keys)
  (let* ((sc (or sc *store-controller*))
	 (current-schema (lookup-schema sc (class-of current)))
	 (previous-schema (compute-transient-schema (type-of current))))
    (setf (oid current) (next-oid sc))
    (setf (db-spec current) (controller-spec sc))
    (change-db-instance current previous current-schema previous-schema)
    (let* ((diff-entries (schema-diff current-schema previous-schema))
	   (add-entries (remove-if-not (lambda (entry) (eq :add (diff-type entry))) diff-entries))
	   (add-names (when add-entries (mapcar #'slot-rec-name (mapcan #'diff-recs add-entries)))))
      (apply #'shared-initialize current add-names initargs))))

