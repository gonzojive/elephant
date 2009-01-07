;; -*- Mode: Lisp; Syntax: ANSI-Common-Lisp; Base: 10 -*-
;;;
;;; controller.lisp -- Lisp interface to a Berkeley DB store
;;; 
;;; Initial version 8/26/2004 by Ben Lee
;;; <blee@common-lisp.net>
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

;;
;; TRACKING OBJECT STORES
;;

(defparameter *elephant-data-stores*
  '((:bdb (:ele-bdb))
    (:clsql (:ele-clsql))
    (:prevalence ())
    (:postmodern (:ele-postmodern))
    )
  "Tells the main elephant code the tag used in a store spec to
   refer to a given data store.  The second argument is an asdf
   dependency list.  Entries have the form of 
  (data-store-type-tag asdf-depends-list")

(defvar *elephant-controller-init* (make-hash-table))

(defun register-data-store-con-init (name controller-init-fn)
  "Data stores must call this function during the
   loading/compilation process to register their initialization
   function for the tag name in *elephant-data-stores*.  The
   initialization function returns a fresh instance of the
   data stores store-controller subclass"
  (setf (gethash name *elephant-controller-init*) controller-init-fn))

(defun lookup-data-store-con-init (name)
  (gethash name *elephant-controller-init*))

(defvar *dbconnection-spec* nil)
(defvar *dbconnection-lock* (ele-make-lock))

(defgeneric get-con (instance)
  (:documentation "This is used to find and validate the connection spec
   maintained for in-memory persistent objects.  Should
   we re-open the controller from the spec if it's not
   cached?  That might be dangerous so for now we error"))

(define-condition controller-lost-error ()
   ((object :initarg :object :accessor store-controller-closed-error-object)
    (spec :initarg :spec :accessor store-controller-closed-error-spec)))

(defun signal-controller-lost-error (object)
  (cerror "Open a new instance and continue?"
	  'controller-lost-error
	  :format-control "Store controller for specification ~A for object ~A cannot be found."
	  :format-arguments (list object (db-spec object))
	  :object object
	  :spec (db-spec object)))

(defun lookup-con-spec (spec)
  (ifret (fast-lookup-con-spec spec)
	 (ifret (slow-lookup-con-spec spec)
		nil)))

(defun fast-lookup-con-spec (spec)
  (let ((result (assoc spec *dbconnection-spec*)))
    (when result
      (cdr result))))

(defun slow-lookup-con-spec (spec)
  (let ((result (assoc spec *dbconnection-spec* :test #'equalp)))
    (when result
      (cdr result))))

(defun set-con-spec (spec sc)
  (ele-with-lock (*dbconnection-lock*)
    (setf *dbconnection-spec* 
	  (acons spec sc *dbconnection-spec*))))

(defun delete-con-spec (spec)
  (ele-with-lock (*dbconnection-lock*)
    (setf *dbconnection-spec*
	  (delete spec *dbconnection-spec* :key #'car :test #'equalp))))

(defmethod get-con ((instance persistent))
  (let ((con (fast-lookup-con-spec (db-spec instance))))
    (cond ((not con)
	   (aif (slow-lookup-con-spec (db-spec instance))
  	     (progn
	       (setf (db-spec instance) 
		     (car (find it *dbconnection-spec* :key #'cdr)))
	       (get-con instance))
	     (progn (signal-controller-lost-error instance)
		    (open-controller 
		     (get-controller (db-spec instance))))))
	  ;; If it's valid and open
	  ((and con (connection-is-indeed-open con))
	   con)
	  ;; If the controller object exists but is closed, reopen
	  (t (open-controller con)))
    con))

(defun get-controller (spec)
  "This is used by open-store to fetch or open a controller.
   This maintains the dbconnection-spec table so should be
   the only point of entry for getting access to controllers
   from specs.  Get-con is used to validate connections and
   reopen if necessary and perhaps these two should be combined
   at some point"
  (let ((cached-sc (lookup-con-spec spec)))
    (if (and cached-sc (connection-is-indeed-open cached-sc))
	cached-sc
	(build-controller spec))))

(defun build-controller (spec)
  "Actually construct the controller & load dependencies"
  (assert (and (consp spec) (symbolp (first spec))))
  (load-data-store (first spec))
  (let ((init (lookup-data-store-con-init (first spec))))
      (unless init (error "Store controller init function not registered for data store ~A." (car spec)))
      (let ((sc (funcall (symbol-function init) spec)))
	(set-con-spec spec sc)
	sc)))

(defun load-data-store (type)
  (assert (find-package :asdf))
  (let ((record (assoc type *elephant-data-stores*)))
    (when (or (null record) (not (consp record)))
      (error "Unknown data store type ~A, cannot load" type))
    (satisfy-asdf-dependencies (second record))))

(defun satisfy-asdf-dependencies (dep-list)
  (mapc #'(lambda (dep) 
	    ;; Only load the first time, after that it's the 
	    ;; users fault if they edit source code
	    (unless (asdf::system-registered-p dep)
	      (asdf:operate 'asdf:load-op dep)))
	dep-list))

;;
;; PER-USER INSTALLATION PARAMETERS
;;

(defun get-user-configuration-parameter (name)
  "This function pulls a value from the key-value pairs stored in
   my-config.sexp so data stores can have their own pairs for appropriate
   customization after loading."
  (elephant-system::get-config-option
   name
   (asdf:find-system :elephant)))

(defun initialize-user-parameters ()
  (loop for (keyword variable) in *user-configurable-parameters* do
       (awhen (get-user-configuration-parameter keyword)
	 (setf (symbol-value variable) it))))

;;
;; COMMON STORE CONTROLLER FUNCTIONALITY
;;

(defclass store-controller ()
  ((spec :type list
	 :accessor controller-spec
	 :initarg :spec
	 :documentation "Data store initialization functions are
	 expected to initialize :spec on the call to
	 make-instance")
   (db-version :type fixnum
	       :accessor controller-database-version
	       :initform 100)
   ;; Generic support for the object, indexing and root protocols
   (root :reader controller-root 
	 :documentation "This is an instance of the data store
	 persistent btree.  It should have an OID that is fixed in
	 the code and does not change between sessions.  Usually
	 it this is something like 0, 1 or -1")
   ;; Schema storage and caching
   (schema-table :reader controller-schema-table
		 :documentation "Schema id to schema database table")
   (schema-name-index :reader controller-schema-name-index
		      :documentation "Schema name to schema database table")
   (schema-cache :accessor controller-schema-cache :initform (make-cache-table :test 'eq)
		 :documentation "This is a cache of class schemas stored in the database indexed by classid")
   (schema-classes :accessor controller-schema-classes :initform nil
		      :documentation "Maintains a list of all classes that have a cached schema value so we can shutdown cleanly")
   (schema-cache-lock :accessor controller-schema-cache-lock :initform (ele-make-fast-lock)
			:documentation "Protection for updates to the cache from multiple threads.  
                                        Do not override.")
   ;; Instance storage
   (instance-table :reader controller-instance-table
		  :documentation "Contains btree of oid to class ids")
   (instance-class-index :reader controller-instance-class-index
			 :documentation "A reverse map of class id to oid")
   (instance-cache :accessor controller-instance-cache :initform (make-cache-table :test 'eql)
		   :documentation 
		   "This is an instance cache and part of the
                    metaclass protocol.  Data stores should not
                    override the default behavior.")
   (instance-cache-lock :accessor controller-instance-cache-lock :initform (ele-make-fast-lock)
			:documentation "Protection for updates to
			the cache from multiple threads.  Do not
			override.")
   ;; Root table for all indices
   (index-table :reader controller-index-table
	       :documentation 
	       "This is another root for class indexing that is
	       also a data store specific persistent btree instance
	       with a unique OID that persists between sessions.
               No cache is needed because we cache in the class slots.")
   ;; Upgradable serializer strategy
   (serializer-version :accessor controller-serializer-version :initform nil
		       :documentation "Governs the default
		       behavior regarding which serializer
		       version the current elephant core is
		       using.  Data stores can override by creating
		       a method on initialize-serializer.")
   (serialize :accessor controller-serialize :initform nil
	      :documentation "Accessed by elephant::serialize to
	      get the entry point to the default serializer or to
	      a data store specific serializer")
   (serialize-fn :accessor controller-serialize-fn :initform nil
	      :documentation "Accessed by elephant::serialize to
	      get the entry point to the default serializer or to
	      a data store specific serializer")
   (deserialize :accessor controller-deserialize :initform nil
		:documentation "Contains the entry point for the
		specific serializer to be called by
		elephant::deserialize")
   (deserialize-fn :accessor controller-deserialize-fn :initform nil
		:documentation "Contains the entry point for the
		specific serializer to be called by
		elephant::deserialize"))
  (:documentation 
   "Superclass for the data store controller, the main interface
    to any book-keeping, references to DB handles, the instance
    cache, btree table creation, counters, locks, the roots (for
    garbage collection,) et cetera.  Behavior is shared between
    the superclass and subclasses.  See slot documentation for
    details."))

(defun schema-classname-keyform (idx schema-id schema)
  (declare (ignore idx schema-id))
  (values t (schema-classname schema)))

(defun instance-cidx-keyform (idx oid cidx)
  (declare (ignore idx oid))
  (values t cidx))

(defmethod print-object ((sc store-controller) stream)
  (format stream "#<~A ~A>" (type-of sc) (second (controller-spec sc))))

;;
;; Maintain persistent instance table
;;

(defmethod register-instance ((sc store-controller) cl instance)
  "When creating an instance for the first time, write it to the persistent
   instance table using the controller instance for its class"
  (set-instance-schema-id sc (oid instance) (class-schema-id sc cl)))

(defun class-schema-id (sc class)
  (if (subtypep (class-name class) 'btree)
      (default-class-id (class-name class) sc)
      (schema-id (lookup-schema sc class))))

(defmethod set-instance-schema-id ((sc store-controller) oid cid)
  (let ((table (controller-instance-table sc)))
    (remove-kv oid table)
    (setf (get-value oid table) cid)))

(defmethod get-instance-class ((sc store-controller) oid &optional classname)
  "Get the class object using the oid or using the provided classname"
  (when classname
    (return-from get-instance-class (find-class classname)))
  (let ((cid (oid->schema-id oid sc)))
    (unless cid
      (signal-missing-instance oid (controller-spec sc))
      (return-from get-instance-class (find-class 'persistent-object)))
    (get-schema-id-class sc cid)))

(defmethod get-schema-id-class ((sc store-controller) cid)
  "Get the class given the schema id"
  (aif (default-class-id-type cid sc)
       (find-class it)
       (let ((schema (get-controller-schema sc cid)))
	 (values (find-class (schema-classname schema)) schema))))

(define-condition missing-persistent-instance ()
   ((oid :initarg :oid :accessor missing-persistent-instance-oid)
    (spec :initarg :spec :accessor missing-persistent-instance-spec)))

(defun signal-missing-instance (oid spec)
  (cerror "Return a proxy object"
	  'missing-persistent-instance
	  :format-control "Instance with OID ~A is not present in ~A"
	  :format-arguments (list oid spec)
	  :oid oid
	  :spec spec))

;;
;; Controller instance creation 
;;


(defmethod controller-recreate-instance ((sc store-controller) oid &optional classname)
  "Called by the deserializer to return an instance"
  (handler-case 
      (progn 
	(awhen (get-cached-instance sc oid)
	  (return-from controller-recreate-instance it))
	(multiple-value-bind (class schema) (get-instance-class sc oid classname)
	  (recreate-instance-using-class class :from-oid oid :sc sc :schema schema)))
    (missing-persistent-instance (e)
      (unless *return-null-on-missing-instance*
	(signal e)))))
    

;;
;; Controller instance caching
;;

(defmethod cache-instance ((sc store-controller) obj)
  "Cache a persistent object with the controller."
  (declare (type store-controller sc))
  (ele-with-fast-lock ((controller-instance-cache-lock sc))
    (setf (get-cache (oid obj) (controller-instance-cache sc)) obj)))

(defmethod get-cached-instance ((sc store-controller) oid)
  "Get a cached instance, or instantiate!"
  (declare (type store-controller sc)
	   (type fixnum oid))
  (awhen (get-cache oid (controller-instance-cache sc))
    it))

(defmethod uncache-instance ((sc store-controller) oid)
  (ele-with-fast-lock ((controller-instance-cache-lock sc))
    (remcache oid (controller-instance-cache sc))))

(defmethod flush-instance-cache ((sc store-controller))
  "Reset the instance cache (flush object lookups).  Useful 
   for testing.  Does not reclaim existing objects so there
   will be duplicate instances with identical functionality"
  (ele-with-fast-lock ((controller-instance-cache-lock sc))
    (setf (controller-instance-cache sc)
	  (make-cache-table :test 'eql))))

;;
;; Maintaining Schemas
;;

;; Schema ID bootstrapping

(defmethod oid->schema-id (oid (sc store-controller))
  (get-value oid (controller-instance-table sc)))

(defgeneric default-class-id (base-type sc)
  (:documentation "A method implemented by the store controller for providing
   fixed class ids for basic btree derivative types"))

(defgeneric default-class-id-type (id sc)
  (:documentation "A method implemented by the store controller which provides
   the type associated with a default id or nil if the id does not match"))

(defgeneric reserved-oid-p (sc oid)
  (:documentation "Is this OID reserved by the controller? GC doesn't touch"))

;; Looking up schemas

(defmethod lookup-schema ((sc store-controller) (class persistent-metaclass))
  "Get the latest db class schema from caches, etc."
  ;; Lookup class cached version
  (awhen (get-class-controller-schema sc class) 
    (when (eq (schema-successor it) nil) 
      (return-from lookup-schema it)))

  ;; Lookup persistent version
  (aif (get-current-db-schema sc (class-name class))
       ;; Store it
       (prog1 it
	 (add-class-controller-schema sc class it))
       ;; Or create it
       (create-controller-schema sc class)))

(defmethod get-controller-schema ((sc store-controller) schema-id)
  "Find the db class schema by schema id"
  (assert (typep schema-id 'fixnum))
  ;; Lookup in controller cache
  (ifret (get-cache schema-id (controller-schema-cache sc))
	 ;; Lookup in store table
	 (let* ((schema (get-value schema-id (controller-schema-table sc)))
		(class (find-class (schema-classname schema))))
	   (assert schema)
	   ;; Update controller cache
	   (ele-with-fast-lock ((controller-schema-cache-lock sc))
	     (setf (get-cache schema-id (controller-schema-cache sc)) schema))
	   ;; Also cache in class slot
	   (add-class-controller-schema sc class schema)
	   schema)))

(defmethod create-controller-schema ((sc store-controller) class)
  "We don't have a cached version, so create a new one"
  (ensure-finalized class)
  (let ((db-schema (make-db-schema (next-cid sc) (%class-schema class))))
    ;; Add to database
    (setf (get-value (schema-id db-schema) (controller-schema-table sc))
	  db-schema)
    ;; Let get-controller-schema cache it for us
    (get-controller-schema sc (schema-id db-schema))))

(defmethod update-controller-schema ((sc store-controller) db-schema &optional update-cache)
  "Use this to update the schema version that is on store and in 
   all the various caches"
  (assert (typep db-schema 'db-schema))
  (assert (schema-id db-schema))
  (let ((schema-id (schema-id db-schema)))
    (setf (get-value schema-id (controller-schema-table sc))
	  db-schema)
    (when update-cache
      (ele-with-fast-lock ((controller-schema-cache-lock sc))
	(setf (get-cache schema-id (controller-schema-cache sc)) db-schema))
      (awhen (find-class (schema-classname db-schema) nil)
	(add-class-controller-schema sc (find-class (schema-classname db-schema)) db-schema)))))

(defmethod remove-controller-schema ((sc store-controller) schema-id)
  "Remove a schema from the controller table; uncache separately"
  (remove-kv schema-id (controller-schema-table sc)))

(defmethod uncache-controller-schema ((sc store-controller) schema-id)
  (handler-case
      (progn
	(ele-with-fast-lock ((controller-schema-cache-lock sc))
	  (remcache schema-id (controller-schema-cache sc)))
	(remove-class-controller-schema sc (get-schema-id-class sc schema-id)))
    (program-error () ;; in case the class is gone for some reason
;;      (format t "Error ~A in uncache-controller-schema , ignoring" e)
      nil)))

(defun get-current-db-schema (sc name)
  (awhen (sort (get-db-schemas sc name)
	       #'> :key #'schema-id)
    (car it)))

(defun get-db-schemas (sc classname)
  "Return schemas ordered oldest to youngest (ascending cids)"
  (sort
   (map-btree #'(lambda (cname schema)
		  (declare (ignore cname))
		  schema)
	      (controller-schema-name-index sc)
	      :value classname :collect t)
   #'<
   :key #'schema-id))

;;
;; Database versioning
;;

(defgeneric database-version (sc)
  (:documentation "Data stores implement this to store the serializer version.
                   The protocol requires that data stores report their database
                   version.  On new database creation, the database is written with the
                   *elephant-code-version* so that is returned by database-version.
                   If a legacy database does not have a version according to the method
                   then it should return nil"))

(defmethod database-version :around (sc)
  "Default version assumption for unmarked databases is 0.6.0.
   It is possible to check for 0.5.0 databases, but it is not implemented 
   now due to the low (none?) number of users still on 0.5.0"
  (declare (ignorable sc))
  (let ((db-version (call-next-method)))
    (if db-version db-version
	'(0 6 0))))

(defun prior-version-p (v1 v2)
  "Is v1 an equal or earlier version than v2"
  (cond ((and (null v1) (null v2))         t)
        ((and (null v1) (not (null v2)))   t)
	((and (not (null v1)) (null v2))   nil)
	((< (car v1) (car v2))             t)
	((> (car v1) (car v2))             nil)
	((= (car v1) (car v2))
	 (prior-version-p (cdr v1) (cdr v2)))
	(t (error "Version comparison problem: (prior-version-p ~A ~A)" v1 v2))))

;;
;; Database upgrade paths
;;

(defparameter *elephant-upgrade-table*
  '( ((1 0 0) (0 6 0))
     ((1 0 0) (0 9 0))
     ((1 0 0) (0 9 1))
     ((1 0 0) (0 9 2))
   ))

(defmethod up-to-date-p ((sc store-controller))
  (equal (database-version sc) *elephant-code-version*))

(defmethod upgradable-p ((sc store-controller))
  "Determine if this store can be brought up to date using the upgrade function"
  (unwind-protect
       (let ((row (assoc *elephant-code-version* *elephant-upgrade-table* :test #'equal))
	     (ver (database-version sc)))
	 (when (member ver (rest row) :test #'equal)) t)
    nil))

(defgeneric upgrade (sc spec)
  (:documentation "Given an open store controller from a prior version, 
                   open a new store specified by spec and migrate the
                   data from the original store to the new one, upgrading
                   it to the latest version"))

(defmethod upgrade ((sc store-controller) target-spec)
  (unless (upgradable-p sc)
    (error "Cannot upgrade ~A from version ~A to version ~A~%Valid upgrades are:~%~A" 
	   (controller-spec sc)
	   (database-version sc)
	   *elephant-code-version*
	   *elephant-upgrade-table*))
  (warn "Please read the current limitations on migrate-based upgrade in migrate.lisp to ensure your 
         data does not require any unsupported features")
  (let ((source sc)
	(target (open-store target-spec)))
    (migrate target source)
    (close-store target)))

;;
;; Modular serializer support and default serializers for a version
;;

(defmethod initialize-serializer ((sc store-controller))
  "Establish serializer version on controller startup.  Data stores call this before
   they need the serializer to be valid and after they enable their database-version
   call.  If the data store shadows this, it has to keep track of serializer versions 
   associated with the database version that is opened."
  (cond ((prior-version-p (database-version sc) '(0 6 0))
	 (setf (controller-serializer-version sc) 1)
	 (setf (controller-serialize sc) 
	       (intern "SERIALIZE" (find-package :ELEPHANT-SERIALIZER1)))
	 (setf (controller-deserialize sc)
	       (intern "DESERIALIZE" (find-package :ELEPHANT-SERIALIZER1))))
	(t 
	 (setf (controller-serializer-version sc) 2)
	 (setf (controller-serialize sc) 
	       (intern "SERIALIZE" (find-package :ELEPHANT-SERIALIZER2)))
	 (setf (controller-deserialize sc) 
	       (intern "DESERIALIZE" (find-package :ELEPHANT-SERIALIZER2))))))

;;
;; Handling package changes in legacy databases 
;;

(defvar *always-convert* nil)

(defparameter *legacy-symbol-conversions*
  '(;; 0.5.0 support 
    (("elephant" . "bdb-btree") . ("sleepycat" . "bdb-btree"))
    (("elephant" . "bdb-indexed-btree") . ("sleepycat" . "bdb-indexed-btree"))
    (("elephant" . "bdb-btree-index") . ("sleepycat" . "bdb-btree-index"))))

(defun add-symbol-conversion (old-name old-package new-name new-package old-version)
  "Users can specify specific symbol conversions on upgrade prior to 
   migrating old databases"
  (declare (ignore old-version))
  (push (cons (cons old-name old-package) (cons new-name new-package)) *legacy-symbol-conversions*))

(defun map-legacy-symbols (symbol-string package-string old-version)
  (declare (ignore old-version))
  (let ((entry (assoc (cons (string-upcase symbol-string) (string-upcase package-string))
		      *legacy-symbol-conversions* :test #'equal)))
    (if entry
	(values t (cadr entry) (cddr entry))
	nil)))


(defparameter *legacy-package-conversions*
  '(("ELEPHANT-CLSQL" . "DB-CLSQL")
    ("SLEEPYCAT" . "DB-BDB")))

(defun add-package-conversion (old-package-string new-package-string old-version)
  "Users can specify wholesale package name conversions on upgrade 
   prior to migrating old databases"
  (declare (ignore old-version))
  (push (cons old-package-string new-package-string) *legacy-package-conversions*))

(defun map-legacy-package-names (package-string old-version)
  (declare (ignore old-version))
  (let ((entry (assoc (string-upcase package-string) *legacy-package-conversions* :test #'equal)))
    (if entry
	(cdr entry)
	package-string)))

(defun map-legacy-names (symbol-name package-name old-version)
  (multiple-value-bind (mapped? new-name new-package)
      (map-legacy-symbols symbol-name package-name old-version)
    (if mapped?
	(values new-name new-package)
	(values symbol-name (map-legacy-package-names package-name old-version)))))

(defun translate-and-intern-symbol (sc symbol-name package-name)
  "Service for the serializer to translate any renamed packages or symbols
   and then intern the decoded symbol."
  (if package-name 
      (multiple-value-bind (sname pname)
	  (if (or *always-convert* (not (= (controller-database-version sc)
					   *elephant-code-version-int*)))
	      (map-legacy-names symbol-name package-name (database-version sc))
	      (values symbol-name package-name))
	(let ((package (find-package pname)))
	  (if package
	      (intern sname package)
	      (progn
		(warn "Couldn't deserialize the package: ~A based on ~A~%
                       An uninterred symbol will be created" pname package-name)
		(make-symbol sname)))))
      (make-symbol symbol-name)))

;; ================================================================================
;;
;;                  DATA STORE CONTROLLER PROTOCOL
;;
;; ================================================================================

(defgeneric open-controller (sc &key recover recover-fatal thread &allow-other-keys)
  (:documentation 
   "Opens the underlying environment and all the necessary
database tables.  Different data stores may use different keys so
all methods should &allow-other-keys.  There are three standard
keywords: :recover, :recover-fatal and :thread.  Recover means
that recovery should be checked for or performed on startup.
Recover fatal means a full rebuild from log files is requested.
Thread merely indicates to the data store that it is a threaded
application and any steps that need to be taken (for example
transaction implementation) are taken.  :thread is usually
true."))

(defgeneric close-controller (sc)
  (:documentation 
   "Close the db handles and environment.  Should be in a state
   where lisp could be shut down without causing an inconsistent
   state in the db.  Also, the object could be used by
   open-controller to reopen the database"))

(defmethod open-controller :after ((sc store-controller) &rest args)
  (declare (ignore args))
  (with-transaction (:store-controller sc)
    ;; Initialize classname -> cidx
    (setf (slot-value sc 'schema-name-index)
	  (ensure-index (slot-value sc 'schema-table) 'by-name
			:key-form 'schema-classname-keyform))

    ;; Initialize class idx -> oid index
    (setf (slot-value sc 'instance-class-index)
	  (ensure-index (slot-value sc 'instance-table) 'by-name
			:key-form 'instance-cidx-keyform))))

(defmethod close-controller :before ((sc store-controller))
  (map-cache (lambda (schema-id schema) 
	       (declare (ignore schema))
	       (handler-case
		   (uncache-controller-schema sc schema-id)
		 (error () nil)))
	     (controller-schema-cache sc))
  (mapc (lambda (classname)
	  (handler-case
	      (remove-class-controller-schema sc (find-class classname))
	    (error () nil)))
	(controller-schema-classes sc))
  (setf (slot-value sc 'schema-name-index) nil)
  (setf (slot-value sc 'instance-class-index) nil)
  (setf (slot-value sc 'serialize-fn) nil)
  (setf (slot-value sc 'deserialize-fn) nil)
  (delete-con-spec (controller-spec sc)))


(defgeneric connection-is-indeed-open (controller)
  (:documentation "Validate the controller and the db that it is connected to")
  (:method ((controller t)) t))

(defgeneric next-oid (sc)
  (:documentation
   "Provides a persistent source of unique id's"))

(defgeneric next-cid (sc)
  (:documentation
   "Provides a unique class schema id's"))

(defgeneric optimize-layout (sc &key &allow-other-keys)
  (:documentation "If supported, speed up the index and allocation by freeing up
                   any available storage and return it to the free list.  See the
                   methods of data stores to determine what options are valid. Supported
                   both on stores (all btrees and persistent slots) and specific btrees"))

;;
;; Low-level support for metaclass protocol 
;;

(defgeneric persistent-slot-reader (sc instance name &optional oids-only)
  (:documentation 
   "Data store specific slot reader function"))

(defgeneric persistent-slot-writer (sc new-value instance name)
  (:documentation 
   "Data store specific slot writer function"))

(defgeneric persistent-slot-boundp (sc instance name)
  (:documentation
   "Data store specific slot bound test function"))

(defgeneric persistent-slot-makunbound (sc instance name)
  (:documentation
   "Data store specific slot makunbound handler"))


;; ================================================================================
;;
;;                             CONTROLLER USER API
;;   
;; ================================================================================


;;
;; Opening and closing data stores
;;

(defun open-store (spec &rest args)
  "Conveniently open a store controller.  Set *store-controller* to the new controller
   unless it is already set (opening a second controller means you must keep track of
   controllers yourself.  *store-controller* is a convenience variable for single-store
   applications or single-store per thread apps.  Multi-store apps should either confine
   their *store-controller* to a given dynamic context or wrap each store-specific op in
   a transaction using with or ensure transaction.  Returns the opened store controller."
  (assert (consp spec))
  ;; Ensure that parameters are set
  (initialize-user-parameters)
  (let ((controller (get-controller spec)))
    (apply #'open-controller controller args)
    (if *store-controller*
	(progn
;;	  (warn "Store controller already set so was not updated") ;; this was annoying me
	  controller)
	(setq *store-controller* controller))))

(defun close-store (&optional sc)
  "Conveniently close the store controller.  If you pass a custom store controller, you are responsible for setting it to NIL."
  (when (or sc *store-controller*)
    (close-controller (or sc *store-controller*)))
  (unless sc
    (setf *store-controller* nil)))

(defun close-all-stores ()
  (loop for pair in *dbconnection-spec*
       do (close-store (cdr pair))))

(defmacro with-open-store ((spec) &body body)
  "Executes the body with an open controller,
   unconditionally closing the controller on exit."
  `(let ((*store-controller* nil))
     (declare (special *store-controller*))
     (open-store ,spec)
     (unwind-protect
	  (progn ,@body)
       (close-store *store-controller*))))


;;
; Operations on the root index
;;

(defun add-to-root (key value &key (sc *store-controller*))
  "Add an arbitrary persistent thing to the root, so you can
   retrieve it in a later session.  Anything referenced by an
   object added to the root is considered reachable and thus live"
  (declare (type store-controller sc))
  (assert (not (eq key *elephant-properties-label*)))
  (setf (get-value key (controller-root sc)) value))

(defun get-from-root (key &key (sc *store-controller*))
  "Get the value associated with key from the root.  Returns two
   values, the value, or nil, and a boolean indicating whether a
   value was found or not (so you know if nil is a value or an
   indication of non-presence)"
  (declare (type store-controller sc))
  (get-value key (controller-root sc)))

(defun root-existsp (key &key (sc *store-controller*))
  "Test whether a given key is instantiated in the root"
  (declare (type store-controller sc))
  (if (existsp key (controller-root sc))
      t 
      nil))

(defun remove-from-root (key &key (sc *store-controller*))
  "Remove something from the root by the key value"
  (declare (type store-controller sc))
  (remove-kv key (controller-root sc)))

(defun map-root (fn &key (sc *store-controller*))
  "Takes a function of two arguments, key and value, to map over
   all key-value pairs in the root"
  (map-btree fn (controller-root sc)))

;;
;; Explicit storage reclamation
;;

(defgeneric drop-instance (persistent-object)
  (:documentation   "drop-instance reclaims persistent object storage by unbinding
   all persistent slot values.  It can also helps catch errors
   where an object should be unreachable, but a reference still
   exists elsewhere in the DB.  On access, the unbound slots
   should flag an error in the application program.  IMPORTANT:
   this function does not clear any serialized references still in the db.  
   Need a migration or GC for that!  drop-instances is the user-facing call 
   as it implements the proper behavior for indexed classes"))

(defmethod drop-instance ((inst persistent-object))
  (let ((sc (get-con inst)))
    (ensure-transaction (:store-controller sc)
      (drop-instance-slots inst)
      (call-next-method))))

(defmethod drop-instance ((inst persistent))
  (let ((sc (get-con inst)))
    (ele-with-fast-lock ((controller-instance-cache-lock sc))
      (remcache (oid inst) (controller-instance-cache sc)))
    (remove-kv (oid inst) (controller-instance-table sc))))

(defun drop-instance-slots (instance)
  "A helper function for drop-instance, that deletes the storage of 
   persistent slots of instance from the db"
  (let ((class (class-of instance)))
    (loop for slot-def in (class-slots class)
       when (persistent-p slot-def)
       do (slot-makunbound-using-class class instance slot-def))))

(defun drop-instances (instances &key (sc *store-controller*) (txn-size 500))
  "Removes a list of persistent objects from all class indices
   and unbinds any persistent slot values associated with those instances"
  (declare (optimize (speed 1) (debug 3) (safety 3)))
  (awhen (mklist instances)
    (assert (consp it))
    (do-subsets (subset txn-size it)
      (ensure-transaction (:store-controller sc)
	(mapc #'drop-instance subset)))))

;;
;; DATABASE PROPERTY INTERFACE (Not used by system as of 0.6.1, but supported)
;;

(defvar *restricted-properties* '()
  "Properties that are not user manipulable")

(defmethod controller-properties ((sc store-controller))
  (get-from-root *elephant-properties-label* :sc sc))

(defmethod set-ele-property (property value &key (sc *store-controller*))
  (assert (and (symbolp property) (not (member property *restricted-properties*))))
  (let ((props (get-from-root *elephant-properties-label* :sc sc)))
    (setf (get-value *elephant-properties-label* (controller-root sc))
	  (if (assoc property props)
	      (progn (setf (cdr (assoc property props)) value)
		     props)
	      (acons property value props)))))

(defmethod get-ele-property (property &key (sc *store-controller*))
  (assert (symbolp property))
  (let ((entry (assoc property 
		      (get-from-root *elephant-properties-label* 
				     :sc sc))))
    (when entry
      (cdr entry))))
