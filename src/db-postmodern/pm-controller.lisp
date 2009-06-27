;;; -*- Mode: Lisp; Syntax: ANSI-Common-Lisp; Base: 10 -*-
;;;
;;; pm-controller.lisp -- A postmodern postgresql elephant backend
;;; 
;;; part of
;;;
;;; Elephant: an object-oriented database for Common Lisp
;;;
;;; Copyright (c) 2007 Henrik Hjelte
;;;
;;; Elephant users are granted the rights to distribute and use this software
;;; as governed by the terms of the Lisp Lesser GNU Public License
;;; (http://opensource.franz.com/preamble.html), also known as the LLGPL.
;;;

(in-package :db-postmodern)

(defclass postmodern-store-controller (store-controller pm-executor)
  ((dbcons :accessor controller-db-table :initarg :db :initform (make-hash-table :test 'equal))
   (persistent-slot-collection :accessor persistent-slot-collection-of :initform nil)
   (dbversion :accessor postmodern-dbversion :initform nil))
  
  (:documentation  "Class of objects responsible for the
    book-keeping of holding DB handles, the cache, table
    creation, counters, locks, the root (for garbage collection,)
    et cetera.  This is the Postgresql-specific subclass of store-controller."))

(defmethod build-btree ((sc postmodern-store-controller))
  (make-instance 'pm-btree-wrapper :sc sc))

(defmethod build-dup-btree ((sc postmodern-store-controller))
  (make-instance 'pm-dup-btree-wrapper :sc sc))

(defmethod supports-sequence ((sc postmodern-store-controller))
  t)

;; This should be much more elegant --- but as of Feb. 6, SBCL 1.0.2 has a weird,
;; unpleasant bug when ASDF tries to load this stuff.
;; (defvar *thread-table-lock* nil)
;;  (defvar *thread-table-lock* (sb-thread::make-mutex :name "thread-table-lock"))

(defvar *thread-table-lock* nil)

(defun ensure-thread-table-lock ()
  (unless *thread-table-lock*
    (setq *thread-table-lock* (elephant-utils::ele-make-lock))))

;;------------------------------------------------------------------------------
;; Bookkeper, keeps a connection and a transaction count per thread

(defun make-thread-bookkeeper (con)
  (cons con 0))

(defmacro thread-bookkeeper-connection (bookkeeper)
  `(car ,bookkeeper))

(defmacro thread-bookkeeper-tran-count (bk)
  `(cdr ,bk))

(defun thread-hash ()
  (elephant-utils::ele-thread-hash-key))

(defun tran-count-of (sc)
  (elephant::ele-with-lock (*thread-table-lock*)
    (let ((bookkeeper (gethash (thread-hash) (controller-db-table sc))))
      (unless bookkeeper
        (error "No connection established, can't get tran-count-of"))
      (thread-bookkeeper-tran-count bookkeeper))))

(defun (setf tran-count-of) (value sc)
  (elephant::ele-with-lock (*thread-table-lock*)
    (setf (thread-bookkeeper-tran-count (gethash (thread-hash) (controller-db-table sc))) value)))


(defun reap-orphaned-connections (sc)
  (let ((n-reaped 0))
    (maphash (lambda (thread bookkeeper)
               (let ((alive-p (thread-alive-p thread)))
                 (unless alive-p
                   (cl-postgres:close-database (car bookkeeper))
                   (incf n-reaped)
                   (remhash thread (controller-db-table sc)))))
             (controller-db-table sc))
    n-reaped))

(defmethod controller-connection-for-thread ((sc postmodern-store-controller))
  (elephant::ele-with-lock (*thread-table-lock*)
    (let ((bookkeeper (gethash (thread-hash) (controller-db-table sc))))
      (if bookkeeper
          (thread-bookkeeper-connection bookkeeper)
          (destructuring-bind (db-type host database user password &key port) (second (controller-spec sc))
            (assert (eq :postgresql db-type))
            (reap-orphaned-connections sc)
            (let ((con (apply #'postmodern:connect database user password host :pooled-p nil
                              (if port
                                  (list :port port)
                                  nil))))
              (setf (gethash (thread-hash) (controller-db-table sc))
                    (make-thread-bookkeeper con))
              con))))))

(defvar *connection* nil)

(defmacro with-connection-for-thread ((controller) &body body)
  `(let ((*connection* (or *connection*
                           (controller-connection-for-thread ,controller))))
    ,@body))

(defun active-connection ()
  *connection*)

(eval-when (:compile-toplevel :load-toplevel)
  (register-data-store-con-init :postmodern 'pm-test-and-construct))

(defun pm-test-and-construct (spec)
  "Entry function for making SQL backend controllers"
  (if (pm-store-spec-p spec)
      (make-instance 'postmodern-store-controller 
		     :spec (if spec spec
			       '("localhost.localdomain" "test" "postgres" "")))
      (error (format nil "uninterpretable path/spec specifier: ~A" spec))))

(defun pm-store-spec-p (spec)
  (and (listp spec)
       (eq (first spec) :postmodern)))

;; Check that the table exists and is in proper form.
;; If it is not in proper form, signal an error, no 
;; way to recover from that automatically.  If it 
;; does not exist, return nil so we can create it later!

(defmacro with-postmodern-conn ((con) &body body)
  `(let ((postmodern:*database* ,con))
    (declare (special postmodern:*database*))
    ,@body))

(defun version-table-exists (con)
  (with-postmodern-conn (con)
    (postmodern:table-exists-p 'versionpm)))

(defun create-version-table (sc)
  (declare (ignore sc))
  (cl-postgres:exec-query (active-connection) "create table versionpm (dbversion varchar(20) not null)")
  (cl-postgres:exec-query (active-connection) (format nil "insert into versionpm (dbversion) values('~a')" *elephant-code-version*)))

(defun message-table-existsp (con)
  (with-postmodern-conn (con)
    (postmodern:table-exists-p 'message)))

(defmethod database-version ((sc postmodern-store-controller))
  "Returns a list of the form '(0 6 0)"
  (or (postmodern-dbversion sc)
      (setf (postmodern-dbversion sc)
            (read-from-string (cl-postgres:exec-query (active-connection)
                                                      "select dbversion from versionpm"
                                                      'first-value-row-reader)))))

;; Henrik todo is this needed
(defmacro with-controller-for-btree ((sc) &body body)
  `(let ((*sc* ,sc))
    (declare (special *sc*))
    ,@body))

(defmethod open-controller :around ((sc postmodern-store-controller) &key &allow-other-keys)
  (let ((*cache-mode* nil)) ;;disable caching during initialization
    (call-next-method)))

(defmethod flush-instance-cache ((sc postmodern-store-controller))
  "Reset the instance cache (flush object lookups). 
   Need specialized version for postmodern because it relies on
   system btrees being cached. "
  (ele-with-fast-lock ((ele::controller-instance-cache-lock sc))
    ;; cannot call-next-method because fast-locks 
    ;; are not recursive, and we need atomicity here.
    (let ((ct (setf (ele::controller-instance-cache sc)
		    (ele::make-cache-table :test 'eql))))
      ;; again, can't use cache-instance 
      ;; because non-recursive locks
      (setf (ele::get-cache (oid (slot-value sc 'root)) ct) (slot-value sc 'root)
	    (ele::get-cache (oid (persistent-slot-collection-of sc)) ct) (persistent-slot-collection-of sc)
	    (ele::get-cache (oid (slot-value sc 'instance-table)) ct) (slot-value sc 'instance-table)
	    (ele::get-cache (oid (slot-value sc 'schema-table)) ct) (slot-value sc 'schema-table)
	    (ele::get-cache (oid (slot-value sc 'index-table)) ct) (slot-value sc 'index-table)))))



(defmethod open-controller ((sc postmodern-store-controller)
			    ;; At present these three have no meaning
			    &key 
			    (recover nil)
			    (recover-fatal nil)
			    (thread t))
  (declare (ignore recover recover-fatal thread))  
  (labels ((make-system-btree (oid name &optional (class 'pm-special-btree-wrapper) (data-example t))
	     (make-instance class :sc sc :from-oid oid :table-name name
				      :key-type (data-type data-example)))
	   (init-root ()
	     (setf (slot-value sc 'root) (make-system-btree 0 "root")
		   (slot-value sc 'instance-table) (make-system-btree 1 "instances" 'pm-special-indexed-btree-wrapper 1)
		   (slot-value sc 'schema-table) (make-system-btree 3 "schemas" 'pm-special-indexed-btree-wrapper 1)
		   (slot-value sc 'index-table) (make-system-btree 4 "indices"))))
	     
    (ensure-thread-table-lock)
    (the postmodern-store-controller
      (with-connection-for-thread (sc)
        (let ((con (active-connection)))
          (assert (cl-postgres::database-open-p con ))

          (unless (version-table-exists con)
            (with-transaction (:store-controller sc)
              (create-version-table sc)))
        
          (initialize-serializer sc)
        
          (setf (persistent-slot-collection-of sc)
		(make-system-btree 2 "slots" 'pm-special-btree-wrapper (form-slot-key 777 'a-typical-slot)))

          (if (message-table-existsp con)
	      (progn
		#+ele-global-sync-cache
		(bootstrap-sync-cache con)
		(init-root))
              (with-transaction (:store-controller sc)
                  (with-controller-for-btree (sc)
                    (create-base-tables con)
		    
		    #+ele-global-sync-cache
		    (create-sync-cache-tables con)
		    #+ele-global-sync-cache
		    (bootstrap-sync-cache con)

                    (init-stored-procedures con)

                    (make-table (persistent-slot-collection-of sc))
                    (init-root)

		    (dolist (slot '(root instance-table 
				    schema-table index-table))
		      (make-table (slot-value sc slot)))

                    (create-message-table con))))
          sc)))))

(defmethod prepare-local-queries ((sc postmodern-store-controller))
  (initialize-global-queries sc))

(defmethod connection-ok-p ((sc postmodern-store-controller))
  (with-connection-for-thread (sc)
    (connection-ok-p-con (active-connection))))

(defun connection-ok-p-con (con)
  (cl-postgres:database-open-p con))

(defmethod connection-really-ok-p ((sc postmodern-store-controller))
  (connection-ok-p sc)) ;; TODO: How should this be done

(defmethod controller-status ((sc postmodern-store-controller))
  :ok) ;;TODO

(defmethod reconnect-controller ((sc postmodern-store-controller))
  (with-connection-for-thread (sc)
    (cl-postgres:reopen-database (active-connection))))

(defmethod close-controller ((sc postmodern-store-controller))
  (loop for v being the hash-values of (controller-db-table sc)
        for con = (thread-bookkeeper-connection v) do
        (if (connection-ok-p-con con)
              (cl-postgres:close-database con)))
  (setf (slot-value sc 'root) nil))

(defmethod next-oid ((sc postmodern-store-controller))
  (with-connection-for-thread (sc)
    (with-postmodern-conn ((active-connection))
      (postmodern:sequence-next 'persistent_seq))))

(defmethod next-cid ((sc postmodern-store-controller))
  (with-connection-for-thread (sc)
    (with-postmodern-conn ((active-connection))
      (postmodern:sequence-next 'class_id_seq))))

(defun deserialize-from-database (x sc &optional oids-only)
  (with-buffer-streams (other)
    (deserialize
     (elephant-memutil::buffer-write-byte-vector x other)
     sc
     oids-only)))

(defun form-slot-key (oid name)
  (declare (optimize speed))
  (let ((*print-pretty* nil))
    (concatenate 'string (princ-to-string oid) " " (symbol-name name))))

(defmethod oid->schema-id (oid (sc postmodern-store-controller))
  "For default data structures, provide a fixed mapping to class IDs based
   on the known startup order. (ugly)"
  (if (<= oid 51)
      (case oid
	(0 1) ; root
	(1 3) ; instances
	(2 1) ; slots
	(3 3) ; schemas
	(4 1) ; indices
	(50 4)   ; btree-indices of
	(51 4))  ;  instances and schemas
      (call-next-method)))

(defmethod reserved-oid-p ((sc postmodern-store-controller) oid)
  (<= oid 51))

(defmethod default-class-id (type (sc postmodern-store-controller))
  (ecase type
    (pm-btree-wrapper 1)
    (pm-dup-btree-wrapper 2)
    (pm-indexed-btree-wrapper 3)
    (pm-btree-index-wrapper 4)))

(defmethod default-class-id-type (cid (sc postmodern-store-controller))
  (case cid
    (1 'pm-btree-wrapper)
    (2 'pm-dup-btree-wrapper)
    (3 'pm-indexed-btree-wrapper)
    (4 'pm-btree-index-wrapper)))


(defmethod persistent-slot-writer ((sc postmodern-store-controller) new-value instance name)
  (with-controller-for-btree (sc)
    (with-connection-for-thread (sc)
      (setf (get-value (form-slot-key (oid instance) name) (persistent-slot-collection-of sc))
            new-value)))
  new-value)

(defmethod persistent-slot-reader ((sc postmodern-store-controller) instance name &optional oids-only)
  (declare (optimize (debug 3)) (ignore oids-only))
  (with-controller-for-btree (sc)
    (multiple-value-bind (v existsp)
        (get-value (form-slot-key (oid instance) name)
                   (persistent-slot-collection-of sc))
      (if existsp
          v
	  (slot-unbound (class-of instance) instance name)))))

(defmethod persistent-slot-boundp ((sc postmodern-store-controller) instance name)
  (with-controller-for-btree (sc)
    (with-connection-for-thread (sc)
      (existsp (form-slot-key (oid instance) name) (persistent-slot-collection-of sc)))))

(defmethod persistent-slot-makunbound ((sc postmodern-store-controller) instance name)
  (with-controller-for-btree (sc)  
    (with-connection-for-thread (sc)
      (remove-kv (form-slot-key (oid instance) name) (persistent-slot-collection-of sc))
      instance)))
