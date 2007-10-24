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
  (make-instance 'pm-btree :sc sc))

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

(defmethod controller-connection-for-thread ((sc postmodern-store-controller))
  (elephant::ele-with-lock (*thread-table-lock*)
    (let ((bookkeeper (gethash (thread-hash) (controller-db-table sc))))
      (if bookkeeper
          (thread-bookkeeper-connection bookkeeper)
          (destructuring-bind (db-type host database user password &key port) (second (controller-spec sc))
            (assert (eq :postgresql db-type))
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
    (declare (special *connection*))
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

(defmethod open-controller ((sc postmodern-store-controller)
			    ;; At present these three have no meaning
			    &key 
			    (recover nil)
			    (recover-fatal nil)
			    (thread t))
  (declare (ignore recover recover-fatal thread))  
  (flet ((init-root ()
           (setf (slot-value sc 'root) (make-instance 'pm-indexed-btree :sc sc :from-oid 0 :table-name "root"))
           (setf (key-type-of (slot-value sc 'root)) (data-type t))
           (setf (slot-value sc 'class-root) (make-instance 'pm-indexed-btree :sc sc :from-oid 1 :table-name "classroot"))
           (setf (key-type-of (slot-value sc 'class-root)) (data-type t))))
    (ensure-thread-table-lock)
    (the postmodern-store-controller
      (with-connection-for-thread (sc)
        (let ((con (active-connection)))
          (assert (cl-postgres::database-open-p con ))

          (unless (version-table-exists con)
            (with-transaction (:store-controller sc)
              (create-version-table sc)))
        
          (initialize-serializer sc)
        
          (setf (persistent-slot-collection-of sc) (make-instance 'pm-btree :table-name "slots" :from-oid 2 :sc sc))
          (setf (key-type-of (persistent-slot-collection-of sc)) (data-type (form-slot-key 777 'a-typical-slot)))

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

                    (make-table (slot-value sc 'root))
                    (make-table (slot-value sc 'class-root))
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

(defun deserialize-from-database (x sc)
  (with-buffer-streams (other)
    (deserialize
     (elephant-memutil::buffer-write-byte-vector x other)
     sc)))

(defun form-slot-key (oid name)
  (with-output-to-string (x)
    (princ oid x)
    (write-char #\Space x)
    (princ name x)))

(defmethod persistent-slot-writer ((sc postmodern-store-controller) new-value instance name)
  (with-controller-for-btree (sc)
    (with-connection-for-thread (sc)
      (setf (get-value (form-slot-key (oid instance) name) (persistent-slot-collection-of sc))
            new-value)))
  new-value)

(defmethod persistent-slot-reader ((sc postmodern-store-controller) instance name)
  (declare (optimize (debug 3)))
  (with-controller-for-btree (sc)
    (multiple-value-bind (v existsp)
        (get-value (form-slot-key (oid instance) name)
                   (persistent-slot-collection-of sc))
      (if existsp
          v
          (error 'unbound-slot :instance instance :name name)))))

(defmethod persistent-slot-boundp ((sc postmodern-store-controller) instance name)
  (with-controller-for-btree (sc)
    (with-connection-for-thread (sc)
      (existsp (form-slot-key (oid instance) name) (persistent-slot-collection-of sc)))))

(defmethod persistent-slot-makunbound ((sc postmodern-store-controller) instance name)
  (with-controller-for-btree (sc)  
    (with-connection-for-thread (sc)
      (remove-kv (form-slot-key (oid instance) name) (persistent-slot-collection-of sc))
      instance)))
