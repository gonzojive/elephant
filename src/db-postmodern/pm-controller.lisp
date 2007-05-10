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


(defun integer-to-string (int)
  (princ-to-string int))

(defclass postmodern-store-controller (store-controller)
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
          (destructuring-bind (db-type host database user password) (second (controller-spec sc))
            (assert (eq :postgresql db-type))
            (let ((con (postmodern:connect database user password host :pooled-p t)))
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

(defmacro with-conn ((con) &body body)
  `(let ((postmodern:*database* ,con))
    (declare (special postmodern:*database*))
    ,@body))

(defun version-table-exists (con)
  (with-conn (con)
    (postmodern:table-exists-p 'version)))

(defun create-version-table (sc)
  (declare (ignore sc))
  (cl-postgres:exec-query (active-connection) "create table version (dbversion varchar(20) not null)")
  (cl-postgres:exec-query (active-connection) (format nil "insert into version (dbversion) values('~a')" *elephant-code-version*)))

(defmethod database-version ((sc postmodern-store-controller))
  "Returns a list of the form '(0 6 0)"
  (or (postmodern-dbversion sc)
      (setf (postmodern-dbversion sc)
            (read-from-string (cl-postgres:exec-query (active-connection)
                                                      "select dbversion from version"
                                                      'first-value-row-reader)))))

;; Henrik todo is this needed
(defmacro with-controller-for-btree ((sc) &body body)
  `(let ((*sc* ,sc))
    (declare (special *sc*))
    ,@body))

(defmethod open-controller ((sc postmodern-store-controller)
			    ;; At present these three have no meaning
			    &key 
			    (recover nil)
			    (recover-fatal nil)
			    (thread t))
  (declare (ignore recover recover-fatal thread))
  (ensure-thread-table-lock)
  (the postmodern-store-controller
    (with-connection-for-thread (sc)
      (let ((con (active-connection))
            (make-tables nil))
        (assert (cl-postgres::database-open-p con ))

        (unless (version-table-exists con)
          (with-transaction (:store-controller sc)
            (create-version-table sc)))
        
        (initialize-serializer sc)
        
        (setf (persistent-slot-collection-of sc) (make-instance 'pm-btree :table-name "slots" :from-oid 2 :sc sc))
        (setf (key-type-of (persistent-slot-collection-of sc)) (data-type (form-slot-key 777 'a-typical-slot)))
        
        (unless (base-table-existsp con)
          (create-base-tables con)
          (init-stored-procedures con)
          (with-controller-for-btree (sc)
            (make-table (persistent-slot-collection-of sc))
            (setf make-tables t)))

        (setf (slot-value sc 'root) (make-instance 'pm-indexed-btree :sc sc :from-oid 0 :table-name "root"))
        (setf (key-type-of (slot-value sc 'root)) (data-type t))            
        
        (setf (slot-value sc 'class-root) (make-instance 'pm-indexed-btree :sc sc :from-oid 1 :table-name "classroot"))
        (setf (key-type-of (slot-value sc 'class-root)) (data-type t))

        (when make-tables
          (with-controller-for-btree (sc)
            (make-table (slot-value sc 'root))
            (make-table (slot-value sc 'class-root))))
        sc))))

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
    (with-conn ((active-connection))
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
  (with-controller-for-btree (sc)
    (multiple-value-bind (v existsp)
        (get-value (form-slot-key (oid instance) name)
                   (persistent-slot-collection-of sc))
      (when (and v (not existsp))
        (error "Debugging:Ooops existsp value was not returned."))
      (if existsp
          v
          (error  'unbound-slot :instance instance :name name)))))

(defmethod persistent-slot-boundp ((sc postmodern-store-controller) instance name)
  (with-controller-for-btree (sc)
    (with-connection-for-thread (sc)
      (existsp (form-slot-key (oid instance) name) (persistent-slot-collection-of sc)))))

(defmethod persistent-slot-makunbound ((sc postmodern-store-controller) instance name)
  (with-controller-for-btree (sc)  
    (with-connection-for-thread (sc)
      (remove-kv (form-slot-key (oid instance) name) (persistent-slot-collection-of sc))
      instance)))
