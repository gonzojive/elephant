;;; -*- Mode: Lisp; Syntax: ANSI-Common-Lisp; Base: 10 -*-
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

(in-package :db-bdb)

(declaim #-elephant-without-optimize (optimize (speed 3) (safety 1) (space 0) (debug 0)))

(defclass bdb-store-controller (store-controller)
  ((environment :type (or null pointer-void) 
		:accessor controller-environment)
   (metadata :type (or null pointer-void) :accessor controller-metadata)
   (db :type (or null pointer-void) :accessor controller-db :initform '())
   (btrees :type (or null pointer-void) :accessor controller-btrees)
   (dup-btrees :type (or null pointer-void) :accessor controller-dup-btrees)
   (indices :type (or null pointer-void) :accessor controller-indices)
   (indices-assoc :type (or null pointer-void)
		  :accessor controller-indices-assoc)
   (oid-db :type (or null pointer-void) :accessor controller-oid-db)
   (oid-seq :type (or null pointer-void) :accessor controller-oid-seq)
   (cid-seq :type (or null pointer-void) :accessor controller-cid-seq)
   (deadlock-pid :accessor controller-deadlock-pid :initform nil)
   (deadlock-detect-thread :type (or null t)
                           :accessor controller-deadlock-detect-thread
                           :initform nil))
  (:documentation "Class of objects responsible for the
book-keeping of holding DB handles, the cache, table
creation, counters, locks, the root (for garbage collection,)
et cetera."))

;;
;; Data store Registry Support
;;

(defun bdb-test-and-construct (spec)
  (if (bdb-store-spec-p spec)
      (make-instance 'bdb-store-controller :spec spec)
      (error (format nil "uninterpretable spec specifier: ~A" spec))))

(eval-when (:compile-toplevel :load-toplevel)
  (register-data-store-con-init :bdb 'bdb-test-and-construct))

(defun bdb-store-spec-p (spec)
  (and (eq (first spec) :bdb)
       (typecase (second spec)
	 (pathname t)
	 (string t)
	 (otherwise nil))))

(defmethod temp-spec ((type (eql :BDB)) spec)
  (let ((dirname (create-temp-dirname (second spec))))
    (ensure-directories-exist dirname)
    `(:BDB ,dirname)))

(defmethod delete-spec ((type (eql :BDB)) spec)
  "BDB delete spec to support automated data store management"
  (assert (eq (first spec) :BDB))
  (let ((directory (second spec)))
    (when (probe-file directory)
      (loop for file in (directory directory) do
	   (delete-file file)))
    (delete-file directory)))

(defmethod copy-spec ((type (eql :BDB)) source target)
  "BDB copy spec to support automated data store mgmt, 
   deletes target and copies source to target."
  (assert (and (eq (first source) :BDB) (eq (first target) :BDB)))
  (let ((source-dir (second source))
	(target-dir (second target)))
    (assert (probe-file source-dir))
    (delete-spec :BDB target)
    (copy-directory source-dir target-dir)))

;;
;; Store-specific transaction support
;;

(defmacro my-current-transaction (sc)
  (let ((txn-rec (gensym)))
    `(let ((,txn-rec *current-transaction*))
       (if (and ,txn-rec (eq (transaction-store ,txn-rec) ,sc))
	   (transaction-object ,txn-rec)
	   +NULL-CHAR+))))

;;
;; Open/close     
;;

(defmethod open-controller ((sc bdb-store-controller) &key (recover t)
			    (recover-fatal nil) (thread t) (register nil) 
			    (deadlock-detect t)
			    (cache-size elephant::*berkeley-db-cachesize*)
			    (max-locks elephant::*berkeley-db-max-locks*)
			    (max-objects elephant::*berkeley-db-max-objects*)
			    (max-transactions elephant::*berkeley-db-max-transactions*)
			    (mvcc elephant::*default-mvcc*)
			    error-log)
  (let ((env (db-env-create))
	(new-p (not (probe-file (elephant-db-path (second (controller-spec sc)))))))
    (setf (controller-environment sc) env)
    (db-env-set-flags env 0 :auto-commit t)
    (db-env-set-cachesize env 0 cache-size 1)
    (db-env-set-max-locks env max-locks)
    (db-env-set-max-objects env max-objects)
    (db-env-set-max-transactions env max-transactions)
    (db-env-set-timeout env 100000 :set-transaction-timeout t)
    (db-env-set-timeout env 100000 :set-lock-timeout t)
    (db-env-open env (namestring (second (controller-spec sc)))
		 :create t :init-rep nil :init-mpool t :thread thread
		 :init-lock t :init-log t :init-txn t :register register
		 :recover recover :recover-fatal recover-fatal)
    (when error-log
      (db-env-set-error-file env error-log))
    (let ((metadata (db-create env))
	  (db (db-create env))
	  (btrees (db-create env))
	  (dup-btrees (db-create env))
	  (indices (db-create env))
	  (indices-assoc (db-create env)))

      ;; Open metadata database
      (setf (controller-metadata sc) metadata)
      (db-open metadata :file "%ELEPHANT" :database "%METADATA" 
	       :auto-commit t :type DB-BTREE :create t :thread thread
	       :multiversion mvcc)


      ;; Establish database version if new
      (when new-p (set-database-version sc))

      ;; Auto upgrade 0 9 2 dbs
      (when (equal (database-version sc) '(0 9 2))
	(set-database-version sc))
		 
      ;; Set the controller-database-version number
      (destructuring-bind (maj min inc) (database-version sc)
	(setf (controller-database-version sc)
	      (+ (* 100 maj)
		 (* 10 min)
		 inc)))

      ;; Initialize serializer so we can load proper sorting C function
      ;; based on serializer type
      (initialize-serializer sc)

      ;; Open slot-value data store
      (setf (controller-db sc) db)
      (db-open db :file "%ELEPHANT" :database "%ELEPHANTDB" 
	       :auto-commit t :type DB-BTREE :create t :thread thread
	       :read-uncommitted t :multiversion mvcc)

      ;; Standard btrees
      (setf (controller-btrees sc) btrees)
      (db-bdb::db-set-lisp-compare btrees (controller-serializer-version sc))
      (db-open btrees :file "%ELEPHANT" :database "%ELEPHANTBTREES" 
	       :auto-commit t :type DB-BTREE :create t :thread thread
	       :read-uncommitted t :multiversion mvcc)

      ;; Indexed btrees
      (setf (controller-indices sc) indices)
      (db-bdb::db-set-lisp-compare indices (controller-serializer-version sc))
      (db-bdb::db-set-lisp-dup-key-compare indices (controller-serializer-version sc))
      (db-set-flags indices :dup-sort t)
      (db-open indices :file "%ELEPHANT" :database "%ELEPHANTINDICES" 
 	       :auto-commit t :type DB-BTREE :create t :thread thread
 	       :read-uncommitted t :multiversion mvcc)
      
      (setf (controller-indices-assoc sc) indices-assoc)
      (db-bdb::db-set-lisp-compare indices-assoc (controller-serializer-version sc))
      (db-bdb::db-set-lisp-dup-key-compare indices-assoc (controller-serializer-version sc))
      (db-set-flags indices-assoc :dup-sort t)
      (db-open indices-assoc :file "%ELEPHANT" :database "%ELEPHANTINDICES" 
	       :auto-commit t :type DB-UNKNOWN :thread thread
	       :read-uncommitted t :multiversion mvcc)
      (db-bdb::db-fake-associate btrees indices-assoc :auto-commit t)
      

      ;; Duplicated btrees
      (setf (controller-dup-btrees sc) dup-btrees)
      (db-bdb::db-set-lisp-compare dup-btrees (controller-serializer-version sc))
      (db-bdb::db-set-lisp-dup-compare dup-btrees (controller-serializer-version sc))
      (db-set-flags dup-btrees :dup-sort t)
      (db-open dup-btrees :file "%ELEPHANTDUP" :database "%ELEPHANTDUPS"
	       :auto-commit t :type DB-BTREE :create t :thread thread
	       :read-uncommitted t :multiversion mvcc)
     
      ;; OIDs
      (let ((db (db-create env)))
	(setf (controller-oid-db sc) db)
	(db-open db :file "%ELEPHANTOID" :database "%ELEPHANTOID" 
		 :auto-commit t :type DB-BTREE :create t :thread thread
		 :multiversion mvcc)
	(let ((oid-seq (db-sequence-create db)))
	  (db-sequence-set-cachesize oid-seq 100)
	  (db-sequence-set-flags oid-seq :seq-inc t :seq-wrap t)
	  (db-sequence-set-range oid-seq 0 most-positive-fixnum)
	  (db-sequence-initial-value oid-seq 0)
	  (db-sequence-open oid-seq "%ELEPHANTOID" :create t :thread t)
	  (setf (controller-oid-seq sc) oid-seq))

	(let ((cid-seq (db-sequence-create db)))
	  (db-sequence-set-cachesize cid-seq 100)
	  (db-sequence-set-flags cid-seq :seq-inc t :seq-wrap t)
	  (db-sequence-set-range cid-seq 0 most-positive-fixnum)
	  (db-sequence-initial-value cid-seq 5)
	  (db-sequence-open cid-seq "%ELEPHANTOID" :create t :thread t)
	  (setf (controller-cid-seq sc) cid-seq)))

      (with-transaction (:store-controller sc)
	(setf (slot-value sc 'root)
	      (make-instance 'bdb-btree :from-oid -1 :sc sc))

	(setf (slot-value sc 'index-table)
	      (make-instance 'bdb-btree :from-oid -2 :sc sc))


	(setf (slot-value sc 'instance-table)
	      (if (or new-p (elephant::prior-version-p (database-version sc) '(0 9 1)))
		  ;; When opening the DB equal or prior to 0.9.1, always get the indices initialized,
		  ;; regardless of the indices is initialized before.
		  ;; Even the indices is serialized before(in the case that open the <=0.9.1 DB file for the second time),
		  ;; it still can not be unserialized here, because its persistent object's oid
                  ;; does not match the hardcoded one in oid->schema-id, which only work on the DB file
                  ;; created in release > 0.9.1. 
		  (make-instance 'bdb-indexed-btree :from-oid -3 :sc sc :indices (make-hash-table))
		  (make-instance 'bdb-indexed-btree :from-oid -3 :sc sc)))

	(setf (slot-value sc 'schema-table)
	      (if (or new-p (elephant::prior-version-p (database-version sc) '(0 9 1)))
		  (make-instance 'bdb-indexed-btree :from-oid -4 :sc sc :indices (make-hash-table))
		  (make-instance 'bdb-indexed-btree :from-oid -4 :sc sc))))

      (when deadlock-detect
        (start-deadlock-detector sc))

      sc)))

(defun elephant-db-path (directory)
  (ctypecase directory
    (pathname
     (merge-pathnames directory (make-pathname :name "%ELEPHANT")))
    ((or cons (vector character) (vector nil) base-string (member :wild nil))
     (make-pathname :directory directory :name "%ELEPHANT"))))

(defmethod close-controller ((sc bdb-store-controller))
  (when (slot-value sc 'root)
    (stop-deadlock-detector sc)
    ;; no root
    (setf (slot-value sc 'index-table) nil)
    (setf (slot-value sc 'schema-table) nil)
    (setf (slot-value sc 'instance-table) nil)
    (setf (slot-value sc 'root) nil)
    ;; clean instance cache
    (flush-instance-cache sc)
    ;; close handles / environment
    (db-sequence-close (controller-cid-seq sc))
    (setf (controller-cid-seq sc) nil)
    (db-sequence-close (controller-oid-seq sc))
    (setf (controller-oid-seq sc) nil)
    (db-close (controller-oid-db sc))
    (setf (controller-oid-db sc) nil)
    (db-close (controller-indices-assoc sc))
    (setf (controller-indices-assoc sc) nil)
    (db-close (controller-indices sc))
    (setf (controller-indices sc) nil)
    (db-close (controller-dup-btrees sc))
    (setf (controller-dup-btrees sc) nil)
    (db-close (controller-btrees sc))
    (setf (controller-btrees sc) nil)
    (db-close (controller-db sc))
    (setf (controller-db sc) nil)
    (db-close (controller-metadata sc))
    (setf (controller-metadata sc) nil)
    ;; XXX if any of the above fails the environment doesn't get closed.
    ;; Close it in this case nevertheless via UNWIND-PROTECT?
    (db-env-close (controller-environment sc))
    (setf (controller-environment sc) nil)
    nil))

(defmethod next-oid ((sc bdb-store-controller))
  "Get the next OID."
  (declare (type bdb-store-controller sc))
  (db-sequence-get-fixnum (controller-oid-seq sc) 1 :transaction +NULL-VOID+
			  :txn-nosync t))

(defmethod next-cid ((sc bdb-store-controller))
  "Get the next class id"
  (declare (type bdb-store-controller sc))
  (db-sequence-get-fixnum (controller-cid-seq sc) 1 :transaction +NULL-VOID+
			  :txn-nosync t))

(defmethod oid->schema-id (oid (sc bdb-store-controller))
  "For default data structures, provide a fixed mapping to class IDs based
   on the known startup order.  It's ugly, it's sad, but it works."
  (if (< oid 2)
      (case oid
	(0 4)
	(1 4)
	(-1 1)
	(-2 1)
	(-3 3)
	(-4 3))
      (call-next-method)))

(defmethod default-class-id (type (sc bdb-store-controller))
  (ecase type
    (bdb-btree 1)
    (bdb-dup-btree 2)
    (bdb-indexed-btree 3)
    (bdb-btree-index 4)))

(defmethod default-class-id-type (cid (sc bdb-store-controller))
  (case cid
    (1 'bdb-btree)
    (2 'bdb-dup-btree)
    (3 'bdb-indexed-btree)
    (4 'bdb-btree-index)))

(defmethod reserved-oid-p ((sc bdb-store-controller) oid)
  (< oid 2))

;;
;; Store the database version
;;
;; For BDB this can be in a file; different data stores may require a different approach.

(defmethod database-version ((sc bdb-store-controller))
  "Elephant protocol to provide the version tag or nil if unmarked"
  (with-buffer-streams (key val)
    (serialize-database-version-key key)
    (let ((buf (db-get-key-buffered (controller-metadata sc)
				    key val
				    :transaction +NULL-VOID+)))
      (if buf (deserialize-database-version-value buf)
	  nil))))

(defun set-database-version (sc)
  "Internal use when creating new database"
  (with-buffer-streams (key val)
    (serialize-database-version-key key)
    (serialize-database-version-value *elephant-code-version* val)
    (db-put-buffered (controller-metadata sc)
		     key val
		     :transaction +NULL-VOID+)
    *elephant-code-version*))

;; (defmethod old-database-version ((sc bdb-store-controller))
;;    "A version determination for a given store
;;    controller that is independant of the serializer as the
;;    serializer is dispatched based on the code version which is a
;;    list of the form '(0 6 0)"
;;   (let ((version (elephant::controller-version-cached sc)))
;;     (if version version
;; 	(let ((path (make-pathname :name "VERSION" :defaults (second (controller-spec sc)))))
;; 	  (if (probe-file path)
;; 	      (with-open-file (stream path :direction :input)
;; 		(setf (elephant::controller-version-cached sc) (read stream)))
;; 	      (with-open-file (stream path :direction :output)
;; 		(setf (elephant::controller-version-cached sc)
;; 		      (write *elephant-code-version* :stream stream))))))))

;;
;; Automated Deadlock Support
;;

(defparameter *deadlock-type-alist*
  `((:default . (,DB_LOCK_DEFAULT . nil))
    (:oldest . (,DB_LOCK_OLDEST . "o"))
    (:youngest . (,DB_LOCK_YOUNGEST . "y"))
    (:timeout . (,DB_LOCK_EXPIRE . "e"))
    (:most . (,DB_LOCK_MAXLOCKS . "m"))
    (:least . (,DB_LOCK_MINLOCKS . "n"))))

(defun lookup-deadlock-type (typestring)
  "Translate a Lisp deadlock abort type into a BDB constant and
a db_deadlock parameter."
  (let ((result (cdr (assoc typestring *deadlock-type-alist*))))
    (unless result
      (error "Unrecognized deadlock type '~A'" typestring))
    (values (car result) (cdr result))))

(defmethod start-deadlock-detector ((ctrl bdb-store-controller) &key (type :default) (time :on-conflict)
                                                                     (log nil) (external-process-p nil))
  "Start the deadlock detector. TYPE specifies which locks should be aborted (see DB-BDB::*DEADLOCK-TYPE-ALIST*). TIME is either :ON-CONFLICT (the default, recommended) or a positive number. LOG must be either NIL (when using on-conflict deadlock detection), a stream (when using the threaded interval checker) or a string (when using the external interval
checker db_deadlock)."
  (unless (typep log '(or stream string null))
    (error "LOG must be a stream, string or NIL."))
  (let ((env (controller-environment ctrl))
        (%type (lookup-deadlock-type type)))
    (cond
      ((eq time :on-conflict)
       (when log
         (warn "LOG argument can't be used with on-conflict deadlock detection."))
       (when external-process-p
         (warn "EXTERNAL-PROCESS-P argument is meaningless with on-conflict deadlock detection."))
       (db-env-set-lock-detect env %type)
       t)
      ((and (typep time 'number) external-process-p)
       (assert (typep log '(or string null)))
       (let ((process-handle 
               (launch-background-program 
                 (second (controller-spec ctrl))
                 (namestring 
                   (make-pathname :defaults (get-user-configuration-parameter :berkeley-db-deadlock)))
                 :args `("-a" ,(let ((arg (nth-value 1 (lookup-deadlock-type type))))
                                 (if arg arg (error "Can't propagate deadlock abortion type ~S to db_deadlock." type)))
                         "-t" ,(format nil "~D" time)
                         ,@(when log (list "-L" (format nil "~A" log)))))))
         (setf (controller-deadlock-pid ctrl) process-handle)))
      ((typep time 'number)
       (assert (typep log '(or stream null)))
       (unless (find-package :bordeaux-threads)
         (asdf:oos 'asdf:load-op :bordeaux-threads))
       (assert (plusp time))
       (warn "The interval deadlock detector might not work reliably in high-concurrency situations.")
       (let ((thread (funcall (find-symbol "MAKE-THREAD" :bordeaux-threads)
                              (lambda ()
                                (loop do (progn
                                           (let ((aborted (db-env-lock-detect env %type)))
                                             (and log (not (zerop aborted))
                                               (format log "INFO: Aborted ~D transactions due to deadlock.~%" aborted)))
                                           (sleep time))))
                                           :name (format nil "Deadlock detector for ~A" ctrl))))
         (setf (controller-deadlock-detect-thread ctrl) thread)))
      (t (error "Invalid deadlock activation time specifier ~S -- must be either :ON-CONFLICT or a positive number." time)))))
			
(defmethod stop-deadlock-detector ((ctrl bdb-store-controller))
  (let ((thread (controller-deadlock-detect-thread ctrl))
        (pid (controller-deadlock-pid ctrl)))
    (when (and thread pid)
      (warn "Both external and built-in threaded deadlock detector are running?!"))
    (when (and thread (find-package :bordeaux-threads))
      (handler-case (funcall (find-symbol "DESTROY-THREAD" :bordeaux-threads) thread)
        (error () (warn "Couldn't stop deadlock detector thread -- who shut it down?")))
      (setf (controller-deadlock-detect-thread ctrl) nil))
    (when pid
      (kill-background-program pid))
    t))

;;
;; Enable program-based checkpointing
;;
  
(defmethod checkpoint ((sc bdb-store-controller) &key force (time 0) (log-size 0))
  "Forces a checkpoint of the db and flushes the memory pool to disk.
   Use keywords ':force t' to write the checkpoint under any
   condition, ':time N' to checkpoint based on if the number of
   minutes since the last checkpoint is greater than time and
   ':log-size N' to checkpoint if the data written to the log is
   greater than N kilobytes"
  (db-env-txn-checkpoint (controller-environment sc) log-size time :force force))

;;
;; Take advantage of release 4.4's compact storage feature.  Feature of BDB only
;;

(defmethod optimize-layout ((ctrl bdb-store-controller) &key start-key stop-key 
			    (freelist-only t) (free-space nil)
			    &allow-other-keys)
  "Tell the data store to optimize and reclaim storage between key values"
  (with-buffer-streams (start stop end)
    (if (null start-key)
	(progn 
	  (db-compact (controller-indices ctrl) nil nil end :transaction +NULL-VOID+)
	  (db-compact (controller-db ctrl) nil nil end :transaction +NULL-VOID+)
	  (db-compact (controller-btrees ctrl) nil nil end :transaction +NULL-VOID+))
	(progn
	  (serialize start-key start ctrl)
	  (when stop-key (serialize stop-key stop ctrl))
	  (db-compact (controller-indices ctrl) start
		      (when stop-key stop) end
		      :freelist-only freelist-only
		      :free-space free-space
		      :transaction +NULL-VOID+)
	  (db-compact (controller-db ctrl) nil
		      (when stop-key stop) end
		      :freelist-only freelist-only
		      :free-space free-space
		      :transaction +NULL-VOID+)
	  (db-compact (controller-btrees ctrl) nil
		      (when stop-key stop) end
		      :freelist-only freelist-only
		      :free-space free-space
		      :transaction +NULL-VOID+)))
    (values (deserialize end ctrl))))

