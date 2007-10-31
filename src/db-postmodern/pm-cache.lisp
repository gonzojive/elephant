;;; -*- Mode: Lisp; Syntax: ANSI-Common-Lisp; Base: 10 -*-
;;;
;;; pm-cache.lisp -- caching implementation
;;; 
;;; part of
;;;
;;; Elephant: an object-oriented database for Common Lisp
;;;
;;; Copyright (c) 2007 Henrik Hjelte <hhjelte@common-lisp.net>
;;; Copyright (c) 2007 Alex Mizrahi <alex.mizrahi@gmail.com>
;;;
;;; Elephant users are granted the rights to distribute and use this software
;;; as governed by the terms of the Lisp Lesser GNU Public License
;;; (http://opensource.franz.com/preamble.html), also known as the LLGPL.
;;;

;; pluggable implementation of value caching.
;; only btree value caching implemented by now (indices don't get cached).

;; cache should be enabled setting db-postmodern::*cache-mode* to one of available options.

;; two options implemented by now:

;; :per-transaction-cache -- performs caching only inside one transaction.
;; cache is not preserved between transactions. 
;; this mode has minimal effect on properties on application -- it's unlikely to produce any
;; wrong interference, but acceleration from this type of cache is not great too.
;; it's possible to enable :per-transaction-cache only for some transactions.

;; :global-sync-cache -- caches data accross boundaries of transactions.
;; this caching is much more efficient, but its more likely to introduce some interference.
;; (caching is designed to be safe, and tests prove that, but as in any complex product
;; elephant/backend can have bugs..)
;; all changes to btree values are logged into special database table, so it's oriented only
;; on read-instensive applications.

;; it's not possible to switch between global-sync-cache and other modes in "automatic" way.
;; it's possible to have global-sync-cache only when code is compiled with :ele-global-sync-cache
;; feature, and this feature has to be enabled when creating the store, as it requires some
;; additional definitions and instrumentation of btree update stored procedure.
;; (so it's not possible to use global-sync-cache with store that is created without this feature).
;; however, feature itself does not enable sync cache, it's only gets enabled when you set *cache-mode*
;; variable to :global-sync-cache for the first time.

;; once global-sync-cache is enabled it cannot be disabled setting variable back. also, it should be
;; enabled on all clients using the store -- because otherwise changes will not properly propagate to
;; other clients using cache.

;; it's possible to do initial import with cache disabled (since caching logs all changes to database,
;; reducing performance), and then enable cache for actual database clients.
;; to disable global-sync-cache safely, you should shutdown all accessing clients, connect to store and
;; execute (db-postmodern:disable-sync-cache-trigger), 
;; having an active connection (in store transaction, for example)

;; when global-sync-cache is enabled, database requires maintenance -- stale log entries should be removed
;; from it. with default settings cache gets stale when it's not used for 10 minutes, so it's safe to remove 
;; update and transaction log entries older than that:
#|
"DELETE FROM transaction_log WHERE commit_time < (extract(epoch from current_timestamp) - 610);
 DELETE FROM update_log WHERE txn_id NOT IN (SELECT txn_id FROM transaction_log);
 "|#
;; for busy environments this values probably should be tuned.

;; caching is optimized for single executor thread 
;; (it's assumed you have multiple clients with one thread in each)
;; if you have transactions in more than one thread simultaneously, multiple independent cache instances 
;; will be created.

;; in order to implement robust behaviour, in some cases implementation clears whole cache,
;;  assuming it's stale or broken. this cases are:
;; * last update is older than max-resync-time, which defaults to 10 minutes. 
;; * too many enties arrived since last update -- defaults to 150 entries.
;; * transaction is aborted (in this case cache is assumed broken).

(in-package :db-postmodern)

(defgeneric cache-get-value (cache id key))
(defgeneric cache-set-value (cache id key value))
(defgeneric cache-clear-value (cache id key))
(defgeneric cache-clear-all (cache))

(defgeneric value-cache-commit (cache))

;; no-ops for null-cache and unsupported types
(defmethod cache-get-value (cache id key) nil)
(defmethod cache-set-value (cache id key value))
(defmethod cache-clear-value (cache id key))
(defmethod cache-clear-all (cache))
(defmethod value-cache-commit (cache))

(defmethod cache-get-value (cache (bt btree) key)
  (cache-get-value cache (oid bt) key))

(defmethod cache-set-value (cache (bt btree) key value)
  (cache-set-value cache (oid bt) key value))

(defmethod cache-clear-value (cache (bt btree) key)
  (cache-clear-value cache (oid bt) key))

;; hash-table cache
(defmethod cache-get-value ((cache hash-table) (id integer) key)
  (gethash (cons id key) cache))

(defmethod cache-set-value ((cache hash-table) (id integer) key value)
  (setf (gethash (cons id key) cache) value))

(defmethod cache-clear-value ((cache hash-table) (id integer) key)
  (remhash (cons id key) cache))

(defmethod cache-clear-all ((cache hash-table))
  (clrhash cache))

(defun make-value-cache (store-controller)
  (case *cache-mode*
    (:per-transaction-cache (make-hash-table :test 'equal))
    #+ele-global-sync-cache
    (:global-sync-cache (make-total-sync-cache-wrapper store-controller))))

(defgeneric ensure-cache-up-to-date (cache))

(defclass chained-cache ()
  ((parent-cache :accessor parent-cache :initarg :parent-cache)))

(defmethod cache-get-value ((w chained-cache) id key)
  (cache-get-value (parent-cache w) id key))

(defmethod cache-set-value ((w chained-cache) id key value)
  (cache-set-value (parent-cache w) id key value))

(defmethod cache-clear-value ((w chained-cache) id key)
  (cache-clear-value (parent-cache w) id key))

(defmethod cache-clear-all ((w chained-cache))
  (cache-clear-all (parent-cache w)))

(defmethod value-cache-commit ((w chained-cache))
  (value-cache-commit (parent-cache w)))

(defclass cache-update-wrapper (chained-cache)
  ((updated :accessor cache-updated :initform nil)))

(defmethod ensure-cache-up-to-date ((w cache-update-wrapper))
  (unless (cache-updated w)
    (ensure-cache-up-to-date (parent-cache w))
    (setf (cache-updated w) t)))

(defmethod value-cache-commit :around ((w cache-update-wrapper))
  (when (cache-updated w) ;;only call next method if there were some activities
    (call-next-method)))

(defmethod cache-get-value :before ((w cache-update-wrapper) id key)
  (ensure-cache-up-to-date w))

(defmethod cache-set-value :before ((w cache-update-wrapper) id key value)
  (ensure-cache-up-to-date w))

(defmethod cache-clear-value :before ((w cache-update-wrapper) id key)
  (ensure-cache-up-to-date w))

(defmethod value-cache-commit :after ((w cache-update-wrapper))
  (return-cache-for-controller (parent-cache w))
  (setf (parent-cache w) nil))

(defvar *default-max-resync-time* 600)
(defvar *default-max-cache-updates* 150)

(defclass pm-synchonized-cache (chained-cache)
  ((last-update :accessor last-update-of :initform 0)
   (limit :accessor max-cache-updates :initform *default-max-cache-updates* :initarg :max-cache-updates)
   (max-resync-time :accessor max-resync-time :initform *default-max-resync-time* :initarg :max-resync-time)
   (sc :reader store-controller-of :initarg :sc)))

(defvar *sync-cache-trigger-enabled-by-process* nil)
(defvar *sync-cache-trigger-enabled-by-process-lock* (elephant-utils::ele-make-lock))

(defun enable-sync-cache-trigger ()
  (elephant::ele-with-lock (*sync-cache-trigger-enabled-by-process-lock*)
    (unless *sync-cache-trigger-enabled-by-process*
      (let ((tries 10))
        (block tuple-concurrently-updated-guard
          (loop
             (handler-case
                 (prog1
                     (cl-postgres:exec-query 
                      (active-connection)
                      (concatenate 
                       'string
                       "CREATE OR REPLACE FUNCTION notify_btree_update (the_id integer, the_key text) RETURNS void AS $$
BEGIN 
INSERT INTO update_log (txn_id, id, key) VALUES (currval('txn_id'), the_id, the_key);"
                       #+nil "EXCEPTION WHEN object_not_in_prerequisite_state THEN" 
                       "END;
$$ LANGUAGE plpgsql;"))
                   (setf *sync-cache-trigger-enabled-by-process* t)
                   (return-from tuple-concurrently-updated-guard))
               (cl-postgres:database-error (e)
                 ;; Postgres sometimes shows this error, possibly when creating the procedure at the same time.
                 ;; ERROR-CODE: "XX000"
                 ;; MESSAGE: "tuple concurrently updated"
                 (if (plusp (decf tries))
                     (sleep 0.02)
                     (signal e))))))))))


(defun disable-sync-cache-trigger ()
  (elephant::ele-with-lock (*sync-cache-trigger-enabled-by-process-lock*)
    (setf *sync-cache-trigger-enabled-by-process* nil))
  (cl-postgres:exec-query 
   (active-connection)
   "CREATE OR REPLACE FUNCTION notify_btree_update (id integer, the_key text) RETURNS void AS $$
BEGIN END;
$$ LANGUAGE plpgsql;"))

(defmethod initialize-instance :after ((cache pm-synchonized-cache) &key sc &allow-other-keys)
  (with-connection-for-thread (sc)
    (enable-sync-cache-trigger)))

(defmethod ensure-cache-up-to-date ((cache pm-synchonized-cache))
  (with-connection-for-thread ((store-controller-of cache))
    
    (cl-postgres:exec-query (active-connection) "SELECT nextval('txn_id')")

    (let* ((transaction-start-time
	    (cl-postgres:exec-query (active-connection)  "SELECT EXTRACT(epoch FROM current_timestamp)"
				    'first-value-row-reader))
	   (last-commited-time
	    (cl-postgres:exec-query (active-connection)  "SELECT MAX(commit_time) FROM transaction_log"
				    'first-value-row-reader)))
      
      (when (plusp (last-update-of cache))
	;;our cache isn't fresh, need to pull updates
	(if (> (- transaction-start-time (last-update-of cache)) 
	       (max-resync-time cache))

	    (cache-clear-all (parent-cache cache)) ;;cache is stale
	    
	    (let ((rows (cl-postgres:exec-query 
			 (active-connection)
			 (format nil
"SELECT commit_time, id, key FROM transaction_log 
INNER JOIN update_log ON  transaction_log.txn_id = update_log.txn_id
WHERE commit_time > ~f LIMIT ~a"
			  (last-update-of cache) (max-cache-updates cache))
		 'cl-postgres:list-row-reader)))
	      (if (>= (length rows) (max-cache-updates cache)) 
		  (cache-clear-all (parent-cache cache)) ;; too much updates
		  (loop for (utime id key) in rows
			do (cache-clear-value (parent-cache cache) id key))))))
      (setf (last-update-of cache) 
	    (if (and (not (eq last-commited-time :null))
		     (< (- transaction-start-time last-commited-time)
					   (max-resync-time cache)))
		;; choose last-commited-time if it exists and isn't stale
		last-commited-time 
		;; otherwise a bit of hackery -- equidistant from "current moment" and 
		;; resync time bound, should be fine for sufficiently large max-resync-time
		(- transaction-start-time (/ (max-resync-time cache) 2)))))))


      

(defmethod value-cache-commit ((cache pm-synchonized-cache))
  (with-connection-for-thread ((store-controller-of cache))
    (dolist (query '("LOCK TABLE transaction_log IN EXCLUSIVE MODE"
		     "INSERT INTO transaction_log (txn_id, commit_time) VALUES 
(currval('txn_id'), EXTRACT (epoch FROM timeofday()::timestamp));"))
      (cl-postgres:exec-query (active-connection)
			      query
			      'cl-postgres:ignore-row-reader))))
			    

(defvar *caches-for-controllers* (make-hash-table))
(defvar *cache-for-controller-lock* (elephant-utils::ele-make-lock))

(defun make-backend-cache ()
   (make-hash-table :test 'equal)
  #+nil  (make-hash-table :test 'equal :weakness :key-and-value))

(defun get-cache-for-controller (sc)
  (or 
   (elephant::ele-with-lock (*cache-for-controller-lock*)
				 (pop (gethash sc *caches-for-controllers*)))
   (make-instance 'pm-synchonized-cache 
		  :sc sc 
		  :parent-cache (make-backend-cache))))

(defun return-cache-for-controller (cache)
  (let ((sc (store-controller-of cache)))
    (elephant::ele-with-lock (*cache-for-controller-lock*)
      (push cache (gethash sc *caches-for-controllers*)))))

(defun make-total-sync-cache-wrapper (sc)
    (make-instance 'cache-update-wrapper 
		   :parent-cache (get-cache-for-controller sc)))
