;;; -*- Mode: Lisp; Syntax: ANSI-Common-Lisp; Base: 10 -*-
;;;
;;; utils.lisp -- utility functions
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Versioning Support

(defvar *elephant-code-version* '(0 9 2)
  "The current database version supported by the code base")

(defvar *elephant-unmarked-code-version* '(0 6 0)
  "If a database is opened with existing data but no version then
   we assume it's version 0.6.0")

(defvar *elephant-properties-label* 'elephant::*database-properties*
  "This is the symbol used to store properties associated with the
   database in the controller-root through the new properties interface.
   Users attempting to directly write this variable will run into an
   error")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; General support for user configurable parameters

(defvar *user-configurable-parameters*
  '((:berkeley-db-map-degree2 *map-using-degree2*)
    (:berkeley-db-cachesize *berkeley-db-cachesize*)
    (:berkeley-db-max-locks *berkeley-db-max-locks*)
    (:berkeley-db-max-objects *berkeley-db-max-objects*)
    (:berkeley-db-version *bdb-version*)
    (:berkeley-db-mvcc *default-mvcc*)
    (:enable-multi-store-indexing *enable-multi-store-indexing*)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; System-wide configuration options

(defvar *enable-multi-store-indexing* nil
  "Allow indexed class instances to reside in more than one
   data store.  Inhibits various checks and errors and allows
   the class to cache multiple index controllers.  Set by
   a user configurable parameter.")

(defconstant +none+ 0
  "Cache specification is ignored")

(defconstant +txn+ 1
  "Cache values for subsequent reads in a transaction.  Writes
   are write-through so any indices get updated.  This is an
   object-wide policy for all cached slot types")

(defconstant +checkout+ 2
  "An object can be checked out and all cached slots are 
   manipulated entirely in memory.  This provides protection
   only for writes.  An object cannot be written without being
   checked out.  If someone tries to check out an object that
   is checked out, an error is flagged.  This enables critical
   sections to be defined to provide per-process isolation for
   short-term operations.  For long term checkouts, the user 
   will need to provide any needed thread isolation.  It would
   be easy to add multiple-process isolation by maintaining an
   owned state in the in-memory object so a process knew it was
   the one that did the check out and any write operations would
   assert an error.")

(defparameter *cached-instance-default-mode* +none+
   "Determines the global default for cache mode on
    instances.  Override with instance initarg :cache-mode")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Optimization parameters 

(defvar *circularity-initial-hash-size* 50
  "This is the default size of the circularity cache used in the serializer")

(defparameter *map-using-degree2* t
  "This parameter enables an optimization for the Berkeley DB data store
   that allows a map operator to walk over a btree without locking all
   read data, it only locks written objects and the current object")

(defvar *berkeley-db-cachesize* 10485760
  "This parameter controls the size of the berkeley db data store page
   cache.  This parameter can be increased by to 4GB on 32-bit machines
   and much larger on other machines.  Using the db_stat utility to identify
   cache hit frequency on your application is a good way to tune this number.
   The default is 20 megabytes specified in bytes.  If you need to specify
   Gigbyte + cache sizes, talk to the developers!  This is ignored for
   existing databases that were created with different parameters")

(defvar *berkeley-db-max-locks* 2000
  "Controls the number of locks allocated for berkeley db.  It helps to increase
   it to enable transactions to be larger.  Typically for bulk loads or large
   transactions.")

(defvar *berkeley-db-max-objects* 2000
  "Controls the number of locks allocated for berkeley db.  It helps to increase
   it to enable transactions to be larger.  Typically for bulk loads or large
   transactions.  See berkeley db docs 'Configuring Locking: sizing the system' 
   for more detail")

(defvar *default-mvcc* nil
  "Determines whether a BDB database is enabled for multiple version concurrency
   controller (DB_MULTIVERSION) and transactions are DB_SNAPSHOT by default.
   These can be overridden on a per-transaction basis using the :mvcc argument
   to open-store and :snapshot to with-transaction.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Legacy Thread-local specials

#+(or cmu sbcl allegro)
(defvar *resourced-byte-spec* (byte 32 0)
  "Byte specs on CMUCL, SBCL and Allegro are conses.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Thread-local specials

(defvar *store-controller* nil 
  "The store controller which persistent objects talk to.")

(defvar *current-transaction* nil
  "The transaction which is currently in effect.")

(defvar *default-retries* 200
  "The number of times the with-transaction macros should retry a
   transaction before the expression passes the error up the stack")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Enables warnings of various kinds

(defparameter *warn-on-manual-class-finalization* nil
  "Issue a printed warnings when the class mechanism has
   to finalize a class to access indexing information")

(defparameter *migrate-messages* t
  "Print information during migrate to update user on ongoing progress")

(defparameter *migrate-verbose* t
  "Print more than a simple status line")

(defparameter *warn-when-dropping-persistent-slots* nil
  "Assert a signal when the user is about to delete a bunch of
   persistent slot values on class redefinition.  This is nil by
   default to stop annoying message and confusing new users, but
   it will help keep users from shooting themselves in the foot
   and losing significant amounts of data during debugging and
   development.  It can be disabled if change-class is used a
   bunch in the application rather than just defclass changes
   interactively.")

(defparameter *return-null-on-missing-instance* t
  "During instance recreation, references to missing instances
   simply return null instead of signaling an error")

(defmacro with-inhibited-warnings (&body body)
  `(let ((*warn-on-manual-class-finalization* nil)
	 (*warn-when-dropping-persistent-slots* nil))
     (declare (special *warn-on-manual-class-finalization*
		       *warn-when-dropping-persistent-slots*))
     ,@body))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Forward references
;;
;; Elephant needs to export these symbols in order to 
;; properly load in asdf due to some circular dependencies
;; between lisp files 

(eval-when (:compile-toplevel :load-toplevel)
  (mapcar (lambda (symbol)
	    (intern symbol :elephant))
	  '("GET-CACHED-INSTANCE"
	    "SET-DB-SYNCH")))




