;;; -*- Mode: Lisp; Syntax: ANSI-Common-Lisp; Base: 10 -*-
;;;
;;; gc.lisp - A wrapper around the migrate interface to support
;;;           stop-and-copy GC at the repository level
;;; 
;;; By Ian Eslick <ieslick at common-lisp.net>
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

(in-package :elephant)


;; MARK AND SWEEP
;;
;; A simple mark and sweep collector.  Easy to use these mechanisms to create
;; a generational approach where at some OID boundry, we store all pointers
;; from old to new space.

;; ISSUES:
;; - Mark non-class-indexed instances & persistent-collections by
;;   walking the root btree and class-indexed classes in the class-index btree
;; - Schema btree is ignored (for now)
;; - Sweep is over non-class-indexed classes in the class index

;; CONSEQUENCES:
;; - Only btrees not stored elsewhere are retained (e.g. pset in instance slot)
;; - Cursor read-committed in mvcc mode will ensure that class-index ops
;;   can proceed while marks are being put on the instance-table

;; =====================================
;; Global GC
;; =====================================

(defun mark-and-sweep-gc (sc &optional test)
  ;; Context variables
  (assert (check-valid-store-controller sc))
  (let ((*store-controller* sc))
    (declare (special *store-controller*))
    (setup-gc sc)

    ;; MARK
    (mark-memory sc)
    (mark-roots sc)
    (mark-memory-final sc)

    ;; AND SWEEP
    (sweep-unindexed-classes sc test)

    (cleanup-gc sc test)))

(defun setup-gc (sc)
  ;; Setup the GC state
  (setf (controller-mark-table sc) (make-btree sc))
  (setf (controller-max-oid sc) 
	(oid (controller-mark-table sc)))
  (setf (controller-mark-list sc) nil))

(defun cleanup-gc (sc test)
  (unless test
    (setf (controller-mark-table sc) nil)))


;; MARK

(defun mark-memory (sc)
  "Inhibit loading new objects while we extract the live items
   from the cache and then walk them from outside the lockup.
   The cache lock is acquired for any lookup of an object from
   the cache - i.e. when the system is reading in a new object."
  ;; NOTE: What happens if there is a GC during this phase?
  (mapc #'walk-heap
	(ele-with-lock ((controller-instance-cache-lock sc))
	  (multiple-value-bind (oids insts) (live-memory-objects sc)
	    (setf (controller-mark-list sc) oids)
	    (setf (controller-marking-p sc) t)
	    insts))))

(defun mark-memory-final (sc)
  "We need to ensure that any loaded objects that are unlinked
   and not yet written back to a live variable are marked prior
   to the sweep.  This locks the cache up for the longest period
   as we want to be completely quiescent (no new objects) until
   we have an accurate mark list for the system"
  ;; NOTE: What happens if there is a GC during this phase?
  ;; NOTE: What happens with concurrent writes during this phase?
  (ele-with-lock ((controller-instance-cache-lock sc))
    (setf (controller-marking-p sc) nil)
    (multiple-value-bind (oids insts) (live-memory-objects sc)
      (let ((final (nset-difference oids (controller-mark-list sc))))
	(dolist (inst insts)
	  (when (member (oid inst) final)
	    (with-transaction (:read-committed t)
	      (walk-heap inst)))))
      (setf (controller-mark-list sc) nil)
      (setf (controller-marking-p sc) nil))))

(defun live-memory-objects (sc)
  "Return the list of oids and list of instances currently
   in memory.  This should be equal to the set of live objects."
  (let ((oids nil)
	(insts nil))
    (map-cache (lambda (oid inst)
		 (unless (reserved-oid-p sc oid)
		   (push oid oids)
		   (push inst insts)))
	       (controller-instance-cache sc))
    (values oids insts)))


(defun mark-roots (sc &key (chunk-size 100))
  (declare (ignore chunk-size))
  (walk-btree (controller-root sc))
  (walk-indexed-classes sc))

(defun mark-object (obj)
  (setf (get-value (oid obj) (controller-mark-table *store-controller*))
	t))


;; SWEEP

(defun sweep-unindexed-classes (sc &optional test (step 5))
  "For each unindexed class, sweep all instances that
   are not on the mark list.  One txn per class for now"
  (dolist (class (unindexed-classes sc))
    (let ((cid (class-schema-id sc class)))
      (sweep-class sc cid step :test test))))

(defun sweep-class (sc cid step &key test (start 0))
  "Sweep over the class and reclaim instances"
  (declare (ignore step start))
  (map-index (lambda (k v pk)
	       (declare (ignore k v))
	       (if test 
		   (sweep-debug sc pk)
		   (sweep-instance sc pk)))
	     (controller-instance-class-index sc)
	     :value cid))

(defun sweep-instance (sc oid)
  "Drop an instance unless it is marked"
  (unless (or (get-value oid (controller-mark-table sc))
	      (dropped-instance-p sc oid))
    (drop-instance (controller-recreate-instance sc oid))))

(defun sweep-debug (sc oid)
  (unless (get-value oid (controller-mark-table sc))
    (print (controller-recreate-instance sc oid))))

;; ================================
;; HEAP WALKER
;; ================================

(defun walk-indexed-classes (sc)
  (dolist (class (indexed-classes sc))
    (map-class #'walk-heap class)))

;; Leaf elements

(defgeneric walk-heap (obj)
  (:documentation "Treat heap objects like a tree and walk over
     aggregate objects to leaf elements (persistent objects and scalara values)")
  (:method ((obj t)) obj)
  (:method ((obj symbol)) obj)
  (:method ((obj string)) obj))

(defmethod walk-heap ((obj persistent-object))
  (mark-object obj))

;; Persistent Aggregates

(defmethod walk-heap ((obj btree))
  (mark-object obj)
  (when (and (< (oid obj) (controller-max-oid *store-controller*))
	     (not (reserved-oid-p *store-controller* (oid obj))))
    (walk-btree obj)))

(defmethod walk-btree ((obj btree))
  (map-btree (lambda (k v)
	       (walk-heap k)
	       (walk-heap v))
	     obj))

(defmethod walk-heap ((obj indexed-btree))
  "Key values of indices can be persistent objects"
  (map-indices (lambda (name index)
		 (declare (ignore name))
		 (walk-heap index))
	       obj)
  (call-next-method))

(defmethod walk-heap ((obj pset))
  (mark-object obj)
  (awhen (pset-btree obj)
    (mark-object it)))

;; Standard Aggregates

(defmethod walk-heap ((obj standard-object))
  (let ((svs (slots-and-values obj)))
    (loop for i from 0 below (/ (length svs) 2) do
	 (let ((slotname (pop svs))
	       (value (pop svs)))
	   (declare (ignore slotname))
	   (walk-heap value)))))

(defmethod walk-heap ((obj structure-object))
  (let ((svs (struct-slots-and-values obj)))
    (loop for i from 0 below (/ (length svs) 2) do
	 (let ((slotname (pop svs))
	       (value (pop svs)))
	   (declare (ignore slotname))
	   (walk-heap value)))))

(defmethod walk-heap ((obj cons))
  (walk-heap (car obj))
  (walk-heap (cdr obj)))

(defmethod walk-heap ((obj array))
  (loop for i fixnum from 0 below (array-total-size obj) do
       (walk-heap (row-major-aref obj i))))


(defmethod walk-heap ((obj hash-table))
  (maphash (lambda (key value)
	     (walk-heap key)
	     (walk-heap value))
	   obj))

;; Utils

(defun indexed-classes (sc)
  (remove-duplicates
   (remove-if #'null
    (map-btree (lambda (class db-schema)
		 (declare (ignore db-schema))
		 (let ((class (find-class class nil)))
		   (when (and class (class-indexing-enabled-p class))
		     class)))
	       (controller-schema-name-index sc)
	       :collect t))))

(defun unindexed-classes (sc)
  (remove-duplicates
   (append
    (all-persistent-collections sc)
    (remove-if #'null
	       (map-btree (lambda (class db-schema)
			    (declare (ignore db-schema))
			    (let ((class (find-class class nil)))
			      (when (and class (not (class-indexing-enabled-p class)))
				class)))
			  (controller-schema-name-index sc)
			  :collect t)))))

(defun all-persistent-collections (sc)
  (loop for i from 0 upto 53 
     when (default-class-id-type i sc)
     collect (find-class (default-class-id-type i sc))))


;;
;; Pre-packaged garbage collection thread
;;

;; (defvar gc-step-size 100
;;   "Number of OIDs to mark per invocation")

;; (defvar gc-step-interval 500
;;   "Number of ms to wait between chunk executions")

;; (defvar gc-oid-interval 1000
;;   "Number of OIDs to allocate ")

;; (defvar gc-pass-check-interval 10000
;;   "Number of ms to wait between checking for a new pass")

;; (defvar gc-drop-undefined-classes nil
;;   "Drop all instances of classes that do not exist in the image")

;;(defun gc-loop ()
;;  (loop
;;     sleep pass check interval
;;     when check oid interval do
;;     (gc-mark-loop)
;;     (gc-sweep-loop)

;;(defun gc-mark-loop ()
;;  (label ((do-chunk (oid)
;;             (let ((new-oid (mark-step oid gc-chunk-size)))
;;                (when new-oid
;;                    (do-chunk new-oid)))))
;;     (do-chunk 0)))
;;
;; Walk root:
;; - mark root refs
;; - mark 



;; ====================================================================
;; DEPRECATE THIS? USE MIGRATE TO COMPACT
;; ====================================================================

;; NOTE: we need to inhibit any operations on the old controller and
;; redirect them to the new one if other threads are accessing
;; can we do this with a lock in get-con?  get-con is used alot, 
;; though, so perhaps we should just rely on the user?

(defgeneric stop-and-copy-gc (sc &key &allow-other-keys)
  (:documentation "Wrap all the migrate machinery in a
   simple top-level gc call.  This will copy all the data
   in the source store to the target store.  Accesses to 
   the store should be inhibited by the user.  
   mapspec saves on memory space by storing oid->object maps on disk
   replace-source means to have the resulting spec be the same 
      as the source using copy-spec.  
   delete-source means do not keep a backup 
   backup means to make a backup with a name derived from the src"))

(defmethod stop-and-copy-gc ((src store-controller) &key target mapspec replace-source delete-source backup)
  (let ((src-spec (controller-spec src))
	(src-type (first (controller-spec src))))
    (when mapspec (set-oid-spec mapspec))
    (unless target (setf target (temp-spec src-type src-spec)))
    (let ((target (gc-open-target target))
	  (global? (eq src *store-controller*)))
      ;; Copy the source before migrate to recover if necessary
      (when backup
	(copy-spec src-type src-spec (temp-spec src-type src-spec)))
      ;; Primary call to migrate
      (migrate target src)
      ;; Cleanup mapspec
      (when mapspec 
	(set-oid-spec nil)
	(delete-spec (first mapspec) mapspec))
      ;; Close 
      (when replace-source
	(unless (eq src-spec (first target))
	  (error "Cannot perform replace-source on specs of different types: ~A -> ~A" target src-spec))
	(copy-spec src-type target src-spec)
	(when delete-source
	  (delete-spec (first target) target)))
      (when global?
	(setf *store-controller* target))
      target)))

(defmethod gc-open-target (spec)
  "Ignore *store-controller*"
  (initialize-user-parameters)
  (let ((controller (get-controller spec)))
    (apply #'open-controller controller (controller-opening-args controller))
    controller))

(defgeneric temp-spec (type spec)
  (:documentation "Create a temporary specification with source spec as hint")
  (:method (type spec)
    (declare (ignore spec))
    (error "temp-spec not implemented for type: ~A" type)))

(defgeneric delete-spec (type spec)
  (:documentation "Delete the storage associated with spec")
  (:method (type spec)
    (declare (ignore spec))
    (error "delete-spec not implemented for type: ~A" type)))

(defgeneric copy-spec (type src targ)
  (:documentation "Copy files associated with spec from src to targ")
  (:method (type src targ)
    (declare (ignore src targ))
    (error "copy-spec not implemented for type: ~A" type)))
