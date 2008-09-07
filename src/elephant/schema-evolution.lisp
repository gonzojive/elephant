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
(in-package "ELEPHANT")

;; =================================
;;  Synchronize a store to a class
;; =================================

(defvar *lazy-db-instance-upgrading* nil
  "Only upgrade instances when loaded.  This may require a chain of 
   transformations and delay reclimation of space, but it amortizes
   upgrade costs over time.  Not compatible with valid, up-to-date 
   indices!")

(defvar *lazy-memory-instance-upgrading* nil
  "Walk through a given store's memory cache on class redefinition by default.
   Setting this variable inhibits calling
   update-instance-for-redefined-class for any instances that
   have been invalidated by the MOP")

(defmethod synchronize-store-classes ((sc store-controller))
  "Synchronize all schemas in the stores to in-memory classes.
   This also populates the schema cache.  It is meant to be
   called when a controller is opened.  If a class and db schema
   mismatch, synchronize the class then update all versions of
   the class to the new schema"
  (map-btree (lambda (cid db-schema)
	       (declare (ignore cid))
	       (let ((classname (schema-classname db-schema)))
		 (awhen (find-class classname)
		   (let ((class-schema (%class-schema it)))
		     (unless (match-schemas class-schema db-schema)
		       	(synchronize-store-class sc class-schema db-schema)
			(unless *lazy-db-instance-upgrading*
			  (upgrade-all-db-instances sc class-schema)))))))
	     (controller-schema-table sc))
  (unless *lazy-memory-instance-upgrading*
    (upgrade-all-memory-instances sc)))

(defmethod synchronize-stores-for-class (class)
  "Synchronize all stores connected to a given class.  Meant to be
   called during class redefinition to keep all DB instances in sync."
  (let ((class-schema (%class-schema class)))
    (loop for (spec . db-schema) in (%store-schemas class) do
	 (unless (match-schemas class-schema db-schema)
	   (let ((store (lookup-con-spec spec)))
	     (synchronize-store-class store class-schema db-schema)
	     (unless *lazy-memory-instance-upgrading*
	       (upgrade-all-memory-instances store))
	     (unless *lazy-db-instance-upgrading*
	       (upgrade-all-db-instances store class-schema)))))))

(defmethod synchronize-store-class ((sc store-controller) class-schema old-schema)
  "Synchronizing a store means adding/removing indices, upgrading
   the default schema if necessary, etc."
  (format t "Synchronizing ~A in ~A~%" (schema-classname class-schema) (controller-spec sc))
  (let* ((class (find-class (schema-classname class-schema)))
	 (new-schema (create-controller-schema sc class))
	 (diff (schema-diff new-schema old-schema)))
    ;; Chain schemas
    (setf (schema-successor old-schema) (schema-id new-schema))
    (setf (schema-predecessor new-schema) (schema-id old-schema))
    (update-controller-schema sc old-schema)
    (update-controller-schema sc new-schema t)
    ;; Update the class
    (loop for entry in diff do
	 (upgrade-class-slot sc class (diff-type entry) (diff-recs entry)))))


(defmethod upgrade-class-slot ((sc store-controller) class (diff-type (eql :add)) recs)
  "At the store level, we'll only need to deal with structures that are at the
   class level, not managed in the individual instance"
  (declare (ignore class))
  (with-slots (name type args) (first recs)
    (case type
      (:indexed (add-slot-index sc (make-dup-btree sc) (getf args :base) name))
      (:derived (add-slot-index sc (make-dup-btree sc) (getf args :base) name))
      (:association nil))))

(defmethod upgrade-class-slot ((sc store-controller) class (diff-type (eql :rem)) recs)
  "Drop index and association storage on upgrade.  Loss of data for associations should
   be flagged during the redefinition."
  (declare (ignore class))
  (with-slots (name type args) (first recs)
    (case type
;;      (:indexed (drop-slot-index sc (getf args :base) name))
      (:association nil))))

(defmethod upgrade-class-slot ((sc store-controller) class (diff-type (eql :change)) recs)
  "For now, we can effectively remove and add at the store level"
  (upgrade-class-slot sc class :rem (list (first recs)))
  (upgrade-class-slot sc class :add (list (second recs))))

;; =====================================================
;;  Default upgrade of instances; call any explicit fns
;; =====================================================

(defmethod upgrade-all-memory-instances ((sc store-controller))
  "Touch each instance in memory to force update-instance-for-redefined class
   to be called on classes that were just redefined.  This in turn calls 
   upgrade-instance.  This should be called after a redefinition."
  (loop for inst being the hash-value of (controller-instance-cache sc) do
       #+(or cmu sbcl)(oid (weak-pointer-value inst))
       #-(or cmu sbcl)(oid inst)
       #+openmcl (value value)))

(defmethod upgrade-all-db-instances ((sc store-controller) class-schema)
  "This does a scan and upgrades each instance of the class referred to
   by the class schema.  If there is a predecessor class in the database,
   its instances are upgraded to the current.  If the db-schema and class
   schema do not match (i.e. we are connecting to a store) then go ahead and
   run synchronize-store-class to upgrade class-level info like indices"
  (let* ((classname (schema-classname class-schema))
	 (db-schema (get-current-db-schema sc classname)))
    ;; When the db-schema is not up to date, make it so
    (unless (match-schemas class-schema db-schema)
      (synchronize-store-class sc class-schema db-schema))
    ;; Update the instances oldest to newest
    (loop for schema in (get-db-schemas sc classname) 
	 unless (eq (schema-id schema) (schema-id db-schema)) do
	 (progn
	   (map-index (lambda (cidx pcidx oid)
			(declare (ignore cidx pcidx))
			(let ((instance (controller-recreate-instance sc oid classname)))
			  (upgrade-db-instance instance db-schema schema nil)))
		      (controller-instance-class-index sc)
		      :value (schema-id schema))
	   (awhen (schema-successor (get-controller-schema sc (schema-id schema)))
	     (awhen (get-controller-schema sc it)
	       (setf (schema-predecessor it) nil)))))))
;;	   (remove-controller-schema sc (schema-id schema))))))

(defmethod upgrade-db-instance ((instance persistent-object) (new-schema db-schema) (old-schema db-schema) old-values)
  "Upgrade a database instance from the old-schema to the new-schema.
   This does mean loading it into memory (for now)!"
  (let ((sc (get-con instance))
	(diff (schema-diff new-schema old-schema)))
    (ensure-transaction (:store-controller sc)
      (awhen (schema-upgrade-fn old-schema)
	(apply-schema-change-fn instance it old-schema))
      (loop for entry in diff do
	   (upgrade-instance-slot sc instance (diff-type entry) (diff-recs entry) old-values))
      (initialize-new-slots instance diff)
      (set-instance-schema-id sc (oid instance) (schema-id new-schema)))))

(defmethod upgrade-instance-slot (sc instance (type (eql :change)) recs old-values)
  "Handle changes in class type"
  (destructuring-bind (old-rec new-rec) recs
    (with-slots ((old-type type) (old-name name) (old-args args)) old-rec
      (cond ;; If it was not indexed, and now is, we have to notify the index of the new value
	    ((and (member old-type '(:persistent :cached))
		  (eq (slot-rec-type new-rec) :indexed)
		  (slot-boundp instance old-name))
	     (setf (slot-value instance old-name) (slot-value instance old-name)))
	    ;; If it was indexed, and the base index has changed 
	    ;; The new index will get updated as a natural part of the rest of the protocol
	    ((and (member old-type '(:indexed :derived))
		  (not (eq (getf old-args :base)
			   (getf (slot-rec-args new-rec) :base))))
	     (let ((slot-value (slot-value instance old-name)))
	       (unindex-slot-value sc slot-value (oid instance) old-name (getf old-args :base))))
	    ;; If it was a persistent slot and now isn't, drop it and add the new type back
	    ((and (member old-type '(:persistent :indexed :cached :derived))
		  (not (member (slot-rec-type new-rec) '(:persistent :indexed :cached :derived))))
	     (upgrade-instance-slot sc instance :rem (list old-rec) old-values)
	     (upgrade-instance-slot sc instance :add (list new-rec) old-values))
	    ;; If the old slot was indexed
	    ((and (eq old-type :indexed) (eq (slot-rec-type new-rec) :indexed)
		  (not (eq (getf (slot-rec-args old-rec) :base)
			   (getf (slot-rec-args new-rec) :base))))
	     nil)
	    (t nil)))))

(defmethod upgrade-instance-slot (sc instance (type (eql :rem)) recs old-values)
  "Handle slot removal and cleanup of values, such as sets"
  (with-slots (type name args) (first recs)
    (when (member type '(:persistent :cached :indexed :derived))
      (persistent-slot-makunbound sc instance name))
    (when (member type '(:indexed :derived))
      (awhen (getf old-values name)
	(unindex-slot-value sc (cdr it) (oid instance) name args)))
    (when (eq type :set-valued)
      (let ((set (and (persistent-slot-boundp sc instance name)
		      (persistent-slot-reader sc instance name))))
	(when set (drop-btree set))
	(slot-makunbound instance name)))))

(defmethod upgrade-instance-slot (sc instance (type (eql :add)) recs old-values)
  "Not needed, new slots are initialized above"
  (declare (ignore sc instance recs old-values))
  nil)
  
(defun initialize-new-slots (instance diff)
  (labels ((adding-persistent? (entry)
	     (when (and (eq :add (diff-type entry))
			(member (slot-rec-type (first (diff-recs entry)))
				'(:persistent :indexed :cached :set-valued)))
	       (slot-rec-name (first (diff-recs entry)))))
	   (change-to-persistent? (entry)
	     (when (and (eq :change (diff-type entry))
			(not (member (slot-rec-type (first (diff-recs entry)))
				     '(:persistent :indexed :cached :set-valued)))
			(member (slot-rec-type (second (diff-recs entry)))
				'(:persistent :indexed :cached :set-valued)))
	       (slot-rec-name (second (diff-recs entry)))))
	   (init-slot? (entry)
	     (or (adding-persistent? entry)
		 (change-to-persistent? entry)))
	   (compute-init-slots ()
	     (remove-if #'null (mapcar #'init-slot? diff))))
    (apply #'shared-initialize instance (compute-init-slots) nil)))

;; ===========================
;;  Change class support
;; ===========================

(defmethod change-db-instance ((current persistent-object) previous
			       new-schema old-schema)
  "Change a database instance from one schema & class to another
   These are different objects with the same oid"
  (let ((sc (get-con current))
	(oid (oid current))
	(diff (schema-diff new-schema old-schema)))
    (ensure-transaction (:store-controller sc)
      ;; do we need to pass the persistent object?  Transient ops require previous?
      (awhen (schema-upgrade-fn old-schema)
	(apply-schema-change-fn current it old-schema))
      ;; Change all the slots
      (loop for entry in diff do
	   (change-instance-slot sc current previous (diff-type entry) (diff-recs entry)))
      ;; Initialize new slots (is this done by default?)
      (initialize-new-slots current diff)
      (uncache-instance sc oid)
      (set-instance-schema-id sc oid (schema-id new-schema)))))

(defmethod change-instance-slot (sc current previous (type (eql :change)) recs)
  "Handle changes in class type"
;; TODO
;;   (print recs)
;;   (dump-btree (elephant::controller-instance-table sc))
;;   (dump-index (elephant::controller-instance-class-index sc))
;;   (awhen (find-inverted-index 'elephant-tests::idx-six 'elephant-tests::slot1 :null-on-fail t)
;;     (dump-btree it))
  (destructuring-bind (old-rec new-rec) recs
    (with-slots ((old-type type) (old-name name) (old-args args)) old-rec
      (with-slots ((new-type type) (new-name name) (new-args args)) new-rec
	(cond ;; If it was not indexed, and now is, we have to notify the index of the new value (?)
	  ((and (member old-type '(:persistent :cached))
		(eq new-type :indexed))
	   (setf (slot-value previous old-name) (slot-value previous old-name)))
	  ;; If the old slot was indexed, we definitely need to unindex it to avoid
          ;; having the objects hang around in the index
	  ((eq old-type :indexed)
	   (unindex-slot-value sc (slot-value previous old-name)
			       (oid previous) old-name (getf old-args :base)))
	  ;; If it was a persistent slot and now isn't, drop it and add the new type back
	  ((and (member old-type '(:persistent :indexed :cached))
		(not (member new-type '(:persistent :indexed :cached))))
	   (change-instance-slot sc current previous :rem (list old-rec))
	   (change-instance-slot sc current previous :add (list new-rec)))
	  (t nil))))))

(defmethod change-instance-slot (sc current previous (type (eql :rem)) recs)
  "Handle slot removal and cleanup of values, such as sets"
  (declare (ignore current))
  (with-slots ((prev-type type) (prev-name name) (prev-args args)) (first recs)
    (cond ((member prev-type '(:persistent :cached :indexed))
	   (slot-makunbound previous prev-name))
	  ((eq type :set-valued)
	   (let ((set (and (persistent-slot-boundp sc previous prev-name)
			   (persistent-slot-reader sc previous prev-name))))
	     (when set (drop-btree set))
	     (slot-makunbound previous prev-name))))))

(defmethod change-instance-slot (sc current previous (type (eql :add)) recs)
  "Not needed, new slots are initialized above"
  (declare (ignore sc current previous recs))
  nil)
  
;;
;; Some utilities for redefine/upgrade class
;;

(defun apply-schema-change-fn (instance expr old-schema)
  (cond ((functionp expr)
	 (funcall expr instance))
	((symbolp expr)
	 (funcall (symbol-function expr) instance))
	((consp expr)
	 (let ((fn (compile nil (eval expr))))
	   (setf (schema-upgrade-fn old-schema) fn)
	   (funcall fn instance)))))

(defun unindex-slot-value (sc key value old-name old-base)
  (let* ((master (controller-index-table sc))
	 (index (get-value (cons old-base old-name) master)))
    (remove-kv-pair key value index)))


;; ================================
;;  Debugging tools
;; ================================

(defun dump-class-schema-status (sc classname &optional (stream t))
  (let* ((class (find-class classname))
	 (class-schema (%class-schema class))
	 (cached-store (get-class-controller-schema sc class))
	 (db-schema-chain (get-db-schemas sc classname)))
    (format stream "Schema status dump for: ~A~%" classname)
    (format stream "CLASS SCHEMA:~%")
    (format stream "--------------------~%")
    (dump-schema class-schema stream)
    (format stream "~%CACHED STORE SCHEMA:~%")
    (format stream "---------------------~%")
    (dump-schema cached-store stream)
    (format stream "~%SCHEMA CHAIN FROM DB:~%")
    (format stream "---------------------~%")
    (dolist (schema db-schema-chain)
      (dump-schema schema stream))))

	 
               
