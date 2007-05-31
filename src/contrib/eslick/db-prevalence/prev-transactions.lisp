;;; -*- Mode: Lisp; Syntax: ANSI-Common-Lisp; Base: 10 -*-
;;;
;;; prev-transactions.lisp -- Transaction support for prevalence data store
;;; 
;;; Initial version 5/1/2007 by Ian Eslick
;;; 
;;; part of
;;;
;;; Elephant: an object-oriented database for Common Lisp
;;;
;;; Copyright (c) 2007 by Ian Eslick
;;; <ieslick common-lisp net>
;;;
;;; Elephant users are granted the rights to distribute and use this software
;;; as governed by the terms of the Lisp Lesser GNU Public License
;;; (http://opensource.franz.com/preamble.html), also known as the LLGPL.
;;;

(in-package :db-prevalence)

;;
;; Data store transaction API 
;;

;; Note: implement retry
(defmethod execute-transaction ((sc prev-store-controller) txn-fn &key &allow-other-keys)
  (if *inhibit-transaction-logging*
      (funcall txn-fn)
      (ele-with-lock ((transaction-lock sc))
	(let ((*current-transaction* (new-or-existing-transaction sc))
	      (*store-controller* sc)
	      (result nil)
	      (success nil))
	  (declare (special *current-transaction* *store-controller*))
	  (unwind-protect
	       (progn
		 (setf result (multiple-value-list (funcall txn-fn)))
		 (controller-commit-transaction sc (transaction-object *current-transaction*))
		 (setq success t))
	    (unless success 
	      (controller-abort-transaction sc (transaction-object *current-transaction*))
	      (signal 'transaction-retry-count-exceeded
		      :count 0)))
	  (when success
	    (values-list result))))))

(defmethod controller-start-transaction ((sc prev-store-controller) &rest rest)
  (make-transaction-record sc (make-instance 'transaction :store sc)))

(defmethod controller-abort-transaction ((sc prev-store-controller) transaction &rest rest)
  (revert-ops transaction)
  t)

(defmethod controller-commit-transaction ((sc prev-store-controller) transaction &rest rest)
  (when (> (length (prev-transaction-ops transaction)) 0)
    (push-txn-op (make-instance 'txn-commit-op) transaction)
    (log-transaction transaction sc))
  t)

(defun my-transaction (sc)
  (if (and (elephant::transaction-object-p *current-transaction*)
	   (elephant::owned-txn-p sc *current-transaction*))
      (transaction-object *current-transaction*)))

(defun new-or-existing-transaction (sc)
  (if (and (elephant::transaction-object-p *current-transaction*)
	   (elephant::owned-txn-p sc *current-transaction*))
      *current-transaction*
      (controller-start-transaction sc)))


;;
;; Transaction logging and caching (slow and easy)
;;

(defclass transaction ()
  ((operation-log :accessor prev-transaction-ops :initarg :log 
		  :initform (make-array 20 :initial-element nil :adjustable t :fill-pointer 0))
   (store :accessor prev-transaction-store :initarg :store)))

(defmethod push-txn-op (txn-op (transaction transaction))
  (vector-push-extend txn-op (prev-transaction-ops transaction)))

(defmethod revert-ops ((transaction transaction))
  (with-inhibited-transactions
    (let ((ops (prev-transaction-ops transaction)))
      (when (> (length ops) 0)
	(loop for i from (1- (fill-pointer ops)) to 0 
	   do (revert-txn-op (aref ops i) (prev-transaction-store transaction)))))))

(defmethod replay-ops ((transaction transaction))
  (with-inhibited-transactions
    (let ((ops (prev-transaction-ops transaction))
	  (store (prev-transaction-store transaction)))
      (loop for op across ops do
	   (replay-txn-op op store)))))

;;
;; Commit or replay transactions to side effect actual data
;;

;; Abstract base class

(defclass txn-op () ())

(defmethod revert-txn-op ((op txn-op) sc)
  (declare (ignore sc))
  t)

(defmethod replay-txn-op ((op txn-op) sc)
  (declare (ignore sc))
  t)

(defclass txn-commit-op (txn-op) ())

(defclass txn-abort-op (txn-op) ())

;; Allocate operation 

(defclass txn-new-instance-op (txn-op) 
  ((oid :initarg :oid :accessor txn-oid)
   (class :initarg :class :accessor txn-classname)))

(defmethod print-object ((op txn-new-instance-op) stream)
  (format stream "#<TXN-NEW-INST ~A ~A>" 
	  (txn-oid op) (txn-classname op)))

(defmethod cache-instance :before ((sc prev-store-controller) instance)
  (unless-inhibited
    (process-txn-op
     (make-instance 'txn-new-instance-op
		    :oid (oid instance)
		    :class (class-name (class-of instance)))
     (get-con instance))))

(defmethod revert-txn-op ((op txn-new-instance-op) sc)
  "This object should not be reference, if it is it will yield an error
   when persistent slots are dereferenced"
  (let ((instance (get-cached-instance sc (txn-oid op) (txn-classname op))))
    (setf (oid instance) nil)
    (decf (last-oid sc))
    (uncache-instance sc (txn-oid op))))

(defmethod replay-txn-op ((op txn-new-instance-op) sc)
  (let ((instance (make-instance (txn-classname op) :sc sc)))
    (assert (eq (txn-oid op) (oid instance)))
    instance))

;; Slot side effects

(defclass txn-slot-op (txn-op)
  ((oid :accessor txn-oid :initarg :oid)
   (slotname :accessor txn-slotname :initarg :slotname)))

(defclass txn-slot-write-op (txn-slot-op)
  ((old-unbound-p :accessor txn-old-unbound-p :initarg :old-unbound-p)
   (old-value :accessor txn-old-value :initarg :old-value)
   (new-value :accessor txn-new-value :initarg :new-value)))

(defmethod print-object ((op txn-slot-write-op) stream)
  (format stream "#<TXN-WRITE-SLOT ~A ~A ~A>" 
	  (txn-oid op) (txn-slotname op) (txn-new-value op)))

(defmethod persistent-slot-writer :before ((sc prev-store-controller) value instance name)
  (unless-inhibited
    (assert (not (subtypep (type-of value) 'persistent-metaclass)))
    (multiple-value-bind (old exists?) 
	(read-controller-slot (oid instance) name sc)
      (process-txn-op 
       (make-instance 'txn-slot-write-op 
		      :oid (oid instance)
		      :slotname name
		      :old-value old
		      :old-unbound-p (not exists?)
		      :new-value value)
       sc))))

(defmethod revert-txn-op ((op txn-slot-write-op) sc)
  (if (txn-old-unbound-p op)
      (unbind-controller-slot (txn-oid op) (txn-slotname op) sc)
      (write-controller-slot (txn-old-value op) (txn-oid op) (txn-slotname op) sc)))

(defmethod replay-txn-op ((op txn-slot-write-op) sc)
  (write-controller-slot  (txn-new-value op) (txn-oid op) (txn-slotname op) sc))

(defclass txn-slot-makunbound-op (txn-slot-op)
  ((unbound-p :accessor txn-unbound-p :initarg :unbound-p)
   (old-value :accessor txn-old-value :initarg :old-value)))

(defmethod persistent-slot-makunbound :before ((sc prev-store-controller) instance name)
  (unless-inhibited
    (multiple-value-bind (old exists?)
	(read-controller-slot (oid instance) name sc)
      (process-txn-op
       (make-instance 'txn-slot-makunbound-op 
		      :oid (oid instance)
		      :unbound-p (not exists?)
		      :old-value old)
       sc))))

(defmethod revert-txn-op ((op txn-slot-makunbound-op) sc)
  (unless (txn-unbound-p op)
    (write-controller-slot (txn-old-value op) (txn-oid op) (txn-slotname op) sc)))

(defmethod replay-txn-op ((op txn-slot-makunbound-op) sc)
  (unbind-controller-slot (txn-oid op) (txn-slotname op) sc))
		    

;; BTree side effects

(defclass txn-btree-op (txn-op)
  ((oid :accessor txn-oid :initarg :oid)
   (key :accessor txn-key :initarg :key)
   (old-value :accessor txn-old-value :initarg :old-value)))

(defclass txn-btree-write-op (txn-btree-op)
  ((new-value :accessor txn-new-value :initarg :new-value)))

(defmethod print-object ((op txn-btree-write-op) stream)
  (format stream "#<TXN-WRITE-BTREE ~A ~A ~A>" 
	  (txn-oid op) (txn-key op) (txn-new-value op)))

(defmethod (setf get-value) :before (value key (bt prev-btree))
  (unless-inhibited
    (when (not (subtypep (type-of bt) 'prev-btree-index))
      (process-txn-op
       (make-instance 'txn-btree-write-op
		      :oid (oid bt)
		      :key key
		      :old-value (get-value key bt)
		      :new-value value)
       (get-con bt)))))

(defmethod revert-txn-op ((op txn-btree-write-op) sc)
  (let ((bt (elephant::get-cached-instance sc (txn-oid op) nil)))
    (remove-kv (txn-key op) (txn-new-value op))
    (setf (get-value (txn-key op) bt) (txn-old-value op))))

(defmethod replay-txn-op ((op txn-btree-write-op) sc)
  (let ((bt (get-object-instance (txn-oid op) sc)))
    (setf (get-value (txn-key op) bt) (txn-new-value op))))

(defclass txn-btree-remove-op (txn-btree-op) ())

(defmethod print-object ((op txn-btree-remove-op) stream)
  (format stream "#<TXN-BTREE-RM ~A ~A>" 
	  (txn-oid op) (txn-key op)))

(defmethod remove-kv :before (key (bt prev-btree))
  (unless-inhibited
    (when (not (subtypep (type-of bt) 'prev-btree-index))
      (process-txn-op
       (make-instance 'txn-btree-remove-op 
		      :oid (oid bt)
		      :key key
		      :old-value (get-value key bt))
       (get-con bt)))))

(defmethod revert-txn-op ((op txn-btree-remove-op) sc)
  (let ((bt (get-object-instance (txn-oid op) sc)))
    (remove-kv (txn-key op) bt)
    (setf (get-value (txn-key op) bt) (txn-old-value op))))

(defmethod replay-txn-op ((op txn-btree-remove-op) sc)
  (let ((bt (get-object-instance (txn-oid op) sc)))
    (remove-kv (txn-key op) bt)))

;;
;; Writing and reading records
;;

(defun process-txn-op (txn-op sc)
  (assert (not *inhibit-transaction-logging*))
  (aif (my-transaction sc)
       (push-txn-op txn-op it)
       (commit-simple-transaction txn-op sc)))

(defvar *simple-transaction* (make-instance 'transaction))
(defvar *simple-txn-commit-op* (make-instance 'txn-commit-op))
			     
(defun commit-simple-transaction (txn-op sc)
  (setf (prev-transaction-ops *simple-transaction*)
	(make-array 2 :initial-contents (list txn-op nil) :adjustable t :fill-pointer 1))
  (setf (prev-transaction-store *simple-transaction*) sc)
  (controller-commit-transaction sc *simple-transaction*))

(defun committed-transaction-p (transaction)
  (and transaction
       (prev-transaction-ops transaction)
       (eq (type-of (last-txn-op transaction))
	   'txn-commit-op)))

(defun last-txn-op (transaction)
  (aref (prev-transaction-ops transaction)
	(1- (fill-pointer (prev-transaction-ops transaction)))))

(defun log-transaction (txn sc)
  (serialize-to-stream (prev-transaction-ops txn) (transaction-log-stream sc) sc)
  (force-output (transaction-log-stream sc)))

(defun recover-transaction (sc)
  (let ((log (deserialize-from-stream (transaction-log-stream sc) sc)))
    (make-instance 'transaction :log log :store sc)))