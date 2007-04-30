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
;; Data store API 
;;

(defmethod execute-transaction ((sc prev-store-controller) fn &rest rest)
  t)

(defmethod controller-start-transaction ((sc prev-store-controller) &rest rest)
  (make-instance 'transaction))

(defmethod controller-abort-transaction ((sc prev-store-controller) &rest rest)
  t)

(defmethod controller-commit-transaction ((sc prev-store-controller) &rest rest)
  (let ((out (transaction-log-stream sc)))
    (loop for txn in (nreverse operation-log) do
	 (serialize-xml txn out (serialization-state sc)))))

;;
;; Transaction logging and caching (slow and easy)
;;

;; TODO: 
;; - transaction pools to avoid allocating caches
;; - cheaper txns; txn-op pools?

(defclass transaction ()
  ((operation-log :accessor transaction-ops)
   (slot-cache :accessor transaction-slot-cache)
   (btree-cache :accessor transaction-btree-cache)
   (oid :accessor transaction-oid)))

(defmethod initialize-instance :after ((txn-state transaction) 
				       &key temp-txn &allow-other-keys)
  (unless temp-txn
    (setf (transaction-slot-cache txn-state)
	  (make-hash-table))
    (setf (transaction-btree-cache txn-state)
	  (make-hash-table))))

(defun commit-transaction (transaction sc)
  (

;;
;; Primitive Operations on the cache; nil on read if absent
;;

(defmethod read-cached-slot (sc transaction oid slotname)
  )

(defmethod write-cached-slot (sc transaction oid slotname value)
  )

(defmethod read-cached-btree (sc transaction oid key)
  )

(defmethod write-cached-btree (sc transaction oid key value)
  )

(defmethod cache-next-oid (sc transaction new-oid)
  )


(defun do-transaction-op (transaction txn-op)
  (if transaction
      (progn
	(push txn-op (transaction-ops transaction))
	(
      (make-transaction  

(defclass txn-op ()
  ((oid :accessor txn-oid)))

;;
;; Operation: Allocate an OID
;;

(defclass txn-oid-op (txn-op)
  ((oid :accessor txn-oid :initform 0)
   (last-oid :accessor txn-last-oid)))

(defmethod do-transaction ((op txn-oid-op) sc)
  (incf (last-oid sc)))

(defmethod replay-transaction ((op txn-oid-op) sc)
  (incf (last-oid sc)))
;;
;; 
;;  
 
(defclass txn-slot-write-op (txn-op)
  (

(defclass txn-btree-insert-op (txn-op)
(defclass txn-btree-delete-op (txn-op)
(defclass txn-btree-overwrite-op (txn-op)

