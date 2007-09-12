;;; -*- Mode: Lisp; Syntax: ANSI-Common-Lisp; Base: 10 -*-
;;;
;;; pm-cache.lisp -- caching support
;;; 
;;; part of
;;;
;;; Elephant: an object-oriented database for Common Lisp
;;;
;;; Copyright (c) 2007 Henrik Hjelte <hhjelte@common-lisp.net>
;;;
;;; Elephant users are granted the rights to distribute and use this software
;;; as governed by the terms of the Lisp Lesser GNU Public License
;;; (http://opensource.franz.com/preamble.html), also known as the LLGPL.
;;;

(in-package :db-postmodern)

(defgeneric cache-get-value (cache bt key))
(defgeneric cache-set-value (cache bt key value))
(defgeneric cache-clear-value (cache bt key))

;; no-ops for null-cache and unsupported types
(defmethod cache-get-value (cache bt key) nil)
(defmethod cache-set-value (cache bt key value))
(defmethod cache-clear-value (cache bt key))

;; hash-table cache
(defmethod cache-get-value ((cache hash-table) (bt btree) key)
  (gethash (cons (oid bt) key) cache))

(defmethod cache-set-value ((cache hash-table) (bt btree) key value)
  (setf (gethash (cons (oid bt) key) cache) value))

(defmethod cache-clear-value ((cache hash-table) (bt btree) key)
  (remhash (cons (oid bt) key) cache))

(defparameter *cache-mode* :per-transaction-cache)  

(defun make-value-cache () 
  (case *cache-mode*
    (:per-transaction-cache (make-hash-table :test 'equal))))
