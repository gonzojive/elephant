;;; -*- Mode: Lisp; Syntax: ANSI-Common-Lisp; Base: 10 -*-
;;;
;;; prev-collections.lisp -- Prevalence collections
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

;; Start slow then add a btree for efficient range queries
;; and gets/sets

(defclass prev-btree (btree)
  ((list :accessor btree-list)))

;;
;; Nodes
;;

(defstruct btree-node (key value next prev))

(defun make-btree-node (key value next prev)
  (cons (cons key value)
	(cons next prev)))

(defun node-key (node) (caar node))
(defsetf node-key (node) (value)
  `(setf (caar ,node) ,value))

(defun node-value (node) (cdar node))
(defsetf node-value (node) (value)
  `(setf (cdar ,node) ,value))

(defun node-next (node) (cadr node))
(defsetf node-next (node) (value)
  `(setf (cadr ,node) ,value))

(defun node-prev (node) (cddr node))
(defsetf node-prev (node) (value)
  `(setf (cddr ,node) ,value))

;;
;; Getting and setting
;;

(defun find-lesser-node (btree key)
  "Returns (values node equalp)"
  (labels ((search-nodes (prior nodes)
	     (cond ((null nodes)
		    (values prior nil))
		   ((null prior)
		    (if (< key (node-key (first nodes)))
			(values nil nil)
			(search-nodes (first nodes) (rest nodes))))
		   ((= (node-key prior) key)
		    (values prior t))
		   ((< (node-key prior) key)
		    (values prior nil))
		   (t (search-nodes (first nodes) (rest nodes))))))
    (search-nodes nil (btree-list btree))))


