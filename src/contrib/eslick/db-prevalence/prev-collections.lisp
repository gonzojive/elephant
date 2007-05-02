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

(declaim (optimize safety))

;;
;; Internal node structure (see end of file for operations)
;;

(defstruct node 
  (key nil)
  (value nil)
  (next nil)
  (prev nil))

(defun make-btree-node (key value prev next)
  (make-node :key key :value value :prev prev :next next))


;;
;; Public interface: standard btree
;;

(defclass prev-btree (btree)
  ((hash :accessor btree-hash :initform (make-hash-table))
   (root :accessor btree-root :initform nil))
  (:metaclass persistent-metaclass))

(defmethod build-btree ((sc prev-store-controller))
  (make-instance 'prev-btree))

(defmethod get-value (key (bt prev-btree) )
  (aif (gethash key (btree-hash bt))
       (values (node-value it) t)
       (values nil nil)))

(defmethod (setf get-value) (value key (bt prev-btree))
  (insert-kv key value bt))

(defun insert-kv (key value bt)
  (aif (gethash key (btree-hash bt))
       (setf (node-value it) value)
       (progn 
	 (multiple-value-bind (node equal?)
	     (find-lesser-node key bt)
	   (cond ((and (null node) (not equal?)) ;; new first node
		  (let ((new-node (make-btree-node key value nil (btree-root bt))))
		    (insert-node nil new-node (btree-root bt))
		    (setf (btree-root bt) new-node)
		    (setf (gethash key (btree-hash bt)) new-node)))
		 ((and node (not equal?))
		  (insert-node node (make-btree-node key value nil nil) (node-next node))
		  (setf (gethash key (btree-hash bt)) (node-next node)))
		 (equal?
		  (setf (node-value node) value))
		 (t (error "oops"))))
	 value)))

(defmethod remove-kv (key (bt prev-btree))
  (let ((node (gethash key (btree-hash bt))))
    (if node (delete-node (node-prev node) node (node-next node)) nil)))

;;
;; Public interface: indexed btree
;;

(defclass prev-indexed-btree (indexed-btree prev-btree)
  ((indices :accessor indices :initform (make-hash-table))
   (indices-assoc :accessor indices-cache :initform (make-hash-table)
		  :transient t))
  (:metaclass persistent-metaclass))

(defmethod shared-initialize :after ((instance prev-indexed-btree) slot-names &rest rest)
  (declare (ignore slot-names rest))
  (setf (indices-cache instance) (indices instance)))

(defmethod build-indexed-btree ((sc prev-store-controller))
  (make-instance 'prev-indexed-btree))

(defmethod add-index ((bt prev-indexed-btree) &key index index-name key-form (populate t))
  (validate-add-index-args index index-name key-form)
  (ensure-transaction (:store-controller (get-con bt))
    (associate-index bt 
      (if index index
	  (build-btree-index (get-con bt) :name index-name :primary bt :key-form key-form)))))

(defun validate-add-index-args (index index-name key-form)
  (unless (or index
	      (and (not (null index-name))
		   (symbop index-name)
		   (or (symbolp key-form) (listp key-form))))
    (error "Invalid index arguments")))

(defun associate-index (bt index)
  (let ((ht (indices bt)))
    (setf (gethash (index-name index) ht) index)
    (setf (indices-cache bt) ht)
    (setf (indices bt) ht))
  (when populate (populate bt index))
  index)

(defmethod populate ((bt prev-indexed-btree) index)
  (map-btree (lambda (k v)
	       (index-kv k v index bt))
	     bt)))



;;
;; Public interface: btree index
;;

(defclass prev-btree-index (btree-index prev-btree)
  ((name :accessor index-name :initarg :name))
  (:metaclass persistent-metaclass))

(defmethod build-btree-index ((sc prev-store-controller) &key name primary key-form)
  (make-instance 'prev-btree-index :primary primary :key-form key-form :sc sc :name name))

;; Indexed btree reference
(defstruct ibtref key btree)

(defmacro get-indexed-value (ibtref)
  `(aif (gethash (ibtref-key ,ibtref) (ibtref-btree ,ibtref))
	(values (node-value it) t)
	(values nil nil)))

(defmethod get-value (key (bt prev-btree-index))
  (awhen (gethash key (btree-hash bt))
    (get-indexed-value (node-value it))))

(defun index-kv (k v index primary-bt)
  (let ((key-fn (key-fn index)))
    (multiple-value-bind (index? skey) 
	(funcall key-fn index k v)
      (when index?
	(insert-kv skey (make-ibtref :key k :btree primary-bt) index)))))

(defmethod get-primary-key (key (bt prev-btree-index))
  (let ((ref (gethash key (btree-hash bt))))
    (values (ibtref-key ref) (ibtref-btree ref))))

;;
;; Common cursor ops
;;

(defclass prev-cursor (cursor)
  ((node :accessor cursor-node :initarg :node)))

(defun cursor-root (cursor)
  (btree-root (cursor-btree cursor)))

(defmethod make-cursor ((bt prev-btree))
  (make-instance 'prev-cursor
		 :btree bt
		 :oid (oid bt)
		 :node nil))

(defmethod cursor-close ((cursor prev-cursor))
  (setf (cursor-initialized-p cursor) nil)
  (setf (cursor-node cursor) nil))

(defmethod cursor-duplicate ((cursor prev-cursor))
  (make-instance 'prev-cursor
		 :btree (cursor-btree cursor)
		 :node (cursor-node cursor)
		 :initialized-p (cursor-initialized-p cursor)
		 :oid (cursor-oid cursor)))

(defmethod cursor-current ((cursor prev-cursor))
  (when (cursor-initialized-p cursor)
    (values t (node-key (cursor-node cursor))
	    (node-value (cursor-node cursor)))))

;;
;; Cursor helpers
;;

(defun set-cursor (cursor node)
  (setf (cursor-node cursor) node)
  (setf (cursor-initialized-p cursor) t))

(defun unset-cursor (cursor)
  (setf (cursor-initialized-p cursor) nil)
  (setf (cursor-node cursor) nil))

(defmacro cursor-setif-ret (pred-val)
  "Symbol capturing macro; assumes cursor variable contains cursor"
  `(aif ,pred-val
	(progn
	  (set-cursor cursor it)
	  (cursor-current cursor))
	(unset-cursor cursor)))

(defmacro with-search-condition ((null-cond test-cond test-result) &body body)
  "Binds 'search-condition' to search forward testing conditions against 'node'"
  `(labels ((search-condition (node)
	      (cond ((null node)
		     ,null-cond)
		    (,test-cond
		     ,test-result)
		    (t (search-condition (node-next node))))))
     ,@body))

;;
;; Cursor ops
;;


(defmethod cursor-first ((cursor prev-cursor))
  (cursor-setif-ret (cursor-root cursor)))

(defmethod cursor-last ((cursor prev-cursor))
  (with-search-condition (node nil nil)
    (cursor-setif-ret (search-condition (cursor-root cursor)))))

(defmethod cursor-next ((cursor prev-cursor))
  (when (cursor-initialized-p cursor)
    (cursor-setif-ret (node-next (cursor-node cursor)))))

(defmethod cursor-prev ((cursor prev-cursor))
  (awhen (cursor-initialized-p cursor)
    (cursor-setif-ret (node-prev (cursor-node cursor)))))

(defmethod cursor-set ((cursor prev-cursor) key)
  (multiple-value-bind (node exists?)
      (gethash key (btree-hash (cursor-btree cursor)))
    (cursor-setif-ret (and exists? node))))

(defmethod cursor-set-range ((cursor prev-cursor) key)
  (with-search-condition (nil (> (node-key node) key) node)
    (multiple-value-bind (node exists?)
	(gethash key (btree-hash (cursor-btree cursor)))
      (cursor-setif-ret 
       (or (and exists? node) 
	   (search-condition (cursor-root cursor)))))))

(defmethod cursor-get-both ((cursor prev-cursor) key value)
  "Btrees do not have duplicate keys, so only found if v matches"
  (multiple-value-bind (exists? k v)
      (cursor-set cursor k)
    (if (and exists? (elephant::lisp-compare-equal v value))  
	(values exists? k v)
	(progn
	  (setf (cursor-initialized-p cursor) nil)
	  nil))))

(defmethod cursor-delete ((cursor prev-cursor))
  (when (cursor-initialized-p cursor)
    (let ((node (cursor-node cursor)))
      (delete-node (node-prev node) node (node-next node))
      (setf (cursor-initialized-p cursor) nil))))

(defmethod cursor-put ((cursor prev-cursor) value &key (key nil key-specified-p))
  "Changes the value of the key-value pair at the current cursor location.
   The cursor remains initialized."
  (if key-specified-p
      (setf (get-value key (cursor-btree cursor)) value)
      (if (not (cursor-initialized-p cursor))
	  (cerror "Ignore, write nothing, and continue?"
		  "Cannot put with uninitialized cursor")
	  (setf (node-value (cursor-node cursor)) value))))

;;
;; Special index cursor operations
;;

(defclass prev-index-cursor (prev-cursor) ()
  (:documentation "Cursor for traversing indexed btrees"))

(defmethod make-cursor ((index prev-btree-index))
  (make-instance 'prev-index-cursor
		 :btree index
		 :node nil
		 :oid (oid index)))

(defmethod cursor-pcurrent ((cursor prev-index-cursor))
  (when (cursor-initialized-p cursor)
    (let ((node (cursor-node cursor)))
      (values t (node-key node)
	      (get-value (node-value node) bt)
	      (node-value node)))))

;;
;; Some more helpers
;;

(defmacro cursor-psetif-ret (pred-val)
  "Symbol capturing macro; assumes cursor variable contains cursor"
  `(aif ,pred-val
	(progn
	  (set-cursor cursor it)
	  (cursor-pcurrent cursor))
	(unset-cursor cursor)))



;;
;; BTree internal node operations
;;
	
(defun find-lesser-node (key btree)
  "Returns (values node equalp)"
  (labels ((search-nodes (node)
	     (cond ((null node) ;; empty
		    (values nil nil))
		   ((elephant::lisp-compare-equal key (node-key node))
		    (values node t))
		   ((elephant::lisp-compare< key (node-key node))
		    (values (node-prev node) nil))
		   ((null (node-next node)) ;; at end
		    (values node nil))
		   (t (search-nodes (node-next node))))))
    (search-nodes (btree-root btree))))

(defun insert-node (prev-node new-node next-node)
  (unless (null prev-node)
    (setf (node-next prev-node) new-node))
  (setf (node-prev new-node) prev-node)
  (setf (node-next new-node) next-node)
  (unless (null next-node)
    (setf (node-prev next-node) new-node)))

(defun delete-node (prev-node old-node next-node)
  (unless (null next-node)
    (setf (node-next prev-node) (node-next old-node)))
  (unless (null prev-node)
    (setf (node-prev next-node) (node-prev old-node))))

