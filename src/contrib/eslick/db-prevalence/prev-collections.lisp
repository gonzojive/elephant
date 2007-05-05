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

(defmethod print-object ((node node) stream)
  (format stream "#<NODE k:~A v:~A p:~A n:~A>" 
	  (node-key node) (node-value node) 
	  (awhen (node-prev node)
	    (node-key it))
	  (awhen (node-next node)
	    (node-key it))))

;;
;; Public interface: standard btree
;;

(defclass prev-btree (btree)
  ((hash :accessor btree-hash :initform (make-hash-table :test #'equal) :initarg :hash)
   (root :accessor btree-root :initform nil :initarg :root))
  (:metaclass persistent-metaclass))

(defmethod build-btree ((sc prev-store-controller))
  (make-instance 'prev-btree))

(defmethod get-value (key (bt prev-btree) )
  (aif (gethash key (btree-hash bt))
       (values (node-value it) t)
       (values nil nil)))

(defmethod (setf get-value) (value key (bt prev-btree))
  (ensure-transaction (:store-controller (get-con bt))
    (aif (gethash key (btree-hash bt))
	 (setf (node-value it) value)
	 (insert-kv key value bt))
    value))

(defmethod remove-kv (key (bt prev-btree))
  (let ((node (gethash key (btree-hash bt))))
    (when node 
      (delete-kv node bt))))

(defmethod existsp (key (bt prev-btree))
  (multiple-value-bind (val valid?)
      (get-value key bt)
    (declare (ignore val))
    valid?))

;;
;; Internal data structure maintaince
;;

(defmethod insert-kv (key value (bt prev-btree))
  (aif (gethash key (btree-hash bt))
       (progn
	 (setf (node-value it) value)
	 it)
       (progn 
	 (multiple-value-bind (node equal?)
	     (find-lesser-node key bt)
	   (cond ((and (null node) (not equal?)) ;; new first node
		  (let ((new-node (make-btree-node key value nil nil)))
		    (insert-node nil new-node (btree-root bt))
		    (setf (btree-root bt) new-node)
		    (setf (gethash key (btree-hash bt)) new-node)))
		 ((and node (not equal?))
		  (let ((new-node (make-btree-node key value nil nil)))
		    (insert-node node new-node (node-next node))
		    (setf (gethash key (btree-hash bt)) new-node)))
		 (equal?
		  (setf (node-value node) value)
		  (setf (gethash key (btree-hash bt)) node))
		 (t (error "oops")))))))

(defmethod delete-kv (node (bt prev-btree))
  (when (eq node (btree-root bt))
    (setf (btree-root bt) (node-next node)))
  (delete-node (node-prev node) node (node-next node))
  (remhash (node-key node) (btree-hash bt)))

;;
;; Public interface: indexed btree
;;

(defclass prev-indexed-btree (indexed-btree prev-btree)
  ((indices :accessor indices :initarg :indices :initform (make-hash-table)))
  (:metaclass persistent-metaclass))

(defmethod build-indexed-btree ((sc prev-store-controller))
  (make-instance 'prev-indexed-btree))

(defmethod (setf get-value) (value key (bt prev-indexed-btree))
  (ensure-transaction (:store-controller (get-con bt))
    (let* ((old-node (gethash key (btree-hash bt)))
	   (new-node (insert-kv key value bt))
	   (changed? (and old-node
			  (or (not (eq old-node new-node))
			      (not (lisp-compare-equal (node-value old-node) (node-value new-node)))))))
      (loop for index being the hash-value of (indices bt)
	 do 
	   (when changed? (delete-kv old-node index))
	   (index-kv new-node index bt))
      value)))

(defmethod remove-kv (key (bt prev-indexed-btree))
  "Have to remove key-value pairs from index as well"
  (let ((node (gethash key (btree-hash bt))))
    (when node 
      (delete-kv node bt)
      (loop for index being the hash-value of (indices bt)
	   do (delete-kv node index))
      t)))

(defmethod add-index ((bt prev-indexed-btree) &key index index-name key-form (populate t))
  (validate-add-index-args index index-name key-form)
  (ensure-transaction (:store-controller (get-con bt))
    (associate-index bt 
      (if index index
	  (build-btree-index (get-con bt) :name index-name :primary bt :key-form key-form))
      populate)))

(defmethod get-index ((bt prev-indexed-btree) name)
  (gethash name (indices bt)))

(defmethod remove-index ((bt prev-indexed-btree) name)
  (with-transaction (:store-controller (get-con bt))
    (remhash name (indices bt))))

(defmethod map-indices (fn (bt prev-indexed-btree))
  (maphash fn (indices bt)))

;;
;; Indexed btree index support
;; 

(defun validate-add-index-args (index index-name key-form)
  (unless (or index
	      (and (not (null index-name))
		   (symbolp index-name)
		   (or (symbolp key-form) (listp key-form))))
    (error "Invalid index arguments")))

(defun associate-index (bt index &optional populate)
  (setf (gethash (index-name index) (indices bt)) index)
  (when populate (populate bt index))
  index)

(defmethod populate ((bt prev-indexed-btree) index)
  (maphash (lambda (k node)
	     (declare (ignore k))
	     (index-kv node index bt))
	   (btree-hash bt)))

;;
;; Public interface: btree index
;;

;; Indexed btree references to primary key/btrees
(defstruct ibtref node btree)

(defclass prev-btree-index (btree-index prev-btree)
  ((name :accessor index-name :initarg :name))
  (:metaclass persistent-metaclass))

(defmethod build-btree-index ((sc prev-store-controller) &key name primary key-form)
  (make-instance 'prev-btree-index :primary primary :key-form key-form :sc sc :name name))

(defmacro get-indexed-value (ibtref)
  `(aif (ibtref-node ,ibtref)
	(values (node-value it) t)
	(values nil nil)))

(defmethod get-value (key (bt prev-btree-index))
  (awhen (gethash key (btree-hash bt))
    (get-indexed-value (node-value it))))

(defmethod remove-kv (key (bt prev-btree-index))
  (multiple-value-bind (pkey primary)
      (get-primary-key key bt)
    (when primary
      (remove-kv pkey primary))))

;;
;; Index helpers
;;

(defmethod get-primary-key (key (bt prev-btree-index))
  (let ((node (gethash key (btree-hash bt))))
    (if (and node (node-value node))
	(values (node-key (ibtref-node (node-value node))) (ibtref-btree (node-value node)))
	(values nil nil))))

(defun index-kv (node index primary-bt)
  (let ((key-fn (key-fn index)))
    (multiple-value-bind (index? skey)
	(funcall key-fn index (node-key node) (node-value node))
      (when index?
	(index-insert-kv skey (make-ibtref :node node :btree primary-bt) index)))))

(defun ibtref-equal (ref1 ref2)
  (and (elephant::lisp-compare-equal 
	(node-value (ibtref-node ref1))
	(node-value (ibtref-node ref2)))
       (elephant::lisp-compare-equal
	(node-key (ibtref-node ref1))
	(node-key (ibtref-node ref2)))))
   

(defun index-insert-kv (skey ref index)
  "Handle duplicates; update hash if necessary"
  (multiple-value-bind (node equal?)
      (find-lesser-node skey index)
    (let ((new-node (make-btree-node skey ref nil nil)))
      (cond ((null node)
	     (setf (gethash skey (btree-hash index)) new-node)
	     (insert-node nil new-node (btree-root index))
	     (setf (btree-root index) new-node))
	    (equal?
	     (unless (ibtref-equal ref (node-value node))
	       (when (null (node-prev node))
		 (setf (btree-root index) new-node))
	       (setf (gethash skey (btree-hash index)) new-node)
	       (insert-node (node-prev node) new-node node)))
	    (t
	     (setf (gethash skey (btree-hash index)) new-node)
	     (insert-node node new-node (node-next node)))))
    t))

(defmethod delete-kv (pnode (index prev-btree-index))
  "Handle duplicates (update hash)"
  (multiple-value-bind (index? skey)
      (funcall (key-fn index) index (node-key pnode) (node-value pnode))
    (when index?
      (labels ((value-matches-pnode (inode)
		 (eq (ibtref-node (node-value inode)) pnode))
	       (first-node-p (inode)
		 (null (node-prev inode)))
	       (next-duplicate-p (inode)
		 (and (node-next inode) 
		      (skey-matches skey (node-key (node-next inode)))))
	       (first-duplicate-p (inode)
		 (or (first-node-p inode)
		     (not (skey-matches skey (node-key inode)))))
	       (update-hash (inode) 
		 (when (first-duplicate-p inode)
		   (if (next-duplicate-p inode)
		       (setf (gethash skey (btree-hash index)) 
			     (node-next inode))
		       (remhash skey (btree-hash index))))))
	(loop 
	   for inode = (gethash skey (btree-hash index))
	   then (node-next inode)
	   while (skey-matches skey (node-key inode))
	   do
	   (when (value-matches-pnode inode)
	     (when (first-node-p inode)
	       (setf (btree-root index) (node-next inode)))
	     (update-hash inode)
	     (delete-node (node-prev inode) inode (node-next inode))
	     (return))
	   finally (error "Did not find indexed value: ~A with skey ~A" pnode skey))))))

(defun skey-matches (skey1 skey2)
  (elephant::lisp-compare-equal skey1 skey2))
	
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
    (values t 
	    (node-key (cursor-node cursor))
	    (node-value (cursor-node cursor)))))

;;
;; Cursor helpers
;;

(defun set-cursor (cursor node)
  (setf (cursor-initialized-p cursor) t)
  (setf (cursor-node cursor) node))

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
;; Standard cursor ops
;;


(defmethod cursor-first ((cursor prev-cursor))
  (cursor-setif-ret (cursor-root cursor)))

(defmethod cursor-last ((cursor prev-cursor))
  (with-search-condition (nil (null (node-next node)) node)
    (cursor-setif-ret 
     (search-condition 
      (if (cursor-initialized-p cursor)
	  (cursor-node cursor)
	  (cursor-root cursor))))))

(defmethod cursor-next ((cursor prev-cursor))
  (if (cursor-initialized-p cursor)
      (cursor-setif-ret (node-next (cursor-node cursor)))
      (cursor-first cursor)))

(defmethod cursor-prev ((cursor prev-cursor))
  (if (cursor-initialized-p cursor)
      (cursor-setif-ret (node-prev (cursor-node cursor)))
      (cursor-last cursor)))

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
      (cursor-set cursor key)
    (if (and exists? (elephant::lisp-compare-equal v value))  
	(values exists? k v)
	(progn
	  (setf (cursor-initialized-p cursor) nil)
	  nil))))

(defmethod cursor-delete ((cursor prev-cursor))
  (when (cursor-initialized-p cursor)
    (let ((node (cursor-node cursor)))
      (remove-kv (node-key node) (cursor-btree cursor))
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
;; Special behavior for index cursor operations
;;

(defclass prev-index-cursor (secondary-cursor prev-cursor) ()
  (:documentation "Cursor for traversing indexed btrees"))

(defmethod make-cursor ((index prev-btree-index))
  (make-instance 'prev-index-cursor
		 :btree index
		 :node nil
		 :oid (oid index)))

(defmethod cursor-current ((cursor prev-index-cursor))
  (when (cursor-initialized-p cursor)
    (let* ((node (cursor-node cursor))
	   (ref (node-value (cursor-node cursor))))
      (values t 
	      (node-key node)
	      (node-value (ibtref-node ref))))))

(defmethod cursor-pcurrent ((cursor prev-index-cursor))
  (when (cursor-initialized-p cursor)
    (let* ((node (cursor-node cursor))
	   (ref (node-value node)))
      (values t (node-key node)
	      (node-value (ibtref-node ref))
	      (node-key (ibtref-node ref))))))

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

(defun node-next-dup (node)
  (awhen (node-next node)
    (when (eq (node-key node) (node-key it))
      it)))

(defun node-next-nodup (node)
  (awhen (node-next node)
    (if (elephant::lisp-compare-equal (node-key node) (node-key it))
	(node-next-nodup it)
	it)))

(defun node-prev-dup (node)
  (awhen (node-prev node)
    (when (elephant::lisp-compare-equal (node-key node) (node-key it))
      it)))

(defun node-prev-nodup (node)
  (awhen (node-prev node)
    (if (eq (node-key node) (node-key it))
	(node-prev-nodup it)
	it)))


;;
;; Index cursor ops
;;

(defmethod cursor-pfirst ((cursor prev-index-cursor))
  (cursor-psetif-ret (cursor-root cursor)))

(defmethod cursor-plast ((cursor prev-index-cursor))
  (with-search-condition (nil (null (node-next node)) node)
    (cursor-psetif-ret (search-condition (cursor-root cursor)))))

(defmethod cursor-pnext ((cursor prev-index-cursor))
  (when (cursor-initialized-p cursor)
    (cursor-psetif-ret (node-next (cursor-node cursor)))))

(defmethod cursor-pprev ((cursor prev-index-cursor))
  (when (cursor-initialized-p cursor)
    (cursor-psetif-ret (node-prev (cursor-node cursor)))))

(defmethod cursor-pset ((cursor prev-index-cursor) key)
  (multiple-value-bind (node exists?)
      (gethash key (btree-hash (cursor-btree cursor)))
    (cursor-psetif-ret (and exists? node))))

(defmethod cursor-pset-range ((cursor prev-index-cursor) key)
  (with-search-condition (nil (> (node-key node) key) node)
    (multiple-value-bind (node exists?)
	(gethash key (btree-hash (cursor-btree cursor)))
      (cursor-psetif-ret 
       (or (and exists? node) 
	   (search-condition (cursor-root cursor)))))))

(defun node-has-pkey (inode pkey)
  (lisp-compare-equal 
   (node-key (ibtref-node (node-value inode)))
   pkey))

(defmethod cursor-pget-both ((cursor prev-index-cursor) key pkey)
  (with-search-condition (nil (node-has-pkey node pkey) node)
    (multiple-value-bind (exists? key value)
	(cursor-set cursor key)
      (declare (ignore key value))
      (when exists?
	(cursor-psetif-ret (search-condition (cursor-node cursor)))))))

(defmethod cursor-pget-both-range ((cursor prev-index-cursor) key pkey)
  (declare (ignore key pkey))
  (warn "pget-both-range not supported by :PREVALENCE data store due to
         primary keys (index values) not being ordered in indices")
  nil)

(defmethod cursor-delete ((cursor prev-index-cursor))
  (if (cursor-initialized-p cursor)
      (let ((ref (node-value (cursor-node cursor))))
	(remove-kv (node-key (ibtref-node ref)) (ibtref-btree ref)))
      (cerror "Continue with process?"
	      "Cannot delete with an uninitialized cursor (on ~A)" (cursor-btree cursor))))

;; Dealing with duplicates

(defmethod cursor-next-dup ((cursor prev-index-cursor))
  (when (cursor-initialized-p cursor)
    (cursor-setif-ret (node-next-dup (cursor-node cursor)))))

(defmethod cursor-pnext-dup ((cursor prev-index-cursor))
  (when (cursor-initialized-p cursor)
    (cursor-psetif-ret (node-next-dup (cursor-node cursor)))))

(defmethod cursor-next-nodup ((cursor prev-index-cursor))
  (if (cursor-initialized-p cursor)
      (cursor-setif-ret (node-next-nodup (cursor-node cursor)))
      (cursor-first cursor)))
    
(defmethod cursor-pnext-nodup ((cursor prev-index-cursor))
  (if (cursor-initialized-p cursor)
      (cursor-psetif-ret (node-next-nodup (cursor-node cursor)))
      (cursor-plast cursor)))

(defmethod cursor-prev-dup ((cursor prev-index-cursor))
  (when (cursor-initialized-p cursor)
    (cursor-setif-ret (node-prev-dup (cursor-node cursor)))))

(defmethod cursor-pprev-dup ((cursor prev-index-cursor))
  (when (cursor-initialized-p cursor)
    (cursor-psetif-ret (node-prev-dup (cursor-node cursor)))))

(defmethod cursor-prev-nodup ((cursor prev-index-cursor))
  (if (cursor-initialized-p cursor)
      (cursor-setif-ret (node-prev-nodup (cursor-node cursor)))
      (cursor-last cursor)))

(defmethod cursor-pprev-nodup ((cursor prev-index-cursor))
  (if (cursor-initialized-p cursor)
      (cursor-psetif-ret (node-prev-nodup (cursor-node cursor)))
      (cursor-plast cursor)))

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
  (unless (null prev-node)
    (setf (node-next prev-node) (node-next old-node)))
  (unless (null next-node)
    (setf (node-prev next-node) (node-prev old-node))))

