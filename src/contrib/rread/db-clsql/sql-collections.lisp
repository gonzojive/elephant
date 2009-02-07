;;; -*- Mode: Lisp; Syntax: ANSI-Common-Lisp; Base: 10 -*-
;;;
;;; sql-controller.lisp -- Interface to a CLSQL based object store.
;;; 
;;; Initial version 10/12/2005 by Robert L. Read
;;; <read@robertlread.net>
;;; 
;;; part of
;;;
;;; Elephant: an object-oriented database for Common Lisp
;;;
;;; Copyright (c) 2005-2007 by Robert L. Read
;;; <rread@common-lisp.net>
;;;
;;; Elephant users are granted the rights to distribute and use this software
;;; as governed by the terms of the Lisp Lesser GNU Public License
;;; (http://opensource.franz.com/preamble.html), also known as the LLGPL.
;;;

(in-package :db-clsql)

(defun array-index-if (p a)
  (do ((i 0 (1+ i)))
      ((or (not (array-in-bounds-p a i))
	(funcall p (aref a i)))
       (if (and (array-in-bounds-p a i) (funcall p (aref a i)))
	   i
	   -1)))
)

(defmethod get-value (key (bt sql-btree-index))
  "Get the value in the primary DB from a secondary key."
  (declare (optimize (speed 3)))
      ;; Below, the take the oid and add it to the key, then look
      ;; thing up--- where?

      ;; Somehow I suspect that what I am getting back here 
      ;; is actually the main key...
  (let* ((sc (get-con bt)))
      (let ((pk (sql-get-from-clcn (oid bt) key  sc)))
	(if pk 
	    (sql-get-from-clcn (oid (primary bt)) pk sc))
	)))

(defmethod get-primary-key (key (bt sql-btree-index))
  (declare (optimize (speed 3)))
      (let* ((sc (get-con bt))
	     )
	(sql-get-from-clcn (oid bt) key sc)))


;; My basic strategy is to keep track of a current key
;; and to store all keys in memory so that we can sort them
;; to implement the cursor semantics.  Clearly, passing 
;; in a different ordering is a nice feature to have here.
(defclass sql-cursor (cursor)
  ((keys :accessor sql-crsr-ks :initarg :sql-cursor-keys :initform '())
   (curkey :accessor sql-crsr-ck :initarg :sql-cursor-curkey :initform -1 :type (or null integer)))
  (:documentation "A SQL cursor for traversing (primary) BTrees."))

(defmethod make-cursor ((bt sql-btree))
  "Make a cursor from a btree."
  (declare (optimize (speed 3)))
  (make-instance 'sql-cursor 
		 :btree bt
		 :oid (oid bt)))



(defmethod cursor-close ((cursor sql-cursor))
  (setf (sql-crsr-ck cursor) nil)
  (setf (cursor-initialized-p cursor) nil))

;; Maybe this will still work?
;; I'm not sure what cursor-duplicate is meant to do, and if 
;; the other state needs to be copied or now.  Probably soo...
(defmethod cursor-duplicate ((cursor sql-cursor))
  (declare (optimize (speed 3)))
  (make-instance (type-of cursor)
		 :initialized-p (cursor-initialized-p cursor)
		 :oid (cursor-oid cursor)
		 ;; Do we need to so some kind of copy on this collection?
		 :keys (sql-crsr-ks cursor)
		 :curkey (sql-crsr-ck cursor)))
;;		 :handle (db-cursor-duplicate 
;;			  (cursor-handle cursor) 
;;			  :position (cursor-initialized-p cursor))))

(defmethod cursor-current ((cursor sql-cursor))
  (declare (optimize (speed 3)))
  (when (cursor-initialized-p cursor)
    (has-key-value cursor)))

;; Only for use within an operation...
(defun my-generic-less-than (a b)
  (lisp-compare< a b))


(defun my-generic-at-most (a b)
  (lisp-compare<= a b))

(defmethod cursor-un-init ((cursor sql-cursor) &key (returnpk nil))
  (setf (cursor-initialized-p cursor) nil)
  (if returnpk
      (values nil nil nil nil)
      (values nil nil nil)))

(clsql::locally-enable-sql-reader-syntax)

(defmethod cursor-init ((cursor sql-cursor))
  (let* ((sc (get-con (cursor-btree cursor)))
	 (con (controller-db sc))
	 (tuples
	  (clsql:select [key] [value]
		  :from [keyvalue]
		  :where [= [clctn_id] (oid (cursor-btree cursor))] 
		  :database con
		  ))
	 (len (length tuples)))
    ;; now we somehow have to load the keys into the array...
    ;; actually, this should be an adjustable vector...
    (setf (sql-crsr-ks cursor) (make-array (length tuples)))
    (do ((i 0 (1+ i))
	 (tup tuples (cdr tup)))
	((= i len) nil)
      (setf (aref (sql-crsr-ks cursor) i)
	    (deserialize-from-base64-string (caar tup) sc)))
    (sort (sql-crsr-ks cursor) #'my-generic-less-than)
    (setf (sql-crsr-ck cursor) 0)
    (setf (cursor-initialized-p cursor) t)
    ))

(clsql::restore-sql-reader-syntax-state) 

;; we're assuming here that nil is not a legitimate key.
(defmethod get-current-key ((cursor sql-cursor))
  (let ((x (sql-crsr-ck cursor)))
    (if (and (>= x 0) (< x (length (sql-crsr-ks cursor))))
	(svref (sql-crsr-ks cursor) x)
	'()
	))
  )

(defmethod get-current-value ((cursor sql-cursor))
  (let ((key (get-current-key cursor)))
    (if key
	(get-value key (cursor-btree cursor))
	'())))

(defmethod has-key-value ((cursor sql-cursor))
  (let ((key (get-current-key cursor)))
    (if key
	(values t key (get-value key (cursor-btree cursor)))
	(cursor-un-init cursor))))

 

(defmethod cursor-first ((cursor sql-cursor))
  (declare (optimize (speed 3)))
  ;; Read all of the keys...
  ;; We need to get the contoller db from the btree somehow...
  (cursor-init cursor)
  (has-key-value cursor)
  )

		 
;;A bit of a hack.....

;; If you run off the end, this can set cursor-initalized-p to nil.
(defmethod cursor-last ((cursor sql-cursor) )
  (unless (cursor-initialized-p cursor)
    (cursor-init cursor))
  (setf (sql-crsr-ck cursor) 
	(- (length (sql-crsr-ks cursor)) 1))
  (setf (cursor-initialized-p cursor) t)
  (has-key-value cursor))



(defmethod cursor-next ((cursor sql-cursor))
  (if (cursor-initialized-p cursor)
      (progn
	(incf (sql-crsr-ck cursor))
	(has-key-value cursor))
      (cursor-first cursor)))
	  
(defmethod cursor-prev ((cursor sql-cursor))
  (declare (optimize (speed 3)))
  (if (cursor-initialized-p cursor)
      (progn
	(decf (sql-crsr-ck cursor))
	(has-key-value cursor))
      (cursor-last cursor)))
	  
(defmethod cursor-set ((cursor sql-cursor) key)
  (declare (optimize (speed 3)))
  (unless  (cursor-initialized-p cursor)
	 (cursor-init cursor))
       (let ((p (position key (sql-crsr-ks cursor) :test #'equal :from-end nil)))
	 (if p
	     (progn
	       (setf (sql-crsr-ck cursor) p)
	       (setf (cursor-initialized-p cursor) t)	  
	       (has-key-value cursor)
	       )
	     (setf (cursor-initialized-p cursor) nil)))
       )
  

(defmethod cursor-set-range ((cursor sql-cursor) key)
  (declare (optimize (speed 3)))
  (unless (cursor-initialized-p cursor)
    (cursor-init cursor))
  (multiple-value-bind (h k v)
      (cursor-first cursor)
  (let ((len (length (sql-crsr-ks cursor)))
	(vs '()))
    (do ((i 0 (1+ i)))
	((or (= i len) (lisp-compare<= key k)
	     vs)
;; This is the return value...
	 (if (= i len) ;; we failed to find a spot
	     (cursor-un-init cursor)
	     (cursor-current cursor)))
      (progn
	(multiple-value-bind (h k v)
	    (cursor-next cursor)
	  (declare (ignore h v))
	  (when (lisp-compare<= key k)
	    (setf vs t))
	  )
	)))))




(defmethod cursor-get-both ((cursor sql-cursor) key value)
;;  (declare (optimize (speed 3)))
  (let* ((bt (cursor-btree cursor))
	 (v (get-value key bt)))
    (if (lisp-compare-equal v value)
;; We need to leave this cursor properly posistioned....
;; For a secondary cursor it's harder, but for this, it's simple
	(cursor-set cursor key)
	(cursor-un-init cursor))))

;; This needs to be rewritten!
(defmethod cursor-get-both-range ((cursor sql-cursor) key value)
  (declare (optimize (speed 3)))
  (let* ((bt (cursor-btree cursor))
	 (v (get-value key bt)))
    ;; Since we don't allow duplicates in primary cursors, I 
    ;; guess this is all that needs to be done!
    ;; If there were a test to cover this, the semantics would be clearer...
    (if (lisp-compare-equal v value)
	(cursor-set cursor key)
	(cursor-un-init cursor))))

(defmethod cursor-delete ((cursor sql-cursor))
  (declare (optimize (speed 3)))
  (if (cursor-initialized-p cursor)
      (multiple-value-bind 
       (has k v) 
       (cursor-current cursor)
       (declare (ignore has v))
       ;; Now I need to suck the value out of the cursor, somehow....
       (remove-kv k (cursor-btree cursor)))
      (error "Can't delete with uninitialized cursor!")))


;; This needs to be changed!
(defmethod cursor-put ((cursor sql-cursor) value &key (key nil key-specified-p))
  "Put by cursor.  Not particularly useful since primaries
don't support duplicates.  Currently doesn't properly move
the cursor."
  (declare (optimize (speed 3))
	   (ignore key value key-specified-p))
  (error "Puts on sql-cursors are not yet implemented, because I can't get them to work on BDB cursors!"))

