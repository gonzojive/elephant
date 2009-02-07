;;; -*- Mode: Lisp; Syntax: ANSI-Common-Lisp; Base: 10 -*-
;;;
;;; sql-dupbtree.lisp -- An implementation of duplicate btrees
;;; 
;;; Initial version 6/17/2008 by Robert L. Read
;;; 
;;; part of
;;;
;;; Elephant: an object-oriented database for Common Lisp
;;;
;;; Copyright (c) 2008 by Robert L. Read
;;;
;;; Elephant users are granted the rights to distribute and use this software
;;; as governed by the terms of the Lisp Lesser GNU Public License
;;; (http://opensource.franz.com/preamble.html), also known as the LLGPL.
;;;


(in-package :db-clsql)

(defclass sql-dup-btree (sql-btree) ()
  (:documentation "A SQL implementation of a duplicate-BTree"))

(defmethod build-dup-btree ((sc sql-store-controller))
  (make-instance 'sql-dup-btree :sc sc))


;; Secondary Cursors
(defclass sql-secondary-cursor (sql-cursor) 
  ((dup-number :accessor dp-nmbr :initarg :dup-number :initform 0 :type integer))
  (:documentation "Cursor for traversing bdb secondary indices."))



(defmethod make-cursor ((bt sql-btree-index))
  "Make a secondary-cursor from a secondary index."
  (declare (optimize (speed 3)))
  (make-instance 'sql-secondary-cursor 
		 :btree bt
		 :oid (oid bt)))


(defmethod has-key-value-scnd ((cursor sql-secondary-cursor) &key (returnpk nil))
  (let ((ck (sql-crsr-ck cursor)))
    (if (and (>= ck  0) (< ck  (length (sql-crsr-ks cursor))))
	(let* ((cur-pk (aref (sql-crsr-ks cursor)
			     (sql-crsr-ck cursor)))
	       (sc (get-con (cursor-btree cursor)))
	       (indexed-pk (sql-get-from-clcn-nth (cursor-oid cursor) cur-pk 
						  sc
						  (dp-nmbr cursor))))
	  (if indexed-pk
	      (let ((v (get-value indexed-pk (primary (cursor-btree cursor)))))
		(if v
		    (if returnpk
			(values t cur-pk v indexed-pk)
			(values t cur-pk v))
		    (cursor-un-init cursor :returnpk returnpk)))
	      (cursor-un-init cursor :returnpk returnpk)))
	(progn
	  (cursor-un-init cursor :returnpk returnpk)))))

(defmethod cursor-current ((cursor sql-secondary-cursor) )
  (cursor-current-x cursor))

(defmethod cursor-current-x ((cursor sql-secondary-cursor) &key (returnpk nil))
  (has-key-value-scnd cursor :returnpk returnpk)
  )

(defmethod cursor-pcurrent ((cursor sql-secondary-cursor))
  (cursor-current-x cursor :returnpk t))

(defmethod cursor-pfirst ((cursor sql-secondary-cursor))
  (cursor-first-x cursor :returnpk t))

(defmethod cursor-plast ((cursor sql-secondary-cursor))
  (cursor-last-x cursor :returnpk t))

(defmethod cursor-pnext ((cursor sql-secondary-cursor))
  (cursor-next-x cursor :returnpk t))
	  
(defmethod cursor-pprev ((cursor sql-secondary-cursor))
  (cursor-prev-x cursor :returnpk t))
	  
(defmethod cursor-pset ((cursor sql-secondary-cursor) key)
  (declare (optimize (speed 3)))
  (unless (cursor-initialized-p cursor)
    (cursor-init cursor))
  (let ((idx (position key (sql-crsr-ks cursor) :test #'equal)))
    (if idx
        (progn
          (setf (sql-crsr-ck cursor) idx)
          (setf (dp-nmbr cursor) 0)
          (cursor-current-x cursor :returnpk t))
        (cursor-un-init cursor)
        )))


(defmethod cursor-pset-range ((cursor sql-secondary-cursor) key)
  (declare (optimize (speed 3)))
  (unless (cursor-initialized-p cursor)
    (cursor-init cursor))
  (let ((idx (array-index-if #'(lambda (x) (my-generic-at-most key x)) (sql-crsr-ks cursor))))
    (if (<= 0 idx)
	(progn
	  (setf (sql-crsr-ck cursor) idx)
	  (setf (dp-nmbr cursor) 0)
	  (cursor-current-x cursor :returnpk t)
	  )
	(cursor-un-init cursor :returnpk t)
	)))


;; Moves the cursor to a the first secondary key / primary key pair, 
;; with secondary key equal to the key argument, and primary key greater or equal to the pkey argument.
;; Returns has-tuple / secondary key / value / primary key.
(defmethod cursor-pget-both ((cursor sql-secondary-cursor) key pkey)
  (declare (optimize (speed 3)))
  ;; It's better to get the value by the primary key, 
  ;; as that is unique..
  (let* ((bt (primary (cursor-btree cursor)))
	 (v (get-value pkey bt)))
    ;; Now, bascially we set the cursor to the key and
    ;; andvance it until we get the value that we want...
    (if v
	(do ((vs 
	      (multiple-value-list (cursor-set cursor key))
	      (multiple-value-list (cursor-next cursor))))
	    ((or (null (car vs)) ;; We ran off the end..
		 (not (equal key (cadr vs))) ;; We ran out of values matching this key..
		 (equal v (caddr vs))) ;; we found what we are loodking for!
	     ;; our return condition...
	     (if (equal v (caddr vs))
		 (cursor-current-x cursor :returnpk t)
		 (cursor-un-init cursor :returnpk t))
	     )
	  ;; Here's a body that's nice for debugging...
	  )
	;; If we don't get a value, we have to un-init this cursor...
	(cursor-un-init cursor :returnpk t))))

(defmethod cursor-pget-both-range ((cursor sql-secondary-cursor) key pkey)
  (declare (optimize (speed 3)))
  ;; It's better to get the value by the primary key, 
  ;; as that is unique..
  (do ((vs 
	(append (multiple-value-list (cursor-set cursor key)) (list pkey))
	(multiple-value-list (cursor-next-x cursor :returnpk t))))
      ((or (null (car vs)) ;; We ran off the end..
	   (not (equal key (cadr vs))) ;; We ran out of values matching this key..
	   (equal pkey (caddr vs)) ;; we found what we are loodking for!
	   (my-generic-less-than ;; we went beond the pkey
	    pkey
	    (cadddr vs)
	    )
	   ) 
       ;; our return condition...
       (if (or (equal pkey (caddr vs))
	       (my-generic-less-than ;; we went beond the pkey
		pkey
		(cadddr vs)
		))
	   (cursor-current-x cursor :returnpk t)
	   (cursor-un-init cursor :returnpk t))
       )
    ))


(defmethod cursor-delete ((cursor sql-secondary-cursor))
  "Delete by cursor: deletes ALL secondary indices."
  (declare (optimize (speed 3)))
  (if (cursor-initialized-p cursor)
      (multiple-value-bind 
	    (m k v p) 
	  (cursor-current-x cursor :returnpk t)
	(declare (ignore m k v))
	(remove-kv p (primary (cursor-btree cursor)))
	(let ((ck (sql-crsr-ck cursor))
	      (dp (dp-nmbr cursor)))
	  (declare (ignorable dp))
	  (cursor-next cursor)
	  ;; Now that we point to the old slot, remove the old slot from the array...
	  (setf (sql-crsr-ks cursor)
		(remove-indexed-element-and-adjust 
		 ck
		 (sql-crsr-ks cursor)))
	  ;; now move us back to where we were
	  (cursor-prev cursor)
	  ))
      (error "Can't delete with uninitialized cursor!")))

(defmethod cursor-first ((cursor sql-secondary-cursor))
  (cursor-first-x cursor)
  )

(defmethod cursor-first-x ((cursor sql-secondary-cursor) &key (returnpk nil))
  (declare (optimize (speed 3)))
  (setf (dp-nmbr cursor) 0)
  (cursor-init cursor)
  (has-key-value-scnd cursor :returnpk returnpk)
  )

(defmethod cursor-next ((cursor sql-secondary-cursor))
  (cursor-next-x cursor)
  )

(defmethod cursor-next-x ((cursor sql-secondary-cursor) &key (returnpk nil))
  (if (cursor-initialized-p cursor)
      (progn
	(let ((cur-pk (get-current-key cursor)))
	  (incf (sql-crsr-ck cursor))
	  (if (lisp-compare-equal cur-pk (get-current-key cursor))
	      (incf (dp-nmbr cursor))
	      (setf (dp-nmbr cursor) 0))
	  (has-key-value-scnd cursor :returnpk returnpk)))
      (cursor-first-x cursor :returnpk returnpk)))
	  
(defmethod cursor-prev ((cursor sql-secondary-cursor))
  (cursor-prev-x cursor)
  )
(defmethod cursor-prev-x ((cursor sql-secondary-cursor)  &key (returnpk nil))
  (declare (optimize (speed 3)))
  (if (cursor-initialized-p cursor)
      (progn
	(let ((prior-pk (get-current-key cursor)))
	  (decf (sql-crsr-ck cursor))
	  (if (lisp-compare-equal prior-pk (get-current-key cursor))
	      (setf (dp-nmbr cursor) (max 0 (- (dp-nmbr cursor) 1)))
	      (setf (dp-nmbr cursor)
		    (1- (sql-get-from-clcn-cnt (cursor-oid cursor)
					       (get-current-key cursor)
					       (get-con (cursor-btree cursor))))
		    )))
	(has-key-value-scnd cursor :returnpk returnpk))
      (cursor-last-x cursor :returnpk returnpk)))

(defmethod cursor-next-dup ((cursor sql-secondary-cursor))
  (cursor-next-dup-x cursor)
  )

(defmethod cursor-pnext-dup ((cursor sql-secondary-cursor))
  (cursor-next-dup-x cursor :returnpk t)
  )

(defmethod cursor-next-dup-x ((cursor sql-secondary-cursor) &key (returnpk nil))
  ;;  (declare (optimize (speed 3)))
  (when (cursor-initialized-p cursor)
    (let* ((cur-pk (aref (sql-crsr-ks cursor)
			 (sql-crsr-ck cursor)))
	   (nint (+ 1 (sql-crsr-ck cursor)))
	   (nxt-pk (if (array-in-bounds-p (sql-crsr-ks cursor) nint) 
		       (aref (sql-crsr-ks cursor)
			     nint)
		       -1
		       ))
	   )
      (if (equal cur-pk nxt-pk)
	  (progn
	    (incf (dp-nmbr cursor))
	    (incf (sql-crsr-ck cursor))
	    (has-key-value-scnd cursor :returnpk returnpk))
	  (progn
	    (setf (dp-nmbr cursor) 0)
	    (cursor-un-init cursor :returnpk returnpk)
	    )))))

(defmethod cursor-next-nodup ((cursor sql-secondary-cursor))
  (cursor-next-nodup-x cursor)
  )	  
(defmethod cursor-next-nodup-x ((cursor sql-secondary-cursor) &key (returnpk nil))
  (declare (ignore returnpk))
  (if (cursor-initialized-p cursor)
      (let ((n
	     (do ((i (sql-crsr-ck cursor) (1+ i)))
		 ((or 
		   (not (array-in-bounds-p (sql-crsr-ks cursor) (+ i 1)))
		   (not 
		    (equal (aref (sql-crsr-ks cursor) i)
			   (aref (sql-crsr-ks cursor) (+ 1 i)))))
		  (+ 1 i)))))
	(setf (sql-crsr-ck cursor) n)
	(setf (dp-nmbr cursor) 0)
	(has-key-value-scnd cursor :returnpk returnpk))
      (cursor-first-x cursor :returnpk returnpk)
      ))

(defmethod cursor-last ((cursor sql-secondary-cursor))
  (cursor-last-x cursor)
  )
(defmethod cursor-last-x ((cursor sql-secondary-cursor) &key (returnpk nil))
  (unless (cursor-initialized-p cursor)
    (cursor-init cursor))
  (setf (sql-crsr-ck cursor) 
	(- (length (sql-crsr-ks cursor)) 1))
  (setf (dp-nmbr cursor) 
	(max 0
	     (- (sql-get-from-clcn-cnt 
		 (cursor-oid cursor)
		 (get-current-key cursor)
		 (get-con (cursor-btree cursor))
		 )
		1)))
  (assert (>= (dp-nmbr cursor) 0))
  (setf (cursor-initialized-p cursor) t)
  (has-key-value-scnd cursor :returnpk returnpk)
  )



(defmethod cursor-prev-nodup ((cursor sql-secondary-cursor))
  (cursor-prev-nodup-x cursor)
  )
(defmethod cursor-prev-nodup-x ((cursor sql-secondary-cursor) &key (returnpk nil))
  (declare (optimize (speed 3)))
  (if (cursor-initialized-p cursor)
      (progn
	(setf (sql-crsr-ck cursor) (- (sql-crsr-ck cursor) (+ 1 (dp-nmbr cursor))))
	(setf (dp-nmbr cursor) 
	      (max 0
		   (- (sql-get-from-clcn-cnt (cursor-oid cursor)
					     (get-current-key cursor)
					     (get-con (cursor-btree cursor))
					     ) 1)))
	(has-key-value-scnd cursor :returnpk returnpk))
      (cursor-last-x cursor :returnpk returnpk)))


(defmethod cursor-pnext-nodup ((cursor sql-secondary-cursor))
  (cursor-next-nodup-x cursor :returnpk t))

(defmethod cursor-pprev-nodup ((cursor sql-secondary-cursor))
  (cursor-prev-nodup-x cursor :returnpk t))




(defmethod remove-kv-pair (k v (bt sql-dup-btree))
  (let* ((sc (get-con bt)))
    (sql-remove-key-and-value-from-clcn (oid bt)
					k
					v
					sc
					)))




;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Begin sql-dup-cursor
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defclass sql-dup-cursor (sql-cursor) 
  ((dup-number :accessor dp-nmbr :initarg :dup-number :initform 0 :type integer))
  (:documentation "A SQL cursor for traversing duplciate-BTrees."))

(defmethod cursor-init ((cursor sql-dup-cursor))
  (setf (dp-nmbr cursor) 0)
  (call-next-method)
  )
 
(defmethod make-cursor ((bt sql-dup-btree))
  "Make a secondary-cursor from a secondary index."
  (declare (optimize (speed 3)))
  (make-instance 'sql-dup-cursor 
		 :btree bt
		 :oid (oid bt)))

(defmethod cursor-current ((cursor sql-dup-cursor) )
  (cursor-current-x cursor))

(defmethod cursor-current-x ((cursor sql-dup-cursor) &key (returnpk nil))
  (declare (ignore returnpk))
  (has-key-value cursor))


(defun array-index-if (p a)
  (do ((i 0 (1+ i)))
      ((or (not (array-in-bounds-p a i))
	   (funcall p (aref a i)))
       (if (and (array-in-bounds-p a i) (funcall p (aref a i)))
	   i
	   -1))))


(defmethod cursor-delete ((cursor sql-dup-cursor))
  "Delete by cursor: deletes ALL secondary indices."
  (declare (optimize (speed 3)))
  (if (cursor-initialized-p cursor)
      (multiple-value-bind 
	    (m k v p) 
	  (cursor-current-x cursor)
	(declare (ignore m k v))
	(remove-kv p (cursor-btree cursor))
	(let ((ck (sql-crsr-ck cursor))
	      (dp (dp-nmbr cursor)))
	  (declare (ignorable dp))
	  (cursor-next cursor)
	  ;; Now that we point to the old slot, remove the old slot from the array...
	  (setf (sql-crsr-ks cursor)
		(remove-indexed-element-and-adjust 
		 ck
		 (sql-crsr-ks cursor)))
	  ;; now move us back to where we were
	  (cursor-prev cursor)
	  ))
      (error "Can't delete with uninitialized cursor!")))

(defmethod cursor-first ((cursor sql-dup-cursor))
  (cursor-first-x cursor)
  )

(defmethod cursor-first-x ((cursor sql-dup-cursor) &key (returnpk nil))
  (declare (optimize (speed 3)))
  (setf (dp-nmbr cursor) 0)
  (cursor-init cursor)
  (has-key-value cursor)
  )

(defmethod cursor-next ((cursor sql-dup-cursor))
  (cursor-next-x cursor)
  )

(defmethod cursor-next-x ((cursor sql-dup-cursor) &key (returnpk nil))
  (if (cursor-initialized-p cursor)
      (progn
	(let ((cur-pk (get-current-key cursor)))
	  (incf (sql-crsr-ck cursor))
	  (if (lisp-compare-equal cur-pk (get-current-key cursor))
	      (incf (dp-nmbr cursor))
	      (setf (dp-nmbr cursor) 0))
	  (has-key-value cursor)))
      (cursor-first-x cursor)))
	  
(defmethod cursor-prev ((cursor sql-dup-cursor))
  (cursor-prev-x cursor)
  )
(defmethod cursor-prev-x ((cursor sql-dup-cursor)  &key (returnpk nil))
  (declare (optimize (speed 3)))
  (if (cursor-initialized-p cursor)
      (progn
	(let ((prior-pk (get-current-key cursor)))
	  (decf (sql-crsr-ck cursor))
	  (if (lisp-compare-equal prior-pk (get-current-key cursor))
	      (setf (dp-nmbr cursor) (max 0 (- (dp-nmbr cursor) 1)))
	      (setf (dp-nmbr cursor)
		    (1- (sql-get-from-clcn-cnt (cursor-oid cursor)
					       (get-current-key cursor)
					       (get-con (cursor-btree cursor))))
		    )))
	(has-key-value cursor))
      (cursor-last-x cursor)))

(defmethod cursor-next-dup ((cursor sql-dup-cursor))
  (cursor-next-dup-x cursor)
  )

(defmethod cursor-next-dup-x ((cursor sql-dup-cursor) &key (returnpk nil))
  ;;  (declare (optimize (speed 3)))
  (when (cursor-initialized-p cursor)
    (let* ((cur-pk (aref (sql-crsr-ks cursor)
			 (sql-crsr-ck cursor)))
	   (nint (+ 1 (sql-crsr-ck cursor)))
	   (nxt-pk (if (array-in-bounds-p (sql-crsr-ks cursor) nint) 
		       (aref (sql-crsr-ks cursor)
			     nint)
		       -1
		       ))
	   )
      (if (equal cur-pk nxt-pk)
	  (progn
	    (incf (dp-nmbr cursor))
	    (incf (sql-crsr-ck cursor))
	    (has-key-value cursor))
	  (progn
	    (setf (dp-nmbr cursor) 0)
	    (cursor-un-init cursor)
	    )))))

(defmethod cursor-next-nodup ((cursor sql-dup-cursor))
  (cursor-next-nodup-x cursor)
  )	  
(defmethod cursor-next-nodup-x ((cursor sql-dup-cursor) &key (returnpk nil))
  (if (cursor-initialized-p cursor)
      (let ((n
	     (do ((i (sql-crsr-ck cursor) (1+ i)))
		 ((or 
		   (not (array-in-bounds-p (sql-crsr-ks cursor) (+ i 1)))
		   (not 
		    (equal (aref (sql-crsr-ks cursor) i)
			   (aref (sql-crsr-ks cursor) (+ 1 i)))))
		  (+ 1 i)))))
	(setf (sql-crsr-ck cursor) n)
	(setf (dp-nmbr cursor) 0)
	(has-key-value cursor))
      (cursor-first-x cursor)
      ))

(defmethod cursor-last ((cursor sql-dup-cursor))
  (cursor-last-x cursor)
  )
(defmethod cursor-last-x ((cursor sql-dup-cursor) &key (returnpk nil))
  (unless (cursor-initialized-p cursor)
    (cursor-init cursor))
  (setf (sql-crsr-ck cursor) 
	(- (length (sql-crsr-ks cursor)) 1))
  (setf (dp-nmbr cursor) 
	(max 0
	     (- (sql-get-from-clcn-cnt 
		 (cursor-oid cursor)
		 (get-current-key cursor)
		 (get-con (cursor-btree cursor))
		 )
		1)))
  (assert (>= (dp-nmbr cursor) 0))
  (setf (cursor-initialized-p cursor) t)
  (has-key-value cursor)
  )



(defmethod cursor-prev-nodup ((cursor sql-dup-cursor))
  (cursor-prev-nodup-x cursor)
  )
(defmethod cursor-prev-nodup-x ((cursor sql-dup-cursor) &key (returnpk nil))
  (declare (optimize (speed 3)))
  (if (cursor-initialized-p cursor)
      (progn
	(setf (sql-crsr-ck cursor) (- (sql-crsr-ck cursor) (+ 1 (dp-nmbr cursor))))
	(setf (dp-nmbr cursor) 
	      (max 0
		   (- (sql-get-from-clcn-cnt (cursor-oid cursor)
					     (get-current-key cursor)
					     (get-con (cursor-btree cursor))
					     ) 1)))
	(has-key-value cursor))
      (cursor-last-x cursor)))

(defmethod has-key-value ((cursor sql-dup-cursor))
  (let ((key (get-current-key cursor)))
    (if key
	(values t key 
		(let* ((bt
			(cursor-btree cursor))
		       (sc (get-con bt))
		       (n (dp-nmbr cursor)))
		  (sql-get-from-clcn-nth (oid bt) key sc n)
		  )
		;; (get-value key (cursor-btree cursor))
		)
	(cursor-un-init cursor))))
 
(defmethod (setf get-value) (value key (bt sql-dup-btree))
  (let* ((sc (get-con bt)))
    (sql-add-to-clcn (oid bt) key value sc :insert-only t)
    )
  )

;; I don't know why this should be needed...
(defmethod get-value (key (bt sql-dup-btree))
    (sql-get-from-clcn (oid bt) key (get-con bt))
  )

