(in-package :db-postmodern)

(defvar *cursor-window-size* 10)

(defclass pm-cursor (cursor)
  ((key :accessor current-key-of :initform nil :initarg key)
   (value :accessor current-value-of :initform nil :initarg value)   
   (current-row :accessor current-row-of :initform nil :initarg current-row)
   (rows :accessor cached-rows-of :initform nil)
   (prior-rows :accessor cached-prior-rows-of :initform nil)))

(defmethod make-cursor ((bt pm-btree))
  (make-instance 'pm-cursor
		 :btree bt
		 :oid (oid bt)))

(defmethod cursor-duplicate ((cursor pm-cursor))
  (make-instance (type-of cursor)
		 :btree bt
		 :oid (oid bt)
		 :current-row (current-row-of cursor)
		 :key (current-key-of cursor)
		 :value (current-value-of cursor)))
;; we do not copy cached rows because they are mutable..		 

(defmethod print-object ((cursor pm-cursor) stream)
  (print-unreadable-object (cursor stream :type t :identity t)
    (format stream "cur-key:~S cur-val:~S"
            (current-key-of cursor)
            (current-value-of cursor))))

(defun ensure-string (o)
  (declare (optimize speed))
  (if (stringp o)
      o
      (let ((*print-pretty* nil)) (princ-to-string o))))    

(defmethod cursor-initialized-p ((cursor pm-cursor)) (current-row-of cursor))

(defmethod cursor-fetch-query ((cursor pm-cursor) &key
			       (key-compare '>) (value-compare :auto)
			       (key-order "ASC") (value-order :auto)
			       (limit *cursor-window-size*))
  "hardcore query-builder. value-compare is handled like ((qi = $1) AND (value OP $2))"
  (let ((da (duplicates-allowed-p (cursor-btree cursor))))
    (when (eq value-compare :auto) (setf value-compare (when da key-compare )))
    (when (eq value-order :auto) (setf value-order (when da key-order))))

  (let* ((table (table-of (cursor-btree cursor)))
	 (what (if (and +join-with-blob-optimization+ (eq :object (value-type-of (cursor-btree cursor))))
		   (format nil "SELECT qi, bob, value FROM ~a INNER JOIN blob ON bid = value" table)
		   (format nil "SELECT qi, value FROM ~a" table)))
	 (where (cond 
		  ((and key-compare value-compare)
		   (format nil "WHERE (qi ~a $1) OR ((qi = $1) AND (value ~a $2))" 
			   key-compare value-compare))
		  (key-compare (format nil "WHERE qi ~a $1" key-compare))
		  (value-compare (format nil "WHERE (qi = $1) AND (value ~a $2)" value-compare))
		  (t "")))
	 (order (cond
		  ((and key-order value-order) (format nil "ORDER BY qi ~a, value ~a" key-order value-order))
		  (key-order (format nil "ORDER BY qi ~a" key-order))
		  (value-order (format nil "ORDER BY val ~a" value-order))
		  (t "")))
	 (limit (if limit (format nil "LIMIT ~a" limit) "")))
    (format nil "~a ~a ~a ~a" what where order limit)))

(defmethod current-raw-key-of ((cursor pm-cursor))
  (first (current-row-of cursor)))

(defmethod current-raw-value-of ((cursor pm-cursor))
  (or (third (current-row-of cursor)) (second (current-row-of cursor))))

(defmethod cursor-current-query-params-auto ((cursor pm-cursor))
  (with-slots (btree) cursor (list (ensure-string (current-raw-key-of cursor)))))

(defmethod cursor-fetch ((cursor pm-cursor) query params &key reset-cache cache-prior)
  "execute query to fetch stuff into cache."
  (when reset-cache
    (setf (cached-prior-rows-of cursor) nil
	  (cached-rows-of cursor) nil
	  (current-row-of cursor) nil))
  (with-vars ((cursor-btree cursor))
    (let ((rows (btree-exec-prepared (cursor-btree cursor) query params 'cl-postgres:list-row-reader)))
      (macrolet ((update-cache (this other)
		   `(progn
		     (setf ,this rows)
		     (let ((nrows-other (length ,other)))
		       (when (> nrows-other *cursor-window-size*)
		       (setf ,other (nbutlast ,other (- nrows-other *cursor-window-size*))))))))
	(if cache-prior
	    (update-cache (cached-prior-rows-of cursor) (cached-rows-of cursor))
	    (update-cache (cached-rows-of cursor) (cached-prior-rows-of cursor)))))))

(defmacro cursor-fetch-auto (cursor query-name (&rest query-description) (&rest fetch-params))
  `(with-slots (btree) ,cursor
    (when (initialized-p btree)
      (with-vars (btree)
	(unless (lookup-query btree ,query-name)
	  (register-query btree ,query-name (cursor-fetch-query ,cursor ,@query-description)))
	(cursor-fetch ,cursor ,query-name ,@fetch-params)))))

(defmethod cursor-close ((cursor pm-cursor))
  (setf	(current-key-of cursor) nil
	(current-value-of cursor) nil
	(current-row-of cursor) nil
	(cached-rows-of cursor) nil
	(cached-prior-rows-of cursor) nil)
  nil)

+(defmethod cursor-update-current ((cursor pm-cursor))
  (let ((row (current-row-of cursor))
	(btree (cursor-btree cursor)))
    (assert row)
    (with-vars (btree)
      (setf (current-key-of cursor) (postgres-value-to-lisp (first row) (key-type-of btree))
	    (current-value-of cursor) (postgres-value-to-lisp (second row) (value-type-of btree))))))


(defmethod cursor-fetch-next-from-cache ((cursor pm-cursor))
  (with-slots (rows prior-rows current-row) cursor
    (if rows
	(progn
	  (when current-row (push current-row prior-rows))
	  (setf current-row (pop rows))
	  (cursor-update-current cursor)
	  (cursor-current cursor))
	(cursor-close cursor))))

(defmethod cursor-fetch-prev-from-cache ((cursor pm-cursor))
  (with-slots (rows prior-rows current-row) cursor
    (if prior-rows
        (progn
	  (when current-row (push current-row rows))
	  (setf current-row (pop prior-rows))
          (cursor-update-current cursor)
	  (cursor-current cursor))
	(cursor-close cursor))))

;;; primary cursor functions

(defmethod cursor-current ((cursor pm-cursor))
  (when (cursor-initialized-p cursor)
    (values t (current-key-of cursor) (current-value-of cursor))))

(defmethod cursor-first ((cursor pm-cursor))
  (cursor-fetch-auto cursor 'first (:key-compare nil) (() :reset-cache t))
  (cursor-fetch-next-from-cache cursor))

(defmethod cursor-last ((cursor pm-cursor))
  (cursor-fetch-auto cursor 'last (:key-compare nil :key-order "DESC") (() :reset-cache t :cache-prior t))
  (cursor-fetch-prev-from-cache cursor))

(defmethod cursor-next ((cursor pm-cursor))
  (unless (cursor-initialized-p cursor) (return-from cursor-next (cursor-first cursor)))
  (unless (cached-rows-of cursor)
      (cursor-fetch-auto cursor 'next () ((cursor-current-query-params-auto cursor))))
  (cursor-fetch-next-from-cache cursor))

(defmethod cursor-prev ((cursor pm-cursor))
  (unless (cursor-initialized-p cursor) (return-from cursor-prev (cursor-last cursor)))
  (unless (cached-prior-rows-of cursor)
    (cursor-fetch-auto cursor 'prev (:key-compare '< :key-order "DESC")
		       ((cursor-current-query-params-auto cursor) :cache-prior t)))
  (cursor-fetch-prev-from-cache cursor))

(defmethod cursor-set-range ((cursor pm-cursor) key)
  (cursor-fetch-auto cursor 'set
		     (:key-compare '>= :value-compare nil) 
		     ((list (key-parameter key btree)) :reset-cache t))
  (cursor-fetch-next-from-cache cursor))

(defmethod cursor-set ((cursor pm-cursor) key)
  (multiple-value-bind (found ckey cval)
      (cursor-set-range cursor key)
    (if (and found (ele::lisp-compare-equal key ckey))
	(values t ckey cval)
	(cursor-close cursor))))

(defmethod cursor-get-both ((cursor pm-cursor) key value)
  (multiple-value-bind (found ckey cval)
      (cursor-set-range cursor key)
    (if (and found (ele::lisp-compare-equal key ckey)
	     (ele::lisp-compare-equal value cval))
	(values t ckey cval)
	(cursor-close cursor))))

(defmethod cursor-get-both-range ((cursor pm-cursor) key value)
  (multiple-value-bind (found ckey cval)
      (cursor-set-range cursor key)
    (if (and found (ele::lisp-compare-equal key ckey)
	     (ele::lisp-compare<= value cval))
	(values t ckey cval)
	(cursor-close cursor))))

(defmethod cursor-delete ((cursor pm-cursor))
  (if (cursor-initialized-p cursor)
      (let ((key (current-key-of cursor)))
	(cursor-close cursor)
	(remove-kv key (cursor-btree cursor))
	(values))
      (error "Can't delete with uninitialized cursor")))

(defmethod cursor-put ((cursor pm-cursor) value &key (key nil key-supplied))
  (when key-supplied (cursor-set cursor key))
  (if (cursor-initialized-p cursor)
      (let ((key (current-key-of cursor)))
	(cursor-close cursor)
	(setf (get-value key (cursor-btree cursor)) value)
	value)
      (error "Can't put with uninitialized cursor")))

