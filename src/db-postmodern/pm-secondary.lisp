(in-package :db-postmodern)

(defclass pm-secondary-cursor (pm-cursor) 
  ()
  (:documentation "Cursor for traversing postmodern secondary indices."))

(defmethod make-cursor ((bt pm-btree-index))
  "Make a secondary-cursor from a secondary index."
  (make-instance 'pm-secondary-cursor 
		 :btree bt
		 :oid (oid bt)))

(defvar *in-secondary-cursor-mover* nil)

(defun key-field= (a b)
  (equalp a b)) ;; TODO is this correct enough

(defun secondary-cursor-mover (cursor movement-function &key dup nodup return-pk)
  (let ((value-column-before (current-key-field cursor))
        (row-id-before (current-row-identifier cursor))
        (*in-secondary-cursor-mover* t))
    (declare (special *in-secondary-cursor-mover*))
    (block main-block
      (loop
       (multiple-value-bind (found key-column value-column)
           (funcall movement-function cursor)
         ;;         (format t "sec-cur-mover ~S " (list found key-column value-column))
         (cond
           ((or (not found)
                (and dup
                     (not (key-field= (current-key-field cursor)
                                      value-column-before))))
            (return-from main-block nil))
           ((and (eq (current-row-identifier cursor) row-id-before)
                 (not (eql movement-function #'cursor-current)))
            (error "Movement function doesn't move"))
           ((and nodup
                 (key-field= (current-key-field cursor)
                             value-column-before))
            (setf row-id-before (current-row-identifier cursor)) ;;TODO row-id stuff can maybe be removed later?
            'continue)
           (t (return-from main-block
                (secondary-cursor-return-values cursor
                                                key-column
                                                value-column
                                                :return-pk return-pk)))))))))

(defun secondary-cursor-return-values (cursor key-column value-column &key return-pk)
  (when key-column
    (if return-pk
        (let ((value (internal-get-value value-column (primary (cursor-btree cursor)))))
          (values t key-column value value-column))
        (let ((value (internal-get-value value-column (primary (cursor-btree cursor)))))
          (values t value-column value)))))

(defmethod cursor-current ((cursor pm-secondary-cursor))
  (if *in-secondary-cursor-mover*
      (call-next-method)
      (multiple-value-bind (has key value)
          (call-next-method)
        (when has
          (values t
                  key
                  (internal-get-value value (primary (cursor-btree cursor))))))))

;;--------------------------------------------------------------------------------
;;                        Secondary cursor specific methods
;;--------------------------------------------------------------------------------
(defmethod cursor-next-dup ((cursor pm-secondary-cursor))
  (secondary-cursor-mover cursor #'cursor-next :dup t))

(defmethod cursor-next-nodup ((cursor pm-secondary-cursor))
  (secondary-cursor-mover cursor #'cursor-next :nodup t))

(defmethod cursor-pnext ((cursor pm-secondary-cursor))
  (secondary-cursor-mover cursor #'cursor-next :return-pk t))

(defmethod cursor-pnext-dup ((cursor pm-secondary-cursor))
  (secondary-cursor-mover cursor #'cursor-next :dup t :return-pk t))

(defmethod cursor-pnext-nodup ((cursor pm-secondary-cursor))
  (secondary-cursor-mover cursor #'cursor-next :nodup t :return-pk t))

(defmethod cursor-pcurrent ((cursor pm-secondary-cursor))
  (secondary-cursor-mover cursor #'cursor-current :return-pk t))

(defmethod cursor-pfirst ((cursor pm-secondary-cursor))
  (secondary-cursor-mover cursor #'cursor-first :return-pk t))

(defmethod cursor-plast ((cursor pm-secondary-cursor))
  (secondary-cursor-mover cursor #'cursor-last :return-pk t))
	  
(defmethod cursor-pprev ((cursor pm-secondary-cursor))
  (secondary-cursor-mover cursor #'cursor-prev :return-pk t))

(defmethod cursor-prev-nodup ((cursor pm-secondary-cursor))
  (secondary-cursor-mover cursor #'cursor-prev :nodup t))

(defmethod cursor-pprev-nodup ((cursor pm-secondary-cursor))
  (secondary-cursor-mover cursor #'cursor-prev :nodup t :return-pk t))

(defmethod cursor-pset ((cursor pm-secondary-cursor) key)
  (when (cursor-initialized-p cursor)
    (cursor-close cursor))
  (with-initialized-cursor
      (cursor :where-clause "where qi>=$1"
              :search-key key)
    (help-cursor-pset-with-equality cursor key)))

(defun help-cursor-pset-with-equality (cursor key)
  ;; The where clause for key is >= (greater than).
  ;; This because sometime you may want to cursor-next to next key,
  ;; and that wont work if we select by =.
  ;; However, we also need to check that the key of the returned value
  ;; is == key and only return the values in that case.
  ;; This is because we ask for a key == using a where >=
  (multiple-value-bind
        (exists? skey val pkey)
      (cursor-pnext cursor)
    (when (elephant::lisp-compare-equal key skey)
      (values exists? skey val pkey))))

(defmethod cursor-pset-range ((cursor pm-secondary-cursor) key)
  (when (cursor-initialized-p cursor)
    (cursor-close cursor))
  (with-initialized-cursor
        (cursor :where-clause "where qi>=$1"
                 :search-key key)
      (cursor-pnext cursor)))

(defmethod cursor-pget-both ((cursor pm-secondary-cursor) key pkey)
  (when (cursor-initialized-p cursor)
    (cursor-close cursor))
  (with-initialized-cursor
        (cursor :where-clause "where qi>=$1 and value=$2"
                 :search-key key
                 :search-value pkey)
    (help-cursor-pset-with-equality cursor key)))

(defmethod cursor-pget-both-range ((cursor pm-secondary-cursor) key pkey)
  (when (cursor-initialized-p cursor)
    (cursor-close cursor))
  (with-initialized-cursor
      (cursor :where-clause "where qi>=$1 and value>=$2"
              :search-key key
              :search-value pkey)
    (help-cursor-pset-with-equality cursor key)))

(defmethod cursor-delete ((cursor pm-secondary-cursor))
  "Delete by cursor: deletes ALL secondary indices."
  (remove-kv (postgres-value-to-lisp (current-value-field cursor) (value-type-of (cursor-btree cursor)))
             (primary  (cursor-btree cursor))))

(defmethod cursor-get-both ((cursor pm-secondary-cursor) key value)
  "cursor-get-both not implemented for secondary indices.
Use cursor-pget-both."
  (declare (ignore key value))
  (error "cursor-get-both not implemented on secondary
indices.  Use cursor-pget-both."))

(defmethod cursor-get-both-range ((cursor pm-secondary-cursor) key value)
  "cursor-get-both-range not implemented for secondary indices.
Use cursor-pget-both-range."
  (declare (ignore key value))
  (error "cursor-get-both-range not implemented on secondary indices.  Use cursor-pget-both-range."))

(defmethod cursor-put ((cursor pm-secondary-cursor) value &rest rest)
  "Puts are forbidden on secondary indices.  Try adding to
the primary."
  (declare (ignore rest value))
  (error "Puts are forbidden on secondary indices.  Try adding to the primary."))
