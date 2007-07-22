(in-package :db-postmodern)

(defvar *default-fetch-size* 500)

(defclass pm-cursor (cursor)
  ((name :accessor db-cursor-name-of)
   (search-key :accessor search-key-of :initform nil)
   (db-oid :accessor current-row-identifier :initform nil
           :documentation "This oid is the postgresql oid, not the elephant oid. Unfortunately they share name")
   (key :accessor current-key-field :initform nil)
   (rows :accessor cached-rows-of :initform nil)
   (prior-rows :accessor cached-prior-rows-of :initform nil)
   (val :accessor current-value-field :initform nil))
  (:documentation "A SQL cursor for traversing (primary) BTrees."))

(defmethod make-cursor ((bt pm-btree))
  (make-instance 'pm-cursor 
		 :btree bt
		 :oid (oid bt)))

(defmethod cursor-duplicate ((cursor pm-cursor))
  (make-instance (type-of cursor)
		 :initialized-p (cursor-initialized-p cursor)
		 :oid (cursor-oid cursor)))

(defmethod cursor-close ((cursor pm-cursor))
  (when (cursor-initialized-p cursor)
    (with-trans-and-vars ((cursor-btree cursor)) ;;Or maybe just vars?
      (cl-postgres:exec-query (active-connection)
                              (format nil "close ~a;" (db-cursor-name-of cursor)))))
  (clean-cursor-state cursor))

(defun clean-cursor-state (cursor)
  (setf (cursor-initialized-p cursor) nil
        (current-key-field cursor) nil
        (current-value-field cursor) nil
        (cached-rows-of cursor) nil
        (cached-prior-rows-of cursor) nil)
  nil)

(defmethod cursor-current ((cursor pm-cursor))
  (internal-cursor-current cursor))

(defun internal-cursor-current (cursor)
  (let (found key val)
    (when (cursor-initialized-p cursor)
      (assert (current-key-field cursor)) ;; Otherwise the query should be uninitialized
      (with-trans-and-vars ((cursor-btree cursor)) ;;Or maybe just vars
        (setf key (postgres-value-to-lisp (current-key-field cursor) (key-type-of (cursor-btree cursor)))
              val (postgres-value-to-lisp (current-value-field cursor) (value-type-of (cursor-btree cursor)))
              found t)))
    (values found key val)))

(defmethod cursor-init ((cursor pm-cursor)
                        &key (where-clause "")
                        (search-key nil key-provided-p)
                        (search-value nil value-provided-p))
  (unless (cursor-initialized-p cursor)
    (with-accessors ((bt cursor-btree))
      cursor
      (when (initialized-p bt)
        (handler-bind
            ((bad-db-parameter #'(lambda (c)
                                   (declare (ignore c))
                                   (return-from cursor-init nil))))
          (with-trans-and-vars (bt) ;;Or maybe just vars?
            (setf (search-key-of cursor) search-key)
            (let ((tempname (gensym "TMPCUR")))
              (setf (db-cursor-name-of cursor) (format nil "cur_~a_~a" (table-of bt) tempname))
              (if key-provided-p
                  (let ((parameters (list (key-parameter search-key (cursor-btree cursor)))))
                    (when value-provided-p
                      (setf parameters (append parameters (list (value-parameter search-value (cursor-btree cursor))))))
                    (register-query bt
                                    tempname
                                    (build-cursor-query-helper cursor :where-clause where-clause))
                    (btree-exec-prepared bt tempname parameters 'cl-postgres:ignore-row-reader))
                  (cl-postgres:exec-query (active-connection) (build-cursor-query-helper cursor))))
            (clean-cursor-state cursor))
          (setf (cursor-initialized-p cursor) t))))))

(defmethod build-cursor-query-helper ((cursor pm-cursor) &key (where-clause ""))
  (if (and +join-with-blob-optimization+ (eq :object (value-type-of (cursor-btree cursor))))
      (format nil "declare ~a scroll cursor with hold for select qi,bob,~a.oid from ~a ,blob ~a ~a bid=value order by qi,value" 
              (db-cursor-name-of cursor)
              (table-of (cursor-btree cursor))
              (table-of (cursor-btree cursor))
              (if (string= "" where-clause)
                  "where"
                  where-clause)
              (if (string= "" where-clause)
                  ""
                  " and "))
      (format nil "declare ~a scroll cursor with hold for select qi,value,oid from ~a ~a order by qi,value" 
              (db-cursor-name-of cursor)
              (table-of (cursor-btree cursor))
              where-clause)))

(defmethod set-has-been-called-p ((cursor pm-cursor))
  "cursor-set moves the cursor so the first position is wrong"
  (when (search-key-of cursor) t))

(defmacro with-initialized-cursor ((cursor &rest args) &body body)
  `(progn
     (cursor-init cursor ,@args)
     (when (cursor-initialized-p ,cursor)
       ,@body)))

(defmethod fetch ((cursor pm-cursor) fetch-direction)
  (when (cursor-initialized-p cursor)
    (with-trans-and-vars ((cursor-btree cursor)) ;;Or maybe just vars?
      (let* ((fetch-stmt (concatenate 'string
                                      "FETCH "
                                      (if (eq fetch-direction 'next)
                                          (format nil "FORWARD ~a" *default-fetch-size*)
                                          (symbol-name fetch-direction))
                                      " FROM "
                                      (db-cursor-name-of cursor)))
             (rows (cl-postgres:exec-query (active-connection)
                                           fetch-stmt
                                           'cl-postgres:list-row-reader)))
        (if (eq fetch-direction 'next)
            (when (> (length (cached-prior-rows-of cursor))
                     *default-fetch-size*)
              (setf (cached-prior-rows-of cursor) (subseq (cached-prior-rows-of cursor) 0 *default-fetch-size*)))
            (setf (cached-prior-rows-of cursor) nil))
        (setf (cached-rows-of cursor) rows)))
    (fetch-next-from-cache cursor)))

(defun fetch-next-from-cache (cursor)
  (with-accessors ((rows cached-rows-of)
                   (prior cached-prior-rows-of))
    cursor
    (if rows
        (progn
          (update-current-from-first-row cursor rows)
          (push (first rows) prior)
          (pop rows))
        (cursor-close cursor)))
  (cursor-current cursor))

(defun update-current-from-first-row (cursor rows)
  (destructuring-bind (key-field value-field db-oid)
      (first rows)
    (setf (current-row-identifier cursor) db-oid
          (current-key-field cursor) key-field
          (current-value-field cursor) value-field)))

(defun fetch-prior (cursor)
  (with-accessors ((rows cached-rows-of)
                   (prior cached-prior-rows-of))
      cursor
    (flet ((from-cache ()
             (update-current-from-first-row cursor prior)
             (cursor-current cursor))
           (pop-prior ()
             (when prior
               (push (first prior) rows)
               (pop prior))))
      (pop-prior)
      (if prior
          (from-cache)
          (progn
            (scan-to-current-row cursor)
            (if prior
                (progn
                  (pop-prior)
                  (if prior
                      (from-cache)
                      (clean-cursor-state cursor)))
                (clean-cursor-state cursor)))))))

(defmethod cursor-first ((cursor pm-cursor))
  (when (set-has-been-called-p cursor)
    (cursor-close cursor))
  (with-initialized-cursor (cursor)
    (fetch cursor 'first)))
		 
(defmethod cursor-last ((cursor pm-cursor))
  (when (set-has-been-called-p cursor)
    (cursor-close cursor))
  (with-initialized-cursor (cursor) 
    (fetch cursor 'last)))

(defmethod cursor-next ((cursor pm-cursor))
  (if (cursor-initialized-p cursor)
      (if (cached-rows-of cursor)
          (fetch-next-from-cache cursor)
          (fetch cursor 'next))
      (cursor-first cursor))) 

(defmethod cursor-prev ((cursor pm-cursor))
  (if (not (cursor-initialized-p cursor))
      nil ;;        (cursor-last cursor)
      (progn 
        (when (set-has-been-called-p cursor)
          (warn "Postmodern users beware, don't mix cursor-prev and cursor-set. Inefficient!")
          (scan-to-current-row cursor))
        ;; We are finally at the position previously found by cursor-set.
        ;; Lets drop down to fetch prior, which will return the correct value. 
        ;; Normal case is this
        (fetch-prior cursor))))

(defun scan-to-current-row (cursor)
  (let ((oid-now (current-row-identifier cursor)))
    (cursor-close cursor)
    (cursor-init cursor)
    (loop for x = (cursor-next cursor)
       do (unless x ;; Should not really happen I guess?
              (return-from scan-to-current-row))
       until (equalp oid-now (current-row-identifier cursor)))))

(defmethod move-absolute ((cursor pm-cursor) absolute-nr)
  (setf (current-key-field cursor) nil
        (current-value-field cursor) nil
        (cached-rows-of cursor) nil
        (cached-prior-rows-of cursor) nil)  
  (let ((fetch-stmt (concatenate 'string
                                 "MOVE ABSOLUTE "
                                 (princ-to-string absolute-nr)
                                 " FROM "
                                 (db-cursor-name-of cursor))))
    (cl-postgres:exec-query (active-connection)
                            fetch-stmt
                            'cl-postgres:ignore-row-reader)))

	  
(defmethod cursor-set ((cursor pm-cursor) key)
  (cursor-set-helper cursor key 'cursor-set)

;;  (progn
;;    (when (cursor-initialized-p cursor)
;;      (cursor-close cursor))
;;    (with-initialized-cursor
;;        (cursor :where-clause "where qi>=$1" 
;;                :search-key key)
;;      ;; need greater than, otherwise next won't work like for berkeley-db.
;;      ;; However, we also need to check that the key of the returned value
;;      ;; is == key and only return the values in that case
;;      (multiple-value-bind
;;            (exists? skey val)
;;          (cursor-next cursor)
;;        (when (elephant::lisp-compare-equal key skey)
;;          (values exists? skey val)))))
  )

(defun cursor-pget-both-helper (cursor key primary-key query-function-name)
  (unless (cursor-initialized-p cursor)
    (cursor-init cursor))
  (let* ((bt (cursor-btree cursor))
         (qid (intern (format nil "CURSOR-SET-BOTH-HELP-~a" (table-of bt)))))
    (register-query bt qid
                    (format nil "select count(*) from (select qi,value from ~a order by qi,value) as c where c.qi<=$1 and c.value<$2"
                            (table-of bt)))
    (with-trans-and-vars (bt)
      (let ((rows-before (btree-exec-prepared bt qid
                                              (list (key-parameter key bt)
                                                    (value-parameter primary-key bt))
                                              'first-value-row-reader)))
        (when rows-before
;;          (format t "rows-before ~a" rows-before)
          (move-absolute cursor rows-before))))
    (multiple-value-bind
          (exists? skey val pkey)
        (cursor-pnext cursor)
      ;;            (print "HELLO")
      ;;            (format t "~a key ~a exits ~a skey ~a val ~a optional-pkey~a" query-function-name key exists? skey val pkey)
      (ecase query-function-name
        (cursor-pget-both (when (elephant::lisp-compare-equal pkey primary-key)
                            ;;                            (print "equal")
                            (values exists? skey val pkey)))
        (cursor-pget-both-range (values exists? skey val pkey))))))

(defun cursor-set-helper (cursor key query-function-name)
  (unless (cursor-initialized-p cursor)
    (cursor-init cursor))
  (let* ((bt (cursor-btree cursor))
         (qid (intern (format nil "CURSOR-SET-HELP-~a" (table-of bt)))))
    (register-query bt qid
                    (format nil "select count(*) from (select qi from ~a order by qi,value) as c where c.qi<$1"
                            (table-of bt)))
    (with-trans-and-vars (bt)
      (let ((rows-before (btree-exec-prepared bt qid
                                              (list (key-parameter key bt))
                                              'first-value-row-reader)))
        (when rows-before
;;          (format t "rows-before ~a" rows-before)
          (move-absolute cursor rows-before))))
    (multiple-value-bind
          (exists? skey val pkey)
        (if (member query-function-name '(cursor-set cursor-set-range))
            (cursor-next cursor)
            (cursor-pnext cursor))
;;      (print "HELLO")
;;      (format t "~a key ~a exits ~a skey ~a val ~a optional-pkey~a" query-function-name key exists? skey val pkey)
      (ecase query-function-name
        (cursor-set (when (elephant::lisp-compare-equal key skey)
;;                      (print "EQUAL")
                      (values exists? skey val)))
        (cursor-set-range (values exists? skey val))
        (cursor-pset (when (elephant::lisp-compare-equal key skey)
;;                       (print "EQUAL")
                       (values exists? skey val pkey)))
        (cursor-pset-range (values exists? skey val pkey))))))


(defmethod cursor-set-range ((cursor pm-cursor) key)
  (cursor-set-helper cursor key 'cursor-set-range))

;;  (when (cursor-initialized-p cursor)
;;    (cursor-close cursor))
;;  (with-initialized-cursor
;;      (cursor :where-clause "where qi>=$1"
;;              :search-key key)
;;    (cursor-next cursor))

(defmethod cursor-get-both ((cursor pm-cursor) key value)
  (if (equal (get-value key (cursor-btree cursor))
             value)
      (cursor-set cursor key)))

(defmethod cursor-get-both-range ((cursor pm-cursor) key value)
  (cursor-get-both cursor key value))

(defmethod cursor-delete ((cursor pm-cursor))
  (if (cursor-initialized-p cursor)
      (let ((key (postgres-value-to-lisp (current-key-field cursor) (key-type-of (cursor-btree cursor)))))
        (cursor-close cursor)
        (remove-kv key (cursor-btree cursor)))
      nil))

(defmethod cursor-put ((cursor pm-cursor) value &key (key nil key-specified-p))
  "Put by cursor.  Not particularly useful since primaries
don't support duplicates.  Currently doesn't properly move
the cursor."
  (declare (ignore key value key-specified-p))
  (error "Puts on pm-cursors are not implemented"))
