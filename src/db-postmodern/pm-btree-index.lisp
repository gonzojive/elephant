(in-package :db-postmodern)

(defclass pm-btree-index (btree-index pm-btree)
  ()
  (:metaclass persistent-metaclass)
  (:documentation "Postmodern secondary index to an indexed-btree"))

(defmethod duplicates-allowed-p ((bt pm-btree-index))
  t)

(defmethod create-table-from-first-values :before ((bt pm-btree-index) key value)
  (declare (ignorable key))
  (setf (value-type-of bt) (data-type value)))

(defmethod build-btree-index ((sc postmodern-store-controller) &key primary key-form)
  (make-instance 'pm-btree-index :primary primary :key-form key-form :sc sc))

(defmethod get-value (key (bt pm-btree-index))
  "Get the value in the primary DB from a secondary key."
  (let ((it (get-primary-key key bt)))
    (when it
      (get-value it (primary bt)))))

(defmethod get-primary-key (secondary-key (bt pm-btree-index))
  (internal-get-value secondary-key bt))

(defmethod prepare-local-queries :after ((bt pm-btree-index))
  (register-query bt 'delete-both (format nil "delete from ~a where qi=$1 and value=$2" (table-of bt))))

(defmethod add-secondary-vs-primary ((bt pm-btree-index) secondary-key primary-key)
  (setf (internal-get-value secondary-key bt) primary-key)) 

(defmethod remove-key-and-value-pair (secondary-key value (bt pm-btree-index))
  (when (and secondary-key (initialized-p bt)) ;;TODO: check for secondary-key was because it was sometimes nil, is this the correct fix?
    (with-trans-and-vars (bt)
      (btree-exec-prepared bt 'delete-both
                           (list (postgres-format secondary-key (key-type-of bt))
                                 (postgres-format value (value-type-of bt)))
                           'cl-postgres:ignore-row-reader))))

(defmethod internal-get-values (key (bt pm-btree))
  (let (value exists-p)
    (when (initialized-p bt)
      (with-vars (bt)
        (let ((results (btree-exec-prepared bt 'select
                                           (list (key-parameter key bt))
					   #'cl-postgres:list-row-reader)))
          (when results
	    (setf value (mapcar #'car results)
		  exists-p t)))))
    (values value exists-p)))

(defmethod map-index (fn (index pm-btree-index) &rest args 
		      &key start end (value nil value-set-p) from-end collect 
		      &allow-other-keys)
  (if value-set-p
      (progn 
	(if collect
	    (loop with pbt = (primary index)
		  for pkey in (internal-get-values value index)
		  for (val exists) = (multiple-value-list (get-value pkey pbt))		  
		  when exists
		  collect (funcall fn value val pkey))
	    (loop with pbt = (primary index)
		  for pkey in (internal-get-values value index)
		for (val exists) = (multiple-value-list (get-value pkey pbt))
		when exists do (funcall fn value val pkey))))
      (call-next-method)))

#|
;; caching for map-index calls for testing purposes
(defparameter *mi-cache* (make-hash-table :test 'equal))

(defmethod map-index (fn (index pm-btree-index) &rest args 
		      &key start end (value nil value-set-p) from-end collect 
		      &allow-other-keys)
  (let* ((ck (list index value start end from-end))
	 (results (gethash ck *mi-cache*)))
    (if results
	(progn
	  (loop for (a b c) in (car results)
		do (funcall fn a b c))
	  (copy-tree (cdr results)))
	(let (abcs)
	  (flet ((pusher (a b c)
		   (push (list a b c) abcs)
		   (funcall fn a b c)))
	    (let ((collected
		   (if value-set-p
		       (call-next-method #'pusher index :start start
					 :end end :value value :from-end from-end :collect collect)
		       (call-next-method #'pusher index :start start
					 :end end :from-end from-end :collect collect))))
	      (setf (gethash ck *mi-cache*) (cons abcs collected))
	      (copy-tree collected)))))))
		   
|#