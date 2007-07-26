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

;;------------------------------------------------------------------------------

#-char-columns
(defmethod map-index (fn (index pm-btree-index) &rest args 
		      &key start end (value nil value-set-p) from-end collect 
		      &allow-other-keys)
  ;; This is because the default map-index method relies on the sort order of strings,
  ;; which is not implemented in the db-postmodern backend as it is now
  (flet ((my-lisp-compare<= (a b)
           (etypecase a
               (number (<= a b))
               (string (string= a b)) ;; This is the important change
               (persistent (<= (oid a) (oid b)))
               (symbol (string= (symbol-name a) (symbol-name b))))))
    (let ((fn-before (symbol-function 'elephant::lisp-compare<=)))
      (unwind-protect
           (progn
             (setf (symbol-function 'elephant::lisp-compare<=)
                   #'my-lisp-compare<=)
             (common-lisp:call-next-method))
        (setf (symbol-function 'elephant::lisp-compare<=)
              fn-before)))))
