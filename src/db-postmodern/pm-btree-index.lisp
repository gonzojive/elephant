(in-package :db-postmodern)

(defclass pm-btree-index (btree-index pm-dup-btree)
  ()
  (:metaclass persistent-metaclass)
  (:documentation "Postmodern secondary index to an indexed-btree"))

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

(defmethod add-secondary-vs-primary ((bt pm-btree-index) secondary-key primary-key)
  (setf (internal-get-value secondary-key bt) primary-key)) 


