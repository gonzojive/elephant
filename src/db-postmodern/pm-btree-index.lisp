(in-package :db-postmodern)


(defclass pm-btree-index-wrapper (btree-index pm-dup-btree-wrapper)
  ()
  (:metaclass persistent-metaclass)
  (:documentation "Postmodern secondary index to an indexed-btree"))

(defmethod build-btree-index ((sc postmodern-store-controller) &key primary key-form)
  (make-instance 'pm-btree-index-wrapper :primary primary :key-form key-form :sc sc))

(defmethod get-value (key (bt pm-btree-index-wrapper))
  "Get the value in the primary DB from a secondary key."
  (let ((it (get-primary-key key bt)))
    (when it
      (get-value it (primary bt)))))

(defmethod get-primary-key (secondary-key (bt pm-btree-index-wrapper))
  (get-value secondary-key (get-connection-btree bt)))

(defmethod add-secondary-vs-primary ((bt pm-btree-index-wrapper) secondary-key primary-key)
  (setf (get-value secondary-key (get-connection-btree bt)) primary-key)) 


