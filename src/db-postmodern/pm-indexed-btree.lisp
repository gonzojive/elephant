(in-package :db-postmodern)

(defclass pm-indexed-btree-wrapper (indexed-btree pm-btree-wrapper)
  ((indices :accessor indices :initform (make-hash-table)))
  (:metaclass persistent-metaclass)
  (:documentation "Postmodern implementation of a SQL-based BTree that supports secondary indices."))

(defclass pm-special-indexed-btree-wrapper (pm-indexed-btree-wrapper 
					    pm-special-btree-wrapper)
  ()
  (:metaclass persistent-metaclass))

(defmethod shared-initialize :after ((instance pm-indexed-btree-wrapper) slot-names
				     &rest rest)
  (declare (ignore slot-names rest))
  ;; questionable workaround to create system tables from oid.
  (unless (slot-boundp instance 'indices)
    (setf (indices instance) (make-hash-table))))

(defmethod build-indexed-btree ((sc postmodern-store-controller))
  (make-instance 'pm-indexed-btree-wrapper :sc sc))

(defmethod map-indices (fn (bt pm-indexed-btree-wrapper))
  (maphash fn (indices bt)))

(defmethod get-index ((bt pm-indexed-btree-wrapper) index-name)
  (gethash index-name (indices bt)))

(defmethod remove-index ((bt pm-indexed-btree-wrapper) index-name)
  (let ((indices (indices bt)))
    (remhash index-name indices)
    (setf (indices bt) indices)))

(defmethod add-index ((bt pm-indexed-btree-wrapper) &key index-name key-form (populate t))
  (with-vars (bt)
    (let ((sc (active-controller)))
      (if (and (not (null index-name))
               (symbolp index-name) (or (symbolp key-form) (listp key-form)))
          (let ((index
                 (ensure-transaction (:store-controller sc)
                   (let ((ht (indices bt))
                         (index (build-btree-index sc 
                                                   :primary bt 
                                                   :key-form key-form)))
                     
                     (setf (gethash index-name ht) index)
                     (setf (indices bt) ht)
                     index))))
            (when populate (populate bt index))
            index)        
          (error "Invalid index initargs!")))))

(defmethod populate ((bt pm-indexed-btree-wrapper) index)
  (with-transaction (:store-controller (active-controller))
    (let ((key-fn (key-fn index)))
      (map-btree
       #'(lambda (k v)
           (maybe-insert/update-secondary-index index key-fn k v))
       bt))))

(defmethod (setf get-value) (value key (bt pm-indexed-btree-wrapper))
  "Set a key / value pair, and update secondary indices."
  (call-next-method)
  (with-trans-and-vars (bt)
    (unless (slot-boundp bt 'indices)
      (setf (indices bt) (make-hash-table)))
    (loop for index being the hash-value of (indices bt) do
          (maybe-insert/update-secondary-index index (key-fn index) key value)))
  value)

(defun maybe-insert/update-secondary-index (index key-fn primary-key value)
  (multiple-value-bind (index? secondary-key)
      (funcall key-fn index primary-key value)
    (when index?
      (add-secondary-vs-primary index secondary-key primary-key))))
;; TODO: Maybe one could refer directly to the VALUE of the original table instead of the key,
;; it would save a roundtrip

(defmethod remove-kv (key (bt pm-indexed-btree-wrapper))
  "Remove a key / value pair, and update secondary indices."
  (with-trans-and-vars (bt)
    (multiple-value-bind (value found) (get-value key bt)
      (when found
        (let ((indices (indices bt)))
          (loop for index being the hash-value of indices do
                (multiple-value-bind (index? secondary-key)
                    (funcall (key-fn index) index key value)
                  (when index?
                    (remove-kv-pair secondary-key key index)))))
        ;; Now we remove the actual value
        (call-next-method))
      value)))
