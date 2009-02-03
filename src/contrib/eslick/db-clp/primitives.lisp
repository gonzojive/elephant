(in-package :db-clp)

(declaim (optimize (speed 1) safety debug))

(defclass btree-proxy (containers:binary-search-tree)
  ())


(defun make-btree-proxy (&key duplicate-keys)
  (cond (duplicate-keys 
	 (containers::make-container 'btree-proxy
				     :key 'identity
				     :test 'dup-proxy-equal
				     :sorter 'dup-proxy-compare<))
	(t 
	 (containers::make-container 'btree-proxy
				     :key 'identity
				     :test 'proxy-equal
				     :sorter 'proxy-compare<))))

;; Sorting

(defun proxy-equal (a b)
  (lisp-compare-equal (car a) (car b)))

(defun proxy-compare< (a b)
  (lisp-compare< (car a) (car b)))

(defun dup-proxy-equal (a b)
  (and (lisp-compare-equal (car a) (car b))
;;       (or (null (cdr a)) (null (cdr b))
       (lisp-compare-equal (cdr a) (cdr b))))

(defun dup-proxy-compare< (a b)
  (or (lisp-compare< (car a) (car b))
      (and (lisp-compare-equal (car a) (car b))
	   (or (null (cdr a))
	       (lisp-compare< (cdr a) (cdr b))))))

;; BTREE API

(defun get-item (proxy key)
  (awhen (containers:find-item proxy (cons key nil))
    (cdr (containers:element it))))

(defun insert-item (proxy key value)
  (containers:insert-item proxy (cons key value))
  value)

(defun unique-insert-item (proxy key value)
  (aif (containers:find-item proxy (cons key nil))
       (setf (cdr (containers:element it)) value)
       (insert-item proxy key value))
  value)

(defun exists (proxy key)
  (when (containers:item-at proxy (cons key nil))
    t))

(defmethod remove-kv (key (proxy btree-proxy))
  (containers:delete-item proxy (cons key nil)))

(defmethod remove-kv-pair (key value (proxy btree-proxy))
  (containers:delete-item proxy (cons key value)))


;;==================================================

(defclass slot-proxy (containers:binary-search-tree)
  ())

(defun make-slot-proxy ()
  (containers::make-container 'slot-proxy
			      :key 'identity
			      :test 'slot-proxy-equal
			      :sorter 'slot-proxy-compare<))

(defun slot-proxy-equal (a b)
  "Compare number & symbol; account for nils"
  (and (not (or (null a) (null b)))
       (and (eq (first a) (first b))
	    (eq (second a) (second b)))))

(defun slot-proxy-compare< (a b)
  "Compare (number symbol); account for nils"
  (or (null a) (null b)
      (< (first a) (first b))
      (and (= (first a) (first b))
	   (string< (symbol-name (second a)) 
		    (symbol-name (second b))))))
  

;; Slot API

(defun proxy-slot-value (proxy oid slotname)
  (awhen (containers:find-item proxy (list oid slotname))
    (values (cddr (containers:element it)) t)))


(defun proxy-set-slot-value (proxy oid slotname value)
  (let ((ref (cons oid (cons slotname value))))
    (aif (containers:find-item proxy ref)
	 (setf (cddr (containers:element it)) value)
	 (containers:insert-item proxy ref))
    value))

(defun proxy-slot-boundp (proxy oid slotname)
  (when (containers:item-at proxy (list oid slotname))
    t))

(defmethod proxy-slot-makunbound (proxy oid slotname)
  (awhen (containers:delete-item proxy (list oid slotname))
    (containers:element it)))

;; Map Utilities

(defun proxy-find-node (proxy start from-end test)
  (let ((node (containers:find-successor-node proxy (cons start nil))))
    (when from-end
      (loop for next = (containers:successor proxy node) do
	   (if (funcall test (containers:element node) (containers:element next))
	       (setf node next)
	       (return nil))))
    node))

;; Serializer support for persistent objects

(defmethod s-serialization::serialize-xml-internal ((object persistent) stream serialization-state)
  (declare (ignore serialization-state))
  (write-string "<PERSISTENT ID=\"" stream)
  (prin1 (oid object) stream)
  (write-string "\"/>" stream))

(defmethod s-serialization::deserialize-xml-new-element-aux 
    ((name (eql :persistent)) attributes)
  (elephant::controller-recreate-instance *store-controller* 
					  (parse-integer (s-serialization::get-attribute-value :id attributes))))
