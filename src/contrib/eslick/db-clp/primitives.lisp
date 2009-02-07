(in-package :db-clp)

(declaim (optimize (speed 1) safety debug))

(defvar *load-table* nil
  "Remember all loaded instances for recreation")

(defclass btree-proxy (containers:red-black-tree)
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
  (awhen (containers:find-node proxy (cons key nil))
    (values (cdr (containers:element it)) t)))

(defun get-successor-item (proxy key)
  (awhen (containers:find-successor-node proxy (cons key nil))
    (values (cdr (containers:element it)) t)))

(defun insert-proxy-item (proxy key value)
  (containers:insert-item proxy (cons key value))
  value)

(defun unique-insert-item (proxy key value)
  (aif (containers:find-item proxy (cons key nil))
       (setf (cdr (containers:element it)) value)
       (insert-proxy-item proxy key value))
  value)

(defun exists (proxy key)
  (when (containers:item-at proxy (cons key nil))
    t))

(defmethod remove-kv (key (proxy btree-proxy))
  (containers:delete-item proxy (cons key nil)))

(defmethod remove-kv-pair (key value (proxy btree-proxy))
  (containers:delete-item proxy (cons key value)))


;;==================================================

(defclass slot-proxy (containers:red-black-tree)
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
  (containers:delete-item proxy (list oid slotname)))

;; Map Utilities

(defun proxy-find-node (proxy start from-end test)
  (let* ((elt (cons start nil))
	 (node (containers:find-successor-node proxy elt)))
    (if from-end 
	(if (funcall test (containers:element node) elt)
	    (find-last-duplicate proxy start test node)
            (containers:predecessor proxy node))
	node)))

(defun find-last-duplicate (proxy start test node)
  (loop 
     for new = (containers:successor proxy node)
     while (funcall test (containers:element new) (cons start nil)) 
     do (setf node new))
  node)


;;   (let ((node 
;;     (when from-end
;;       (loop for next = (containers:successor proxy node) do
;; 	   (if (funcall test (containers:element node) (containers:element next))
;; 	       (setf node next)
;; 	       (return nil))))
;;     node))

;; Serializer support for persistent objects

(defmethod s-serialization::serialize-xml-internal ((object persistent-object) stream serialization-state)
  (write-string "<PERSISTENT ID=\"" stream)
  (prin1 (oid object) stream)
  (write-string "\" CLASS=\"" stream)
  (princ (class-name (class-of object)) stream)
  (write-string "\" PKG=\"" stream)
  (princ (package-name (symbol-package (type-of object))) stream)
  (write-string "\"/>" stream))
;  (when (subtypep (type-of object) 'clp-btree)
;    (s-serialization::serialize-xml-internal (slot-value object 'tree)
;					     stream serialization-state))

(defmethod s-serialization::deserialize-xml-new-element-aux 
    ((name (eql :persistent)) attributes)
  (let ((oid (parse-integer (s-serialization::get-attribute-value :id attributes)))
	(classname (intern (s-serialization::get-attribute-value :class attributes)
			   (find-package (s-serialization::get-attribute-value :pkg attributes)))))
    (aif (gethash oid *load-table*) it
	 (setf (gethash oid *load-table*)
	       (ele::initial-persistent-setup
		(allocate-instance (find-class classname))
		:from-oid oid :sc *store-controller*)))))

(defmethod s-serialization::deserialize-xml-finish-element-aux
    ((name (eql :persistent)) attributes parent-seed seed)
  (let ((object (gethash (parse-integer 
			  (s-serialization::get-attribute-value :id attributes))
			 *load-table*)))
    object))
	    


;;  (elephant::controller-recreate-instance *store-controller* 
;     (parse-integer (s-serialization::get-attribute-value :id attributes))
;     (intern (s-serialization::get-attribute-value :class attributes)
;	     (find-package (s-serialization::get-attribute-value :pkg attributes)))))

