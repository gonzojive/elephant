(in-package :db-postmodern)

(defclass pm-secondary-cursor (pm-dupb-cursor) 
  ()
  (:documentation "Cursor for traversing postmodern secondary indices."))

(defmethod make-cursor ((bt pm-btree-index-wrapper))
  "Make a secondary-cursor from a secondary index."
  (make-instance 'pm-secondary-cursor
		 :btree (get-connection-btree bt)
		 :oid (oid bt)))

(defvar *cursor-current-internal* nil "disables value dereferencing in cursor-current, useful to implement cursor-p* methods")

(defmethod primary ((cursor pm-secondary-cursor))
  (primary (wrapper-of (cursor-btree cursor))))

(defmethod cursor-current ((cursor pm-secondary-cursor))
  (if *cursor-current-internal*
      (call-next-method)
      (multiple-value-bind (has key value)
          (call-next-method)
        (when has
          (values t key (internal-get-value value (primary cursor)))))))

(defmacro def-cursor-p-synonym (name base-name &rest params)
  `(defmethod ,name ((cursor pm-secondary-cursor) ,@params)
    (let ((*cursor-current-internal* t))
      (multiple-value-bind (found key val)
	  (,base-name cursor ,@params)
	(when found
	  (values t key (internal-get-value val (primary cursor)) val))))))

(def-cursor-p-synonym cursor-pfirst cursor-first)
(def-cursor-p-synonym cursor-plast cursor-last)
(def-cursor-p-synonym cursor-pnext cursor-next)
(def-cursor-p-synonym cursor-pprev cursor-prev)
(def-cursor-p-synonym cursor-pcurrent cursor-current)

(def-cursor-p-synonym cursor-pnext-dup cursor-next-dup)
(def-cursor-p-synonym cursor-pprev-dup cursor-prev-dup)

(def-cursor-p-synonym cursor-pnext-nodup cursor-next-nodup)
(def-cursor-p-synonym cursor-pprev-nodup cursor-prev-nodup)

(def-cursor-p-synonym cursor-pset cursor-set key)
(def-cursor-p-synonym cursor-pset-range cursor-set-range key)

(def-cursor-p-synonym cursor-pget-both cursor-get-both key pkey)
(def-cursor-p-synonym cursor-pget-both-range cursor-get-both-range key pkey)

(defmethod cursor-delete ((cursor pm-secondary-cursor))
  "Delete by cursor: deletes ALL secondary indices."
  (if (cursor-initialized-p cursor)
      (let ((pkey (current-value-of cursor)))
        (cursor-close cursor)
        (remove-kv pkey (primary cursor)))
      (error "Can't delete with uninitialized cursor")))

(defmethod cursor-put ((cursor pm-secondary-cursor) value &rest rest)
  "Puts are forbidden on secondary indices.  Try adding to
the primary."
  (declare (ignore rest value))
  (error "Puts are forbidden on secondary indices.  Try adding to the primary."))
