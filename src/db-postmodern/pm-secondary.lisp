(in-package :db-postmodern)

(defclass pm-secondary-cursor (pm-cursor) 
  ()
  (:documentation "Cursor for traversing postmodern secondary indices."))

(defmethod make-cursor ((bt pm-btree-index))
  "Make a secondary-cursor from a secondary index."
  (make-instance 'pm-secondary-cursor 
		 :btree bt
		 :oid (oid bt)))

(defvar *cursor-current-internal* nil "disables value dereferencing in cursor-current, useful to implement cursor-p* methods")

(defmethod cursor-current-query-params-auto ((cursor pm-secondary-cursor))
  (with-slots (btree) cursor 
    (list (ensure-string (current-raw-key-of cursor))
	  (ensure-string (current-raw-value-of cursor)))))

(defmethod cursor-current ((cursor pm-secondary-cursor))
  (if *cursor-current-internal*
      (call-next-method)
      (multiple-value-bind (has key value)
          (call-next-method)
        (when has
          (values t key (internal-get-value value (primary (cursor-btree cursor))))))))

(defmacro def-cursor-p-synonym (name base-name &rest params)
  `(defmethod ,name ((cursor pm-secondary-cursor) ,@params)
    (let ((*cursor-current-internal* t))
      (multiple-value-bind (found key val)
	  (,base-name cursor ,@params)
	(when found
	  (values t key (internal-get-value val (primary (cursor-btree cursor))) val))))))

(def-cursor-p-synonym cursor-pfirst cursor-first)
(def-cursor-p-synonym cursor-plast cursor-last)
(def-cursor-p-synonym cursor-pnext cursor-next)
(def-cursor-p-synonym cursor-pprev cursor-prev)
(def-cursor-p-synonym cursor-pcurrent cursor-current)

(defmacro def-cursor-dup-mover (myname basename) ; macro just for two functions ain't really good, but..
  `(defmethod ,myname ((cursor pm-secondary-cursor))
    (unless (cursor-initialized-p cursor) (return-from ,myname))
    (let ((ckey (current-raw-key-of cursor)))
      (multiple-value-bind (found k v)
	  (,basename cursor)
	(if (and found (equal ckey (current-raw-key-of cursor)))
	    (values found k v)
	    (cursor-close cursor))))))

(def-cursor-dup-mover cursor-next-dup cursor-next)
(def-cursor-dup-mover cursor-prev-dup cursor-prev)

(def-cursor-p-synonym cursor-pnext-dup cursor-next-dup)
(def-cursor-p-synonym cursor-pprev-dup cursor-prev-dup)

(defmacro def-cursor-nodup-mover (name from-cache-fetcher cache-filler)
  `(defmethod ,name ((cursor pm-secondary-cursor))
    (unless (cursor-initialized-p cursor) (return-from ,name))
    (let ((okey (current-raw-key-of cursor)))
      (loop (multiple-value-bind (found k v)
		(,from-cache-fetcher cursor)
	      (cond
		((and found (not (equal (current-raw-key-of cursor) okey)))
		 (return-from ,name (values t k v)))
		((not found)
		 ,cache-filler
		 (return-from ,name (,from-cache-fetcher cursor)))
	      (t :continue)))))))

(def-cursor-nodup-mover cursor-next-nodup cursor-fetch-next-from-cache
  (cursor-fetch-auto cursor 'next-nodup 
		     (:key-compare '> :value-compare nil)
		     ((list (ensure-string okey)) :reset-cache t)))

(def-cursor-nodup-mover cursor-prev-nodup cursor-fetch-prev-from-cache
  (cursor-fetch-auto cursor 'prev-nodup
		     (:key-compare '< :value-compare nil :key-order "DESC")
		     ((list (ensure-string okey)) :reset-cache t :cache-prior t)))

(def-cursor-p-synonym cursor-pnext-nodup cursor-next-nodup)
(def-cursor-p-synonym cursor-pprev-nodup cursor-prev-nodup)

(def-cursor-p-synonym cursor-pset cursor-set key)
(def-cursor-p-synonym cursor-pset-range cursor-set-range key)

(defmethod cursor-get-both-range ((cursor pm-secondary-cursor) key pkey)
  (cursor-fetch-auto cursor 'get-both
		     (:key-compare nil :value-compare '>=)
		     ((list (key-parameter key btree)
			    (value-parameter pkey btree)) :reset-cache t))
  (cursor-fetch-next-from-cache cursor))

(defmethod cursor-get-both ((cursor pm-secondary-cursor) key pkey)
  (multiple-value-bind (found k v)
      (cursor-get-both-range cursor key pkey)
    (if (and found (ele::lisp-compare-equal pkey (current-value-of cursor)))
	(values t k v)
	(cursor-close cursor))))

(def-cursor-p-synonym cursor-pget-both cursor-get-both key pkey)
(def-cursor-p-synonym cursor-pget-both-range cursor-get-both-range key pkey)

(defmethod cursor-delete ((cursor pm-secondary-cursor))
  "Delete by cursor: deletes ALL secondary indices."
  (if (cursor-initialized-p cursor)
      (let ((pkey (current-value-of cursor)))
        (cursor-close cursor)
        (remove-kv pkey (primary (cursor-btree cursor))))
      (error "Can't delete with uninitialized cursor")))

(defmethod cursor-put ((cursor pm-secondary-cursor) value &rest rest)
  "Puts are forbidden on secondary indices.  Try adding to
the primary."
  (declare (ignore rest value))
  (error "Puts are forbidden on secondary indices.  Try adding to the primary."))
