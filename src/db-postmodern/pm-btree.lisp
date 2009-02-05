(in-package :db-postmodern)

(defclass pm-btree (pm-executor)
  ((dbtable :accessor table-of :initform nil :initarg :table-name)
   (key-type :accessor key-type-of :initform nil :initarg :key-type)
   (value-type :accessor value-type-of :initform :object :initarg :value-type)
   (wrapper :accessor wrapper-of :initarg :wrapper))
  (:documentation "btree object of db-postmodern. used via a wrapper object")) 

(defclass pm-btree-wrapper (btree)
  ()
  (:documentation "wrapper object for btrees of db-postmodern"))

(defclass pm-special-btree-wrapper (pm-btree-wrapper)
  ((dbtable :accessor table-of :initform nil :initarg :table-name)
   (key-type :accessor key-type-of :initarg :key-type)
   (value-type :accessor value-type-of :initform :object :initarg :value-type ))
  (:documentation "wrapper for special (fixed) btrees"))

(defmacro with-trans-and-vars ((bt) &body body)
  `(let ((*sc* (or *sc* (get-con ,bt)))) ;; verify that sc is ours?
    (ensure-transaction (:store-controller *sc*)
      (with-connection-for-thread (*sc*)
	  ,@body))))

(defmacro with-vars ((bt) &body body)
  `(let ((*sc* (or *sc* (get-con ,bt))))
    (with-connection-for-thread (*sc*)
	,@body)))

(defmethod make-connection-btree ((bt pm-btree-wrapper))
  (make-instance 'pm-btree :wrapper bt))

(defmethod make-connection-btree ((bt pm-special-btree-wrapper))
  (make-instance 'pm-btree :wrapper bt :table-name (table-of bt) 
		 :key-type (key-type-of bt) :value-type (value-type-of bt)))

(defmethod get-connection-btree ((bt pm-btree-wrapper))
  (with-vars (bt)
    (let ((meta (cl-postgres:connection-meta (active-connection))))
    (or (gethash bt meta)
	(setf (gethash bt meta)
	      (make-connection-btree bt))))))

(defmethod internal-get-value (key (bt pm-btree-wrapper))
  (with-trans-and-vars (bt)
    (get-value key (get-connection-btree bt))))

(defmethod get-value (key (bt pm-btree-wrapper))
  (with-trans-and-vars (bt)
    (get-value key (get-connection-btree bt))))

(defmethod (setf internal-get-value) (value key (bt pm-btree-wrapper))
  (with-trans-and-vars (bt)
    (setf (get-value key (get-connection-btree bt)) value)))


(defmethod (setf get-value) (value key (bt pm-btree-wrapper))
  (with-trans-and-vars (bt)
    (setf (get-value key (get-connection-btree bt)) value)))

(defmethod remove-kv (key (bt pm-btree-wrapper))
  (with-trans-and-vars (bt)
    (remove-kv key (get-connection-btree bt))))

(defmethod get-con ((bt pm-btree))
  (get-con (wrapper-of bt)))

(defmethod oid ((bt pm-btree))
  (oid (wrapper-of bt)))

(defmethod existsp (key (bt pm-btree-wrapper))
  (with-trans-and-vars (bt)
    (existsp key (get-connection-btree bt))))

(defmethod make-table ((bt pm-btree-wrapper) &key suppress-metainformation) 
  (make-table (get-connection-btree bt) 
	      :suppress-metainformation suppress-metainformation))

(define-condition db-error (serious-condition) ())
(define-condition bad-db-parameter (db-error) ())
(define-condition suspicios-db-parameter (warning) ())

(defparameter +join-with-blob-optimization+ t)

(defun ignore-warning (condition)
   (declare (ignore condition))
   (muffle-warning))

(defmacro while-ignoring-warnings (&body body)
  `(handler-bind
    ((warning #'ignore-warning))
        (let ((cl-user::*break-on-signals* nil)) ;; convenient when debugging
          ,@body)))

(defvar *sc* nil)

(defun active-controller ()
  *sc*)

(defun btree-read-meta (bt)
  (with-trans-and-vars (bt)
    (let ((rows (sp-meta-select (active-connection) (princ-to-string (oid bt)))))
      (when rows ;; check if meta-data is present
	(destructuring-bind (tablename keytype valuetype) (first rows)
	  (setf (table-of bt) tablename
		(key-type-of bt) (read-from-string keytype)
		(value-type-of bt) (read-from-string valuetype)))
	(assert (postmodern:table-exists-p (table-of bt)))
	t))))

(defun check/initialize-btree (bt)
  (or (initialized-p bt)
      (with-trans-and-vars (bt)
	(btree-read-meta bt))))

(defmethod initialize-instance :after ((bt pm-btree) &rest rest)
  (declare (ignore rest))
  (unless (table-of bt) ; if table name is given it is a system btree, it has its own initialization protocol
    (with-trans-and-vars (bt)
      (unless (btree-read-meta bt)
	;; no meta data, initialize table name with a default value
	(setf (table-of bt) (format nil "tree~a" (oid bt)))))))

(defmethod duplicates-allowed-p ((bt pm-btree))
  nil)

(defmethod make-table ((bt pm-btree) &key suppress-metainformation)
  (with-trans-and-vars (bt)
    (unless (table-of bt)
      (error "btree is not initialized properly"))
    (while-ignoring-warnings 
      (cl-postgres:exec-query (active-connection)
                              (format nil "create table ~a (qi ~a ~a not null, value ~a not null);"
                                      (table-of bt)
                                      (postgres-type (key-type-of bt))
                                      (if (duplicates-allowed-p bt)
                                          ""
                                          " unique ")
                                      (postgres-type (value-type-of bt)))))
    (unless suppress-metainformation
      (sp-metatree-insert (active-connection)
			  (princ-to-string (oid bt))
			  (table-of bt)
			  (prin1-to-string (key-type-of bt))
			  (prin1-to-string (value-type-of bt))
			  :row-reader 'cl-postgres:ignore-row-reader))  
    (when (duplicates-allowed-p bt)
      (cl-postgres:exec-query (active-connection)
                              (format nil "create unique index ~a_idx on ~a(qi,value);"
                                      (table-of bt)
                                      (table-of bt))))
  
    (cl-postgres:exec-query (active-connection)
                            (make-plpgsql-insert/update bt))))


(defmethod make-plpgsql-insert/update ((bt pm-btree))
  (let ((sql (concatenate 'string
                          (format nil "CREATE OR REPLACE FUNCTION ins_upd_~a" (table-of bt))
                          (format nil "(the_key ~a, val ~a) RETURNS void AS $$"
                                  (postgres-type (key-type-of bt))
                                  (postgres-type (value-type-of bt)))
                          "                             
BEGIN
    PERFORM qi FROM " (table-of bt)
                          "
          WHERE qi = the_key "
                          (if (duplicates-allowed-p bt)
                              " and value=val;"
                              ";"
                              )
                          "
    IF FOUND THEN 
"   (if (duplicates-allowed-p bt)
	"NULL;"
	(concatenate 'string
	"UPDATE " (table-of bt)
                          "
          SET value = val
          WHERE qi = the_key;"))
"   ELSE
    INSERT INTO " (table-of bt) " (qi, value)
              VALUES (the_key, val);
    END IF;"

#+ele-global-sync-cache
(format nil "PERFORM notify_btree_update(~a, the_key::text);"	(oid bt))

"END;
$$ LANGUAGE plpgsql;
")))
    sql))

(defmethod executor-prefix ((bt pm-btree))
  (table-of bt))

(defmethod prepare-local-queries ((bt pm-btree))
  (if (and +join-with-blob-optimization+
           (eq :object (value-type-of bt)))
      (register-query bt 'select (format nil "select bob from ~a,blob where qi=$1 and bid = value" (table-of bt)))
      (register-query bt 'select (format nil "select value from ~a where qi=$1" (table-of bt))))
  (register-query bt 'insert (format nil "select ins_upd_~a($1,$2)" (table-of bt)))
  (register-query bt 'delete (format nil "delete from ~a where qi=$1" (table-of bt)))
  #+ele-global-sync-cache
  (register-query bt 'notify-update (format nil "select notify_btree_update(~a, $1::text)" (oid bt))))

(defmethod btree-exec-prepared ((bt pm-btree) query-identifier params row-reader)
  (executor-exec-prepared bt query-identifier params row-reader))

(defmethod initialized-p ((bt pm-btree))
  (not (null (key-type-of bt))))


(defun deserialize-binary-result (item)
  (if (and +join-with-blob-optimization+ (arrayp item))
      (deserialize-from-database item (active-controller))
      (deserialize-from-database (ensure-bob item) (active-controller))))

(defun postgres-format (parameter data-type)
  (ecase data-type
    (:integer (princ-to-string (cond
                                 ((integerp parameter) parameter)
                                 ((numberp parameter)
                                  ;; TODO: Do something here
                                  #+nil (signal suspicious-paramtere) 
                                  #+nil (warn "Suspect numeric input to postgres-format when expecting integer")
                                  
                                  ;; Round up, because it is probably a query in for greater than.
                                  (ceiling parameter))
                                 (t (error 'bad-db-parameter)))))
    (:string (cond
               ((stringp parameter) (if (>= (length parameter) 2000)
                                        (subseq parameter 0 2000)
                                        parameter))
               ((null parameter)
                (error 'bad-db-parameter))
               (t (warn "Suspect string input to postgres-format.")
                  (format nil "~a" parameter))))
    (:object (princ-to-string (ensure-bid (serialize-to-postmodern parameter (active-controller)))))    
    ))

(defun postgres-value-to-lisp (value data-type)
  (ecase data-type
    (:integer value)
    (:string value)
    (:object (deserialize-binary-result value))))

(defun data-type (item)
  (typecase item
    ((signed-byte 64) :integer)
    (string  :string)
    (t :object)))

(defun postgres-type (data-type)
  (ecase data-type
    (:integer 'bigint)
    (:string 'text)
    (:object 'bigint))) ;; Object are integers that refer to blob table

(defmethod create-table-from-first-values ((bt pm-btree) key value)
  (declare (ignorable value))
  (setf (key-type-of bt) (data-type key))
  (make-table bt))

(defmethod upgrade-btree-type ((old-bt pm-btree) key-type val-type)
  "We started by guessing the key from the first value. If this was wrong, 
we need to make a new database table with a new keytype, copy the old values
and replace old table with new. this is the really evil function."
  ;; in SERALIZABLE isolation mode data is "fixed" at time of the 
  ;; first statement in transaction, so to ensure that we're copying
  ;; latest data we'll have to make sure that we lock table by the first
  ;; statement. to do this, we'll have to abort current transaction,
  ;; execute upgrade in a separate transaction and retry current transaction.
  ;; it is implemented via fix-and-retry-txn mechanism
  (fix-and-retry-txn
   (lambda () 
     ;; disable caching during upgrade, to be sure
     (let* ((prev-cache-mode *cache-mode*)
	    (*cache-mode* nil))
       (with-trans-and-vars (old-bt)

	 ;; lock table so nobody changes data while we're copying it.
	 (cl-postgres:exec-query (active-connection)
				 (format nil "LOCK TABLE ~a IN SHARE MODE" 
					 (table-of old-bt)))

	 (let ((bt (make-instance (class-of old-bt)
				  :wrapper (wrapper-of old-bt))))
	   (setf (key-type-of bt) key-type
		 (value-type-of bt) val-type
		 (table-of bt) (format nil "table~a"
				       (next-oid (active-controller))))
						       
	       (make-table bt :suppress-metainformation t)
	       
	       ;; copy data
	       (map-btree #'(lambda (k v)
			      (setf (get-value k bt)
				    v))
			  (wrapper-of old-bt))

	       ;; update metainformation
	       (sp-metatree-update (active-connection)
				   (prin1-to-string (oid old-bt))
				   (table-of bt)
				   (prin1-to-string key-type)
				   (prin1-to-string val-type)
				   :row-reader 'cl-postgres:ignore-row-reader)

	       (cl-postgres:exec-query (active-connection)
				       (format nil "DROP TABLE ~a" 
					       (table-of old-bt)))

	       (cl-postgres:exec-query (active-connection) 
			      (format nil "DROP FUNCTION ins_upd_~a(~a, ~a)"
				      (table-of old-bt)
				      (postgres-type (key-type-of old-bt))
				      (postgres-type (value-type-of old-bt))))

	       (setf (key-type-of old-bt) key-type
		     (value-type-of old-bt) val-type
		     (table-of old-bt) (table-of bt))
	       
	       ;; obliterate query list to refresh it
	       (setf (queries-of old-bt) nil)

	       #+ele-global-sync-cache
	       (when (eq prev-cache-mode :global-sync-cache)
		 (invalidate-sync-cache (active-controller)))

	       old-bt))))))

(defun key-parameter (key bt)
  (postgres-format key (key-type-of bt)))

(defun value-parameter (value bt)
  (postgres-format value (value-type-of bt)))


;;------------------------------------------

(defmethod get-value (key (bt pm-btree))
  (declare (optimize (debug 3)))
  (multiple-value-bind (value exists)
      (internal-get-value key bt)
    (values value exists)))

(defmethod internal-get-value (key (bt pm-btree))
  (declare (optimize (debug 3)))
  (let (value exists-p)

    (multiple-value-bind (cached-value cache-hit)
	(txn-cache-get-value bt key)
      (when cache-hit
	(setf value cached-value
	      exists-p t)))

    (when (and (not exists-p)
               (check/initialize-btree bt)
               key)
      (with-vars (bt)
        (let ((result (btree-exec-prepared bt 'select
                                           (list (key-parameter key bt))
                                           'first-value-row-reader)))
          (when result
            (setf value (postgres-value-to-lisp result (value-type-of bt))
                  exists-p t)

	    ;;update cache
	    (txn-cache-set-value bt key value)))))
    (values value exists-p)))
;; Comment about implementation: with-trans-and-vars end up in a prog1,
;; which only return the primary return value. That's why values form is last.

(defmethod (setf get-value) (value key (bt pm-btree))
  (setf (internal-get-value key bt) value))

(defmethod maybe-upgrade-btree-type ((bt pm-btree) key value)
  (let ((bt-kt (key-type-of bt))
	(bt-vt (value-type-of bt)))
    (flet ((need-upgrade ()
	     (let ((upgrade-key (not (or (eq bt-kt :object)
					 (eq bt-kt (data-type key)))))
		   (upgrade-value (not (or (eq bt-vt :object)
					   (eq bt-vt (data-type value))))))
	       (values (or upgrade-key upgrade-value)
		       upgrade-key
		       upgrade-value))))
      (when (need-upgrade)
	;; it could be that some other process have upgraded btree, but we
	;; do not know it yet, try re-reading metadata
	(with-trans-and-vars (bt)
	  (btree-read-meta bt)
	  (setf (queries-of bt) nil)

	  (setf bt-kt (key-type-of bt)
		bt-vt (value-type-of bt))
	  (multiple-value-bind (need-upgrade upgrade-key upgrade-value)
	      (need-upgrade)
	    (when need-upgrade
	      (upgrade-btree-type bt 
				  (if upgrade-key :object bt-kt)
				  (if upgrade-value :object bt-vt)))))))))


(defmethod (setf internal-get-value) (value key (bt pm-btree))
  (when key
    (if (initialized-p bt)
	(maybe-upgrade-btree-type bt key value)
	(or (check/initialize-btree bt)
	    (progn (create-table-from-first-values bt key value)
		   (assert (initialized-p bt))))) ;; Should be initialized now

    (txn-cache-set-value bt key value)

    (with-trans-and-vars (bt)
      (btree-exec-prepared bt 'insert
                           (list (key-parameter key bt)
                                 (value-parameter value bt))
                           'cl-postgres:ignore-row-reader)))
  value)

(defmethod existsp (key (bt pm-btree))
  
  (multiple-value-bind (value exists)
      (txn-cache-get-value bt key)
    (declare (ignore value))
    (when exists (return-from existsp t)))

  (when (check/initialize-btree bt)
    (with-vars (bt)
      (when (btree-exec-prepared bt 'select
                                 (list (key-parameter key bt))
                                 'first-value-row-reader)
        t))))

(defmethod remove-kv (key (bt pm-btree))

  (txn-cache-clear-value bt key)

  (when (check/initialize-btree bt)
    (with-vars (bt)
      (btree-exec-prepared bt 'delete
                           (list (key-parameter key bt))
                           'cl-postgres:ignore-row-reader)
      #+ele-global-sync-cache
      (btree-exec-prepared bt 'notify-update
			   (list (key-parameter key bt))
                           'cl-postgres:ignore-row-reader))))

;; btree with duplicates

(defclass pm-dup-btree (pm-btree) ())

(defclass pm-dup-btree-wrapper (dup-btree pm-btree-wrapper)
  ())

(defmethod make-connection-btree ((bt pm-dup-btree-wrapper))
  (make-instance 'pm-dup-btree :wrapper bt))

  
(defmethod duplicates-allowed-p ((bt pm-dup-btree)) t)

(defmethod create-table-from-first-values :before ((bt pm-dup-btree) key value)
  (declare (ignorable key))
  (setf (value-type-of bt) (data-type value)))

(defmethod prepare-local-queries :after ((bt pm-dup-btree))
  (register-query bt 'delete-both (format nil "delete from ~a where qi=$1 and value=$2" (table-of bt))))

(defmethod remove-kv-pair (k v (bt pm-dup-btree-wrapper))
  (with-trans-and-vars (bt)
    (remove-kv-pair k v (get-connection-btree bt))))

(defmethod remove-kv-pair (k v (bt pm-dup-btree))

  (txn-cache-clear-value bt k)
  
  (when (check/initialize-btree bt)
    (with-trans-and-vars (bt)
      (btree-exec-prepared bt 'delete-both
                           (list (postgres-format k (key-type-of bt))
                                 (postgres-format v (value-type-of bt)))
                           'cl-postgres:ignore-row-reader)

       #+ele-global-sync-cache
       (btree-exec-prepared bt 'notify-update
			    (list (key-parameter k bt))
			    'cl-postgres:ignore-row-reader))))
