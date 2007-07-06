(in-package :db-postmodern)

(defclass pm-btree (btree)
  ((dbtable :accessor table-of :initform nil :initarg :table-name)
   (key-type :accessor key-type-of :initform nil)
   (value-type :accessor value-type-of :initform :object)
   (queries :accessor queries-of :initform nil))
  (:documentation "A SQL implementation of a BTree"))


(define-condition db-error (serious-condition) ())
(define-condition bad-db-parameter (db-error) ())
(define-condition suspicios-db-parameter (warning) ())

(defmacro ignore-bad-params (&body body)
  `(handler-case
    (progn ,@body)
    (bad-db-parameter (c)
     (declare (ignore c))
     nil)))

(defparameter +join-with-blob-optimization+ t)

(defvar *sc* nil)

(defun active-controller ()
  *sc*)

(defmacro with-trans-and-vars ((bt) &body body)
  `(let ((*sc* (or *sc* (get-con ,bt))))
    (declare (special *sc*))
    (ensure-transaction (:store-controller *sc*)
      (with-connection-for-thread (*sc*)
        ,@body))))

(defmacro with-vars ((bt) &body body)
  `(let ((*sc* (or *sc* (get-con ,bt))))
    (declare (special *sc*))
    (with-connection-for-thread (*sc*)
        ,@body)))

(defmethod initialize-instance :after ((bt pm-btree) &rest initargs)
  (declare (ignore initargs))
  (unless (table-of bt)
    (setf (table-of bt) (format nil "tree~a" (oid bt)))
    (with-trans-and-vars (bt)
      (when (postmodern:table-exists-p (table-of bt))
        (let ((rows (sp-meta-select (active-connection) (table-of bt))))
          (destructuring-bind (keytype valuetype) (first rows)
            (setf (key-type-of bt) (read-from-string keytype))
            (setf (value-type-of bt) (read-from-string valuetype))))))))

(defmethod duplicates-allowed-p ((bt pm-btree))
  nil)

(defmethod make-table ((bt pm-btree))
  (with-trans-and-vars (bt)
    (unless (table-of bt)
      (setf (table-of bt) (format nil "tree~a" (next-tree-number (active-connection) :row-reader 'first-value-row-reader))))
    (while-ignoring-warnings 
      (cl-postgres:exec-query (active-connection)
                              (format nil "create table ~a (qi ~a ~a not null, value ~a not null) with oids;"
                                      (table-of bt)
                                      (postgres-type (key-type-of bt))
                                      (if (duplicates-allowed-p bt)
                                          ""
                                          " unique ")
                                      (postgres-type (value-type-of bt)))))
    (sp-metatree-insert (active-connection)
                        (table-of bt)
                        (prin1-to-string (key-type-of bt))
                        (prin1-to-string (value-type-of bt))
                        :row-reader 'cl-postgres:ignore-row-reader)
  
    (when (duplicates-allowed-p bt)
      (cl-postgres:exec-query (active-connection)
                              (format nil "create index ~a_idx on ~a(qi,value);"
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
DECLARE
    do_update boolean := FALSE;
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
          do_update := TRUE;
    END IF;
    
    IF do_update THEN
       UPDATE " (table-of bt)
                          "
          SET value = val
          WHERE qi = the_key;
    ELSE
       INSERT INTO " (table-of bt) " (qi, value)
              VALUES (the_key, val);
    END IF;
END;
$$ LANGUAGE plpgsql;
")))
    sql))

(defmethod make-local-name ((bt pm-btree) name)
  (read-from-string (format nil "~a~a" (table-of bt) name)))

(defmethod register-query ((bt pm-btree) query-identifier sql)
  (let ((local-name (make-local-name bt query-identifier)))
    (pushnew (cons query-identifier
                   (list (symbol-name local-name)
                         local-name
                         sql))
             (queries-of bt)
             :key #'car)))

(defmethod prepare-local-queries ((bt pm-btree))
  (if (and +join-with-blob-optimization+
           (eq :object (value-type-of bt)))
      (register-query bt 'select (format nil "select bob from ~a,blob where qi=$1 and bid = value" (table-of bt)))
      (register-query bt 'select (format nil "select value from ~a where qi=$1" (table-of bt))))
  (register-query bt 'insert (format nil "select ins_upd_~a($1,$2)" (table-of bt)))
  (register-query bt 'delete (format nil "delete from ~a where qi=$1" (table-of bt))))

(defmethod initialized-p ((bt pm-btree))
  (not (null (key-type-of bt))))

(defun deserialize-binary-result (item)
  (if (and +join-with-blob-optimization+ (arrayp item))
      (deserialize-from-database item (active-controller))
      (deserialize-from-database (ensure-bob item) (active-controller))))

(defmethod btree-exec-prepared ((bt pm-btree) query-identifier params row-reader)
  (labels ((lookup-query (query-identifier)
             (cdr (assoc query-identifier (queries-of bt))))
           (ensure-registered-on-class (query-identifier)
             (or (lookup-query query-identifier)
                 (progn (prepare-local-queries bt)
                        (or (lookup-query query-identifier)
                            (error "btree-exec-prepared could not find the prepared query ~S in ~S" query-identifier (table-of bt))))))
           (ensure-prepared-on-connection (name-symbol name-string sql)
             (let ((meta (cl-postgres:connection-meta (active-connection))))
               (unless (gethash name-symbol meta)
                 (cl-postgres:prepare-query (active-connection) name-string sql)
                 (setf (gethash name-symbol meta) t)))))
    (destructuring-bind (name-string name-symbol sql)
        (ensure-registered-on-class query-identifier)
      (ensure-prepared-on-connection name-symbol name-string sql)
      (cl-postgres:exec-prepared (active-connection)
                                 name-string
                                 params
                                 row-reader))))

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
                                 (t (signal 'bad-db-parameter)))))
    (:string (cond
               ((stringp parameter) parameter)
               ((null parameter)
                (signal 'bad-db-parameter)
                "")
               (t (warn "Suspect string input to postgres-format.")
                  (format nil "~a" parameter))))
    (:object (princ-to-string (ensure-bid (serialize-to-base64-string parameter (active-controller)))))))

(defun postgres-value-to-lisp (value data-type)
  (ecase data-type
    (:integer value)
    (:string value)
    (:object (deserialize-binary-result value))))

(defun data-type (item)
  (typecase item
    (integer :integer)
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

(defmethod upgrade-btree-type ((old-bt pm-btree) data-type)
  "We started by guessing the key from the first value. If this was wrong, 
we need to make a new database table with a new keytype, copy the old values
and make the old instance refer to the new database table"
  (let ((bt (make-instance 'pm-btree)))
    (setf (key-type-of bt) data-type)
    (make-table bt)
    (map-btree #'(lambda (k v)
                   (setf (get-value k bt)
                         v))
               old-bt)
    (loop for slot in '(dbtable key-type value-type queries) do
         (setf (slot-value old-bt slot)
               (slot-value bt slot)))
    (setf bt nil)
    old-bt))

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
    (when (initialized-p bt)
      (with-trans-and-vars (bt)
        (let ((result (btree-exec-prepared bt 'select
                                           (list (key-parameter key bt))
                                           'first-value-row-reader)))
          (when result
            (setf value (postgres-value-to-lisp result (value-type-of bt))
                  exists-p t)))))
    (values value exists-p)))
;; Comment about implementation: with-trans-and-vars end up in a prog1,
;; which only return the primary return value. That's why values form is last.

(defmethod (setf get-value) (value key (bt pm-btree))
  (setf (internal-get-value key bt) value))

(defmethod (setf internal-get-value) (value key (bt pm-btree))
  (unless (initialized-p bt)
    (create-table-from-first-values bt key value))
  (unless (eq :object (key-type-of bt))
    (unless (eq (key-type-of bt) (data-type key))
      (upgrade-btree-type bt :object)))
  (with-trans-and-vars (bt)
    (btree-exec-prepared bt 'insert
                         (list (key-parameter key bt)
                               (value-parameter value bt))
                         'cl-postgres:ignore-row-reader))
  ;; Comment by Henrik 2007-may-08
  ;; Previously I had this wrapped in ignore-bad-params, but the recent upgrade might have solved that?
  ;; Remove this comment if everything works, and remove ignore-bad-params macro as well.
  ;;  (ignore-bad-params ;;TODO: Check why value and or key is sometimes nil and what to do
  ;;    )
  value)

(defmethod existsp (key (bt pm-btree))
  (when (initialized-p bt)
    (with-vars (bt)
      (when (btree-exec-prepared bt 'select
                                 (list (key-parameter key bt))
                                 'first-value-row-reader)
        t))))

(defmethod remove-kv (key (bt pm-btree))
  (when (initialized-p bt)
    (with-vars (bt)
      (btree-exec-prepared bt 'delete
                           (list (key-parameter key bt))
                           'cl-postgres:ignore-row-reader))))

