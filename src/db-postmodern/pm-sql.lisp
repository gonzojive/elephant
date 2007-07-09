(in-package :db-postmodern)

(defparameter *stored-procedures* nil)

(defmacro define-stored-procedure (sql-code)
  `(push ,sql-code *stored-procedures*))

(define-stored-procedure "
CREATE OR REPLACE FUNCTION sp_ensure_bid (value bytea) RETURNS bigint as $$
DECLARE
    value_md5 bytea;
    retrieved_bid bigint;
    existing_p boolean := FALSE;
    
BEGIN
    /* md5 encodes to a hex string 32 chars which is converted to an bytea of 16 bytes */
    select decode(md5(value),'hex') into value_md5;

    select bid into retrieved_bid from blob where bob_md5=value_md5 and bob=value limit 1;
    IF FOUND THEN 
          existing_p := TRUE;
    END IF;

    IF existing_p THEN
        return retrieved_bid;
    ELSE
        insert into blob (bob,bob_md5) values(value,value_md5);
        return currval('blob_bid_seq');
    END IF;
END;
$$ LANGUAGE plpgsql;

")

(defun ignore-warning (condition)
   (declare (ignore condition))
   (muffle-warning))

(defmacro while-ignoring-warnings (&body body)
  `(handler-bind
    ((warning #'ignore-warning))
        (let ((cl-user::*break-on-signals* nil)) ;; convenient when debugging
          ,@body)))

(defun init-stored-procedures (con)
;;  (ignore-errors
;;    (cl-postgres:exec-query con "CREATE LANGUAGE plpgsql;"))
  (loop for sp-def in *stored-procedures* do
        (cl-postgres:exec-query con sp-def)))

(defparameter *stats* (make-hash-table))

(defun add-statistics (identifier run-time)
  (let ((x (gethash identifier *stats*)))
    (if x
        (progn
          (incf (car x))
          (incf (cdr x) run-time))
        (setf (gethash identifier *stats*) (cons 0 run-time)))))

(defmacro with-stat-collector ((identifier) &body body)
  `(let ((before (get-internal-real-time)))
    (prog1
        (progn ,@body)
      (add-statistics ,identifier (- (get-internal-real-time) before)))))


(defun show-statistics ()
  (maphash #'(lambda (id data)
             (destructuring-bind (calls . time) data
               (setf time  (/ time internal-time-units-per-second))
               (format t "~%~a calls:~a time:~f avg:~f" id calls time (when (> calls 0) (/ time  calls)))))
           *stats*))

(cl-postgres:def-row-reader first-value-row-reader (fields)
  (let (value-set value)
    (loop :while (cl-postgres:next-row)
          :collect (loop :for field :across fields
                         :do (if value-set
                                 (cl-postgres:next-field field)
                                 (setf value-set t
                                       value (cl-postgres:next-field field)))))
    (values value value-set)))

(defmacro define-prepared-query (name parameters sql-definition) ;;TODO rebind to make macro safer
  ;;TODO:Cleanup this and code in db-btree.
  (flet ((safe-sql-name (name)
           (substitute #\_ #\- (string name) :test #'char=))
         (tweak-param (parameter)
           (cond
             ((eq 'key parameter) `(setf key (integer-to-string (ensure-bid key))))
             ((eq 'value parameter) `(setf value (integer-to-string (ensure-bid value))))
             (t nil))))
    (let ((sp-name (safe-sql-name name)))
      `(defun ,name (connection ,@parameters &key (row-reader 'cl-postgres:list-row-reader))
         ,@(mapcar #'tweak-param parameters)
         (let ((meta (cl-postgres:connection-meta connection)))
           (unless (gethash ',name meta)
             (setf (gethash ',name meta) t)
             (cl-postgres:prepare-query connection ,(symbol-name name) ,sql-definition))) ;;same as ensure-prepared-on-conn in db-btree
         (with-stat-collector (,sp-name)
           (cl-postgres:exec-prepared connection ,(symbol-name name) (list ,@parameters) row-reader))))))

(define-prepared-query next-tree-number ()
  "select nextval('tree_seq');")

(define-prepared-query sp-select-blob-bob-by-bid (bid)
  "select bob from blob where bid=$1")

(define-prepared-query sp-ensure-bid (buffer)
  "select sp_ensure_bid($1)")

(define-prepared-query sp-meta-select (tablename)
   "select keytype,valuetype from metatree where tablename=$1")

(define-prepared-query sp-metatree-insert (tablename keytype valuetype)
   "insert into metatree(tablename,keytype,valuetype) values ($1,$2,$3)")

(defun ensure-bob (bid)
  (sp-select-blob-bob-by-bid (active-connection) (integer-to-string bid) :row-reader 'first-value-row-reader))

(defun serialize-to-postmodern (x sc)
  "Encode object using the store controller's serializer format,"
  (with-buffer-streams (out-buf)
    (elephant-memutil::buffer-read-byte-vector 
     (serialize x out-buf sc))))

(defun ensure-bid (bob)
  (declare (type (simple-array (unsigned-byte 8)) bob))
  (sp-ensure-bid (active-connection) bob :row-reader 'first-value-row-reader))

(defun create-base-tables (connection)
  (dolist (stmt '("create sequence tree_seq;"
                  "create sequence blob_bid_seq;"
                  "create table blob (
bid bigint primary key not null default nextval('blob_bid_seq'),
bob_md5 bytea not null,
bob bytea not null);"
                  
                  "create table metatree (
tablename text primary key not null,
keytype text not null,
valuetype text not null);"
                  "create index idx_blob_bob_md5 on blob (bob);"
                  ;;  "create index idx_blob_bid on blob (bid);" ;;already made implicitly when defining primary key for table
                  "create sequence persistent_seq start with 10;")) ;; make room for some system tables
    (cl-postgres:exec-query connection stmt)))

(defun base-table-existsp (con)
  (with-conn (con) (postmodern:table-exists-p 'blob)))
