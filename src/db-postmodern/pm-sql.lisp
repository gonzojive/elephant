(in-package :db-postmodern)

;; One postgresql limitation is that indexes can not be created on
;; columns longer than about 2,000 characters.
;; 
;; We have strings as keys but truncate the strings after 2000 chars.
;; This is not perfect but it will probably not happen often.

(eval-when (:compile-toplevel :load-toplevel :execute)
  ;; enables global cache that is synchonized accross instances
  (pushnew :ele-global-sync-cache cl::*features*))

(defvar *cache-mode* nil)

;;--------- Stored procedures ---------

(defparameter *stored-procedures* nil)

(defmacro define-stored-procedure (sql-code)
  `(push ,sql-code *stored-procedures*))

;;   Postgresql unlike most databases automatically aborts transactions if an error occurs,
;;   without letting the programmer decide what to do. A way around is to use a savepoint
;;   when expecting an error.
;;   http://archives.postgresql.org/pgsql-jdbc/2006-04/msg00002.php
;;
;;   But this only works inside a transcation..
(defmacro with-safe-postgres-error-handler ((connection &optional (error-object nil))
                                            statement &body error-body)
  (let ((savepoint (princ-to-string (gensym)))
        (con (gensym)))
    `(let ((,con ,connection))
       (handler-case
           (progn
             (ignore-errors ;; We might not be in a transaction, so wrap in ignore errors
               (cl-postgres:exec-query ,con ,(concatenate 'string "SAVEPOINT " savepoint)))
             ,statement)
         (cl-postgres:database-error (,@error-object)
           (ignore-errors 
            (cl-postgres:exec-query ,con ,(concatenate 'string "ROLLBACK TO " savepoint)))
           ,@error-body)))))

(defmacro safe-ignore-postgres-error ((connection) &body body)
  `(with-safe-postgres-error-handler (,connection) (progn,@body)))

(defun init-stored-procedures (con)
  (safe-ignore-postgres-error (con)
    (cl-postgres:exec-query con "CREATE LANGUAGE plpgsql;"))
  (loop for sp-def in *stored-procedures* do
       (cl-postgres:exec-query con sp-def)))

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

;; ---------- collecting data ---------

(defparameter *performance-stats* (make-hash-table))
(defvar *collect-performance-stats* nil)

(defun add-performance-statistics (identifier run-time)
  (let ((x (gethash identifier *performance-stats*)))
    (if x
        (progn
          (incf (car x))
          (incf (cdr x) run-time))
        (setf (gethash identifier *performance-stats*) (cons 0 run-time)))))

(defmacro with-performance-stat-collector ((identifier) &body body)
  `(while-connecting-performance-stats ,identifier
                           (lambda () ,@body)))

(defun while-connecting-performance-stats (identifier function)
  (if *collect-performance-stats*
      (let ((before (get-internal-real-time)))
        (prog1
            (funcall function)
          (add-performance-statistics identifier (- (get-internal-real-time) before))))
      (funcall function)))

(defun reset-performance-stats ()
  (setf *performance-stats* (make-hash-table)))

(defun start-collecting-performance-stats ()
  (setf *collect-performance-stats* t))

(defun show-performance-statistics ()
  (maphash #'(lambda (id data)
             (destructuring-bind (calls . time) data
               (setf time  (/ time internal-time-units-per-second))
               (format t "~%~a calls:~a time:~f avg:~f" id calls time (when (> calls 0) (/ time  calls)))))
           *performance-stats*))

;;--------- executing prepared queries ---------

(defclass pm-executor ()
   ((queries :accessor queries-of :initform nil)))

(defgeneric prepare-local-queries (pm-executor))

(defmethod prepare-local-queries ((ex pm-executor))
  nil)

(defgeneric executor-prefix (pm-executor))

(defmethod executor-prefix ((ex pm-executor))
  (declare (ignorable ex))
  "")

(defmethod make-local-name ((ex pm-executor) name)
  (read-from-string (format nil "~a~a" (executor-prefix ex) name)))

(defmethod register-query ((ex pm-executor) query-identifier sql)
  (let ((local-name (make-local-name ex query-identifier)))
    (pushnew (cons query-identifier
                   (list (symbol-name local-name)
                         local-name
                         query-identifier
                         sql))
             (queries-of ex)
             :key #'car)))

(defmethod executor-exec-prepared ((ex pm-executor) query-identifier params row-reader)
  (labels ((lookup-query (query-identifier)
             (cdr (assoc query-identifier (queries-of ex))))
           (ensure-registered-on-class (query-identifier)
             (or (lookup-query query-identifier)
                 (progn (prepare-local-queries ex)
                        (or (lookup-query query-identifier)
                            (error "executor-exec-prepared didn't find the query ~S"
                                   query-identifier)))))
           (ensure-prepared-on-connection (name-symbol name-string sql)
             (let ((meta (cl-postgres:connection-meta (active-connection))))
               (unless (gethash name-symbol meta)
                 (handler-case
                     (cl-postgres:prepare-query (active-connection) name-string sql)
                   (cl-postgres:database-error (e)
                     (cond
                       ((string= (cl-postgres:database-error-code e)
                                 "42P05")
                   ;; TODO note 20070810: Ugly but I sometimes get:
                   ;;Database error 42P05: prepared statement "TREE140CURSOR-SET-HELPER" already exists
                   ;; Despite the attempts above trying to check if it is already prepared.
                   ;; This error in itself does not cause any problems, so we can ignore it.
                   ;; But this should be investigated
                   ;;
                   ;; Update: Maybe it has to do with connection pooling within postmodern?
                         'ignoring-this-error)
                       (t (error e)))))
                 (setf (gethash name-symbol meta) t))))
           (exec-prepared (name-string)
             (cl-postgres:exec-prepared (active-connection)
                                 name-string
                                 params
                                 row-reader)))
    (destructuring-bind (name-string name-symbol stat-identifier sql)
        (ensure-registered-on-class query-identifier)
      (ensure-prepared-on-connection name-symbol name-string sql)
      (with-performance-stat-collector (stat-identifier)
        (handler-case
            (exec-prepared name-string)
          (cl-postgres:database-error (e)
            ;; Sometimes the prepared statement might hold references to old oids,
            ;; which might be have been dropped after a rollback. For safety, try
            ;; to remove the prepared statement and prepare it again
            (cond
              ((string= (cl-postgres:database-error-code e)
                        "42P01")
               ;; It seems that this error automatically drops the transaction! Postgresql bug?
               (cl-postgres:exec-query (active-connection) (concatenate 'string "DEALLOCATE " name-string))
               (cl-postgres:prepare-query (active-connection) name-string sql)
               (exec-prepared name-string))
              (t (error e)))))))))

;;---------------- Global queries -----------

(defparameter *global-queries* nil)

(defun initialize-global-queries (executor)
  (mapcar #'(lambda (fn)
              (funcall fn executor))
          *global-queries*))

(defmacro define-prepared-query (name parameters sql-definition) ;;TODO rebind to make macro safer
  `(let ((name ',name))
     (push #'(lambda (ex)
               (register-query ex name ,sql-definition))
           *global-queries*)
     (defun ,name (connection ,@parameters &key (row-reader 'cl-postgres:list-row-reader))
       (declare (ignore connection)) ;; TODO remove connetion
       (executor-exec-prepared (active-controller) name (list ,@parameters) row-reader))))

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

;; ------------- Misc functions ---------------

(cl-postgres:def-row-reader first-value-row-reader (fields)
  (let (value-set value)
    (loop :while (cl-postgres:next-row)
          :collect (loop :for field :across fields
                         :do (if value-set
                                 (cl-postgres:next-field field)
                                 (setf value-set t
                                       value (cl-postgres:next-field field)))))
    (values value value-set)))

(defun ensure-bob (bid)
  (sp-select-blob-bob-by-bid (active-connection) (princ-to-string bid) :row-reader 'first-value-row-reader))

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
                  "create index idx_blob_bob_md5 on blob (bob_md5);"
                  ;;  "create index idx_blob_bid on blob (bid);" ;;already made implicitly when defining primary key for table
                  "create sequence persistent_seq start with 10;")) ;; make room for some system tables
    (cl-postgres:exec-query connection stmt)))

(defun create-message-table (connection)
  "This table is (for now) only used as a flag that everything went ok with the initialization"
  (cl-postgres:exec-query connection "create table message(message text);"))

#+ele-global-sync-cache
(defun create-sync-cache-tables (connection)
  (dolist (stmt '(
"CREATE TABLE transaction_log (txn_id integer PRIMARY KEY, commit_time double precision not null);"
"CREATE TABLE update_log (txn_id integer not null, id integer not null, key text not null);"
"CREATE INDEX update_log_index ON update_log (txn_id);"
"CREATE INDEX transaction_log_index ON transaction_log (commit_time);"
"CREATE FUNCTION notify_btree_update (id integer, the_key text) RETURNS void AS $$
BEGIN END;
$$ LANGUAGE plpgsql;"
"CREATE SEQUENCE txn_id;"))
    (cl-postgres:exec-query connection stmt)))

#+ele-global-sync-cache
(defun bootstrap-sync-cache (connection)
  (cl-postgres:exec-query 
   connection 
   "INSERT INTO transaction_log (txn_id, commit_time) VALUES (nextval('txn_id'), 0.0);"))