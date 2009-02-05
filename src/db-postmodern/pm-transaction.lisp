;;; -*- Mode: Lisp; Syntax: ANSI-Common-Lisp; Base: 10 -*-
;;;
;;; pm-transactions.lisp -- Top level transaction support for elephant postmodern
;;; 
;;; part of
;;;
;;; Elephant: an object-oriented database for Common Lisp
;;;
;;; Copyright (c) 2007 Henrik Hjelte <hhjelte@common-lisp.net>
;;;
;;; Elephant users are granted the rights to distribute and use this software
;;; as governed by the terms of the Lisp Lesser GNU Public License
;;; (http://opensource.franz.com/preamble.html), also known as the LLGPL.
;;;

(in-package :db-postmodern)

(defvar *txn-value-cache* nil)

(defun txn-cache-get-value (bt key)
  (cache-get-value *txn-value-cache* bt key))

(defun txn-cache-set-value (bt key value)
  (cache-set-value *txn-value-cache* bt key value))

(defun txn-cache-clear-value (bt key)
  (cache-clear-value *txn-value-cache* bt key))

(defun execute-transaction-one-try (sc txn-fn always-rollback)
  (let (tran commited
	(*txn-value-cache* (make-value-cache sc))
	(*current-transaction* (make-transaction-record sc nil))
	(*store-controller* sc))
    (incf (tran-count-of sc))
    (setf tran (controller-start-transaction sc))
    (unwind-protect
	 (multiple-value-prog1 	     
	     (funcall txn-fn) 
	   (unless always-rollback ; automatically commit unless always-rollback is on
	     (controller-commit-transaction sc tran)
	     (setf commited t)))
      (unless commited (controller-abort-transaction sc tran))
      (decf (tran-count-of sc)))))

(defmacro with-concurrency-errors-handler (&body body)
  "execute body with a handler catching postgres concurrency errors 
   and invoking restart-transaction restart automatically"
  `(handler-bind
    ((cl-postgres:database-error
      (lambda (c)
	(let ((err-code (cl-postgres:database-error-code c)))
	  (when (or (string= err-code "40001") ; SERIALIZATION FAILURE
		    (string= err-code "40P01")); DEADLOCK DETECTED
	    (invoke-restart 'retry-transaction c))))))
    ,@body))

(defmethod execute-transaction ((sc postmodern-store-controller) txn-fn
				&key (always-rollback nil) 
				(retry-cleanup-fn nil)
				(retries 10) &allow-other-keys)
  (with-postmodern-conn ((controller-connection-for-thread sc))
    (if (> (tran-count-of sc) 0)

	;; SQL doesn't support nested transaction
	;; TODO: perhaps it's worth detecting abnormal exit here 
	;; and abort parent transaction too.	
	(with-concurrency-errors-handler (funcall txn-fn))

	(loop named txn-retry-loop
	  ;; NB: it does (1+ retries) attempts, 1 try + retries.
	  for try from retries downto 0 
	  do (block txn-block
	       (restart-bind ((retry-transaction
			       (lambda (&optional condition) 
				 (when (and retry-cleanup-fn 
					    (not (= try 0))) ; cleanup is skipped when we are exiting
				   (funcall retry-cleanup-fn condition sc))
				 (return-from txn-block))
			       :report-function (lambda (s) (princ "retry db-postmodern transaction" s)))
			      (abort-transaction 
			       (lambda () (return-from txn-retry-loop))))
			     (with-concurrency-errors-handler 
			       (return-from txn-retry-loop
				 (execute-transaction-one-try sc txn-fn always-rollback)))))
	  finally (error 'transaction-retry-count-exceeded
			 :format-control "Transaction exceeded the ~A retries limit"
			 :format-arguments (list retries)
			 :count retries)))))

(defmethod controller-start-transaction ((sc postmodern-store-controller) &key &allow-other-keys)
  (with-postmodern-conn ((controller-connection-for-thread sc))
    (let ((transaction (make-instance 'postmodern::transaction-handle)))
      (postmodern:execute "BEGIN ISOLATION LEVEL SERIALIZABLE")
      transaction)))

(defmethod controller-commit-transaction ((sc postmodern-store-controller) transaction &key &allow-other-keys)
  (with-postmodern-conn ((controller-connection-for-thread sc))
    (value-cache-commit *txn-value-cache*)
    (postmodern:commit-transaction transaction)))

(defmethod controller-abort-transaction ((sc postmodern-store-controller) transaction &key &allow-other-keys)
  (with-postmodern-conn ((controller-connection-for-thread sc))
    (postmodern:abort-transaction transaction)))
