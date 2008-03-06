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

(defmethod execute-transaction ((sc postmodern-store-controller) txn-fn
				&key (retries 50) (always-rollback nil) &allow-other-keys)
  (with-postmodern-conn ((controller-connection-for-thread sc))
    (let (savepoint (try 0))
      (tagbody
        retry (incf try)
              (when (>= try retries)
                (cerror "Retry transaction again?"
		       'transaction-retry-count-exceeded
		       :format-control "Transaction exceeded the limit of ~A retries"
		       :format-arguments (list retries)
		       :count retries))
              ;(format t "txn-mgr (thr ~A): try ~A~%" sb-thread::*current-thread* try)
              ;; XXX honor max retries
              (if (> (tran-count-of sc) 0) ;; SQL doesn't support nested transaction
                (progn
                  ;(setf savepoint (princ-to-string (gensym)))
                  ;(set-savepoint (active-connection) savepoint)
                  ;(setf savepoint nil)
                  ;(format t "detected nested transaction~%")
                  (return-from execute-transaction (funcall txn-fn)))
                (let (tran)
                  (handler-case
                    (let (commited (*txn-value-cache* (make-value-cache sc)))
                      (incf (tran-count-of sc))
                      (unwind-protect
                        (return-from execute-transaction
                          (prog2 
                            (setf tran (controller-start-transaction sc))
                            (funcall txn-fn) ;; this gets returned
                            (unless always-rollback ;; automatically commit unless always rollback
                              (controller-commit-transaction sc tran)
                              (setf commited t))))
                        (unless commited (controller-abort-transaction sc tran))
                        (decf (tran-count-of sc))))
                    (dbpm-error (e)
                      (warn "dbpm txn manager: caught error ~A~%" (errno e))
                      (cond
                        ((string= (errno e) "40P01")
                         ;(if savepoint
                         ;(rollback-to-savepoint (active-connection) savepoint)
                         (controller-abort-transaction sc tran)
                         (go retry)))))))))))

(defmethod controller-start-transaction ((sc postmodern-store-controller) &key &allow-other-keys)
  (with-postmodern-conn ((controller-connection-for-thread sc))
    (let ((transaction (make-instance 'postmodern::transaction-handle)))
      (postmodern:execute "BEGIN")
      transaction)))

(defmethod controller-commit-transaction ((sc postmodern-store-controller) transaction &key &allow-other-keys)
  (with-postmodern-conn ((controller-connection-for-thread sc))
    (value-cache-commit *txn-value-cache*)
    (postmodern:commit-transaction transaction)))

(defmethod controller-abort-transaction ((sc postmodern-store-controller) transaction &key &allow-other-keys)
  (with-postmodern-conn ((controller-connection-for-thread sc))
    (postmodern:abort-transaction transaction)))
