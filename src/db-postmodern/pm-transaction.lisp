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

(defmethod execute-transaction ((sc postmodern-store-controller) txn-fn &key (always-rollback nil) &allow-other-keys)
  ;; SQL doesn't support nested transaction
  (with-conn ((controller-connection-for-thread sc))
    (if (> (tran-count-of sc) 0)
        (funcall txn-fn)
        (progn
          (incf (tran-count-of sc))
          (unwind-protect
               (if always-rollback
                   (let (tran)
                     (unwind-protect
                          (progn
                            (setf tran (controller-start-transaction sc))
                            (funcall txn-fn))
                       (controller-abort-transaction sc tran)))
                   (with-conn ((controller-connection-for-thread sc))
                     (postmodern:with-transaction ()
                       (funcall txn-fn))))
            (decf (tran-count-of sc)))))))

(defmethod controller-start-transaction ((sc postmodern-store-controller) &key &allow-other-keys)
  (with-conn ((controller-connection-for-thread sc))
    (let ((transaction (make-instance 'postmodern::transaction-handle)))
      (postmodern:execute "BEGIN")
      transaction)))

(defmethod controller-commit-transaction ((sc postmodern-store-controller) transaction &key &allow-other-keys)
  (with-conn ((controller-connection-for-thread sc))  
    (postmodern:commit-transaction transaction)))

(defmethod controller-abort-transaction ((sc postmodern-store-controller) transaction &key &allow-other-keys)
  (with-conn ((controller-connection-for-thread sc))
    (postmodern:abort-transaction transaction)))
