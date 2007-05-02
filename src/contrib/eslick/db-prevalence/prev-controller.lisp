;;; -*- Mode: Lisp; Syntax: ANSI-Common-Lisp; Base: 10 -*-
;;;
;;; prev-controller.lisp -- Prevalence data store controller
;;; 
;;; Initial version 5/1/2007 by Ian Eslick
;;; 
;;; part of
;;;
;;; Elephant: an object-oriented database for Common Lisp
;;;
;;; Copyright (c) 2007 by Ian Eslick
;;; <ieslick common-lisp net>
;;;
;;; Elephant users are granted the rights to distribute and use this software
;;; as governed by the terms of the Lisp Lesser GNU Public License
;;; (http://opensource.franz.com/preamble.html), also known as the LLGPL.
;;;

;; TODO1:
;; - snapshot/restore
;; - transactions
;; - store slot values with objects in snapshot?
;; - overload serializer to reconstruct persistent metaclass objects
;; - universal comparison fn for keys & values

;; TODO2:
;; - Rotate snapshot files (see cl-prev's approach)
;; - Implement backup command
;; - snapshot based on auto dirty-p?
;; - auto reclaim oid on restore except roots?

(in-package :db-prevalence)

(defclass prev-store-controller (store-controller)
  ((rootdir :accessor prevalence-root-dir)
   (transaction-log :accessor transaction-log 
		    :documentation "The pathname to the txn log")
   (transaction-log-stream :accessor transaction-log-stream
			   :documentation "Transaction stream")
   (serialization-state :accessor serialization-state
			:documentation "Reference unification"
			:initform (make-serialization-state))
   (slots :accessor controller-slots)
   (last-oid :accessor last-oid
	     :documentation "OID state"
	     :initform 0)
   (transaction-lock :accessor transaction-lock
		     :initform (ele-make-fast-lock)
		     :documentation "Mediates starting & stopping transactions so one can abort another"))
  (:documentation "Controller for the Elephant prevalence system"))

(defmethod snapshot-file ((sc prev-store-controller))
  (assert (prevalence-root-dir sc))
  (merge-pathnames "snapshot.xml" (prevalence-root-dir sc)))

;;
;; Controller utilities
;;

(defun initialize-roots (sc)
  (setf (slot-value sc 'root)
	(make-instance 'prev-btree :from-oid -1 :sc sc))
  (setf (slot-value sc 'class-root)
	(make-instance 'prev-btree :from-oid -2 :sc sc)))
  
(defun close-open-streams (sc)
  (close (transaction-log-stream sc)))

;;
;; Registry
;;

(defun prev-test-and-construct (spec)
  (if (prev-store-spec-p spec)
      (make-instance 'prev-store-controller :spec spec)
      (error (format nil "Unrecognized database specifier: ~A" spec))))

(eval-when (:compile-toplevel :load-toplevel)
  (register-data-store-con-init :prevalence 'prev-test-and-construct))

(defun prev-store-spec-p (spec)
  (and (eq (first spec) :prevalence)
       (typecase (second spec)
	 (pathname t)
	 (string t)
	 (otherwise nil))))

;;
;; Open and Close
;;

(defmethod open-controller ((sc prev-store-controller) &key (recover nil) (recover-fatal nil) (thread t))
  (declare (ignore recover recover-fatal thread))
  (setf (prevalence-root-dir sc) (second (controller-spec sc)))
  (unless (probe-file (prevalence-root-dir sc) :follow-symlinks t)
    (error "Directory ~A does not exist" (prevalence-root-dir sc)))
  (setf (transaction-log sc) (merge-pathnames "txn.log" (prevalence-root-dir sc)))
  (if (probe-file (snapshot-file sc))
      (controller-restore sc)
      (initialize-roots sc))
  (setf (controller-slots sc) (build-controller-slots (next-oid sc))))

(defmethod transaction-log-stream :before ((sc prev-store-controller))
  (setf (transaction-log-stream sc)
	(open (transaction-log sc)
	      :direction :output
	      :if-does-not-exist :create
	      :if-exists :append)))


(defmethod close-controller ((sc prev-store-controller))
  (when (transaction-log-stream sc)
    (close (transaction-log-stream sc))))

;;
;; DB Version
;;

(defun set-database-version (sc)
  (with-open-store (stream (version-file sc)
			   :direction :output
			   :if-does-not-exist :create
			   :if-exists :overwrite)
    (write stream *elephant-code-version*)))

(defmethod database-version ((sc prev-store-controller))
  (when (probe-file (version-file sc))
    (with-open-file (stream (version-file sc) :direction :input)
      (read stream))))

(defun version-file (sc)
  (merge-pathnames "version.sexp" (prevalence-root-dir sc)))

;;
;; UIDs
;;

(defmethod next-oid ((sc prev-store-controller))
  (incf (last-oid sc)))
;;  (do-transaction :oid (incf oid)))

;;
;; Snapshots
;;

(defmethod controller-snapshot ((sc prev-store-controller))
  "Take a snapshot of the system state; write everything to the file"
  (let ((timetag (timetag))
	(txn-log (transaction-log sc)))
    (close-open-streams sc)
    (with-open-file (out (snapshot-file sc) :direction :output 
			 :if-does-not-exist :create
			 :if-exists :supersede)
      (serialize-xml out 
		     (list (controller-root sc)
			   (controller-class-root sc)
			   (last-oid sc))
		     (serialization-state sc))
      (delete-file txn-log))))
      
    
(defmethod controller-restore ((sc prev-store-controller))
  (close-open-streams sc)
  (with-open-file (in (snapshot-file sc) :direct :input)
    (destructuring-bind (root class-root oid)
	(deserialize-xml in (serialization-state sc))
      (setf (slot-value sc 'root) root)
      (setf (slot-value sc 'class-root) class-root)
      (setf (last-oid sc) oid)))
  (when (probe-file (transaction-log sc))
    (let ((position 0))
      (handler-bind ((s-xml:xml-parser-error
		      #'(lambda (condition)
			  (format *standard-output*
				  ";; Warning: error during transaction log restore: ~s~%"
				  condition)
			  (truncate-file (transaction-log sc) position)
			  (return-from controller-restore))))
	(with-open-file (in (transaction-log system) :direction :input)
	  (loop
	     (let ((transaction (deserialize-xml in (serialization-state sc))))
	       (setf position (file-position in))
	       (if transaction
		   (replay-transaction transaction)
		   (return)))))))))

;;
;; File utilities
;;
	  
(defun copy-file (source target)
  "From cl-prevalence; Copyright Sven Van Caekenberghe; LLGPL"
  (let ((buffer (make-string 4096))
	(read-count 0))
    (with-open-file (in source :direction :input)
      (with-open-file (out target :direction :output :if-exists :overwrite :if-does-not-exist :create)
	(loop
	 (setf read-count (read-sequence buffer in))
	 (write-sequence buffer out :end read-count)
	 (when (< read-count 4096) (return)))))))

(defun truncate-file (file position)
  "Truncate the physical file at position by copying and replacing it
  From cl-prevalence; Copyright Sven Van Caekenberghe; LLGPL"
  (let ((tmp-file (merge-pathnames (concatenate 'string "tmp-" (pathname-name file)) file))
	(buffer (make-string 4096))
	(index 0)
	(read-count 0))
    (with-open-file (in file :direction :input)
      (with-open-file (out tmp-file :direction :output :if-exists :overwrite :if-does-not-exist :create)
	(when (> position (file-length in)) (return-from truncate-file))
	(loop
	 (when (= index position) (return))
	 (setf read-count (read-sequence buffer in))
	 (when (>= (+ index read-count) position)
	   (setf read-count (- position index)))
	 (incf index read-count)
	 (write-sequence buffer out :end read-count))))
    (delete-file file)
    (rename-file tmp-file file))
  (format t ";; Notice: truncated transaction log at position ~d~%" position))
