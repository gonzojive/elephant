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
;; - Debug rest of tests
;; - Duplicate ordering on primary key 
;; - Universal comparison fn for keys & values
;; - Compress OID and do reachability on snapshot

;; TODO2:
;; - Performance
;;   - log(n) 'set' commands (need full k-ary tree implementation?)
;;   - also reduces hash storage and access overhead
;; - Snapshots
;;   - revert to prior snapshot and preserve txn.log when snapshot fails
;;   - snapshot based on auto dirty-p?
;;   - auto mark and sweep on snapshot?
;;   - auto snapshot in separate thread?
;;   - versioning: move db backward in time (to last snapshot, back N txns, etc?)
;;     or save snapshot versions
;;   - Implement backup command
;;   - Rotate snapshot files (see cl-prev's approach)
;; - Concurrent transactions (store side effects, cancel conflicting transactions on commit)
;;   - Implement retry in execute-transaction
;; - Txn costs
;;   - transaction op pools to avoid allocating caches?
;;   - or txn structs for cheaper allocation costs?
;;   - rotate txn logs ala BDB?

(in-package :db-prevalence)

(defclass prev-store-controller (store-controller)
  ((rootdir :accessor prevalence-root-dir)
   (transaction-log :accessor transaction-log 
		    :documentation "The pathname to the txn log")
   (transaction-log-stream :accessor transaction-log-stream :initform nil
			   :documentation "Transaction stream")
   (transaction-lock :accessor transaction-lock
		     :initform (ele-make-lock)
		     :documentation "Provides for transaction isolation in non-concurrent model")
   (object-records :accessor controller-object-records :initform nil)
   (last-oid :accessor last-oid
	     :documentation "OID state"
	     :initform 0))
  (:documentation "Controller for the Elephant prevalence system"))

(defmethod snapshot-file ((sc prev-store-controller))
  (assert (prevalence-root-dir sc))
  (merge-pathnames "snapshot.db" (prevalence-root-dir sc)))

(defmethod flush-instance-cache ((sc prev-store-controller))
  "Override the default flush behavior as we assume our cache 
   is never flushed!"
  nil)

;;
;; Controller utilities
;;

(defun initialize-roots (sc)
  (setf (slot-value sc 'root)
	(make-instance 'prev-btree :sc sc))
  (setf (slot-value sc 'class-root)
	(make-instance 'prev-indexed-btree :sc sc)))

(defun close-open-streams (sc)
  (when (slot-boundp sc 'transaction-log-stream)
    (when (slot-value sc 'transaction-log-stream) 
      (close (transaction-log-stream sc))))
  (setf (transaction-log-stream sc) nil))

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
  (setf (transaction-log sc) 
	(merge-pathnames "txn.log" (prevalence-root-dir sc)))
  (initialize-serializer sc)
  (setf (controller-object-records sc) 
	(build-controller-object-records (last-oid sc)))
  (let ((snapshot? (probe-file (snapshot-file sc)))
	(txn-log? (probe-file (transaction-log sc))))
    (when snapshot?
      (controller-restore sc)
      (when txn-log?
	(replay-transaction-log sc)))
    (when (not snapshot?)
      (set-database-version sc)
      (initialize-roots sc)
      (controller-snapshot sc))))

(defmethod initialize-serializer ((sc prev-store-controller))
	 (setf (controller-serializer-version sc) 2)
	 (setf (controller-serialize sc) 
	       (intern "SERIALIZE" (find-package :ELEPHANT-SERIALIZER2)))
	 (setf (controller-deserialize sc) 
	       (intern "DESERIALIZE" (find-package :ELEPHANT-SERIALIZER2))))

(defmethod transaction-log-stream :before ((sc prev-store-controller))
  (unless (slot-value sc 'transaction-log-stream)
    (setf (transaction-log-stream sc)
	  (open (transaction-log sc)
		:direction :io
		:if-does-not-exist :create
		:if-exists :append))))

(defmethod close-controller ((sc prev-store-controller))
  (close-open-streams sc)
  (setf (last-oid sc) 0)
  (setf (transaction-lock sc) nil))

;;
;; DB Version
;;

(defun set-database-version (sc)
  (with-open-file (stream (version-file sc)
			   :direction :output
			   :if-does-not-exist :create
			   :if-exists :overwrite)
    (write *elephant-code-version* :stream stream)))

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
  (maybe-extend-records (+ 10 (last-oid sc)) sc)
  (incf (last-oid sc)))

;;
;; Objects and slots
;;

(defun build-controller-object-records (hint)
  (make-array (max (floor (* 1.5 hint)) 1000) :initial-element nil :adjustable t))

(defun maybe-extend-records (oid sc)
  (let ((records (controller-object-records sc)))
    (when (>= oid (length records))
      (setf (controller-object-records sc) 
	    (adjust-array records (ceiling (* (length records) 1.5)))))))

(defun make-object-record (classname slots)
  (cons classname slots))

(defun get-object-record (oid sc)
  (aif (aref (controller-object-records sc) oid) 
       it
       (setf (aref (controller-object-records sc) oid)
	     (cons nil nil))))

(defsetf set-object-record (oid sc) (record)
  `(progn
     (setf (aref (controller-object-records ,sc) ,oid) ,record)))

(defun object-record-classname (rec)
  (car rec))

(defsetf object-record-classname (rec) (classname)
  `(setf (car ,rec) ,classname))

(defun object-record-slots (rec)
  (cdr rec))

(defsetf object-record-slots (rec) (slots)
  `(setf (cdr ,rec) ,slots))

(defmethod cache-instance ((sc prev-store-controller) instance)
  (call-next-method)
  (setf (object-record-classname (get-object-record (oid instance) sc))
	(class-name (class-of instance))))

;; higher level accessors

(defun get-object-slots (oid sc)
  (awhen (get-object-record oid sc)
    (object-record-slots it)))

(defun get-object-classname (oid sc)
  (awhen (get-object-record oid sc)
    (object-record-classname it)))

(defun get-object-instance (oid sc)
  (awhen (get-object-classname oid sc)
    (get-cached-instance sc oid it)))


;;
;; Restore objects without recording new transactions
;;

(defparameter *inhibit-transaction-logging* nil)

(defmacro unless-inhibited (&body body)
  `(unless *inhibit-transaction-logging*
     (progn ,@body)))

(defmacro with-inhibited-transactions (&body body)
  `(let ((*inhibit-transaction-logging* t))
     (declare (special *inhibit-transaction-logging*))
     ,@body))

;;
;; Controller transaction recover
;;

(defun replay-transaction-log (sc)
  (ele-with-lock ((transaction-lock sc))
    (close-open-streams sc)
    (let ((transaction (make-instance 'transaction :store sc :log nil)))
      (handler-case
	  (with-open-file (stream (transaction-log sc) 
				  :direction :input
				  :if-does-not-exist :error)
	    (loop for log = (deserialize-from-stream stream sc)
	       while log
	       do (progn (setf (prev-transaction-ops transaction) log)
			 (if (committed-transaction-p transaction)
			     (replay-ops transaction)
			     (return t)))))
	(elephant-deserialization-error () t)
	(end-of-file () t)))))

;;
;; Snapshots
;;

(defmethod controller-snapshot ((sc prev-store-controller))
   "Take a snapshot of the system state; write everything to the file"
   (ele-with-lock ((transaction-lock sc))
     (close-open-streams sc)
     (with-open-file (out (snapshot-file sc) :direction :output 
 			 :if-does-not-exist :create
 			 :if-exists :supersede)
       (serialize-to-stream (last-oid sc) out sc)
       (snapshot-objects (last-oid sc) out sc))
     (delete-file (transaction-log sc))))

(defun snapshot-objects (count stream sc)
  (loop for oid from 1 upto count do
       (let ((record (get-object-record oid sc)))
	 (serialize-to-stream oid stream sc)
	 (serialize-to-stream record stream sc))))
   
(defmethod controller-restore ((sc prev-store-controller))
  "Recover the state of the system from a snapshot"
  (close-open-streams sc)
  (with-open-file (in (snapshot-file sc) :direction :input
		      :if-does-not-exist :error)
    (let ((count (deserialize-from-stream in sc)))
      (setf (last-oid sc) count)
      (restore-object-records count in sc)
      (restore-object-instances count sc)
      (set-cached-roots sc))))

(defun restore-object-records (count stream sc)
  (maybe-extend-records count sc)
  (with-inhibited-transactions 
    (loop for i from 1 upto count do
	 (restore-object stream sc))))

(defun restore-object (stream sc)
  (let ((oid (deserialize-from-stream stream sc))
	(record (deserialize-from-stream stream sc)))
    (setf (aref (controller-object-records sc) oid) record)))

(defun restore-object-instances (count sc)
  (with-inhibited-transactions
    (loop 
       for oid from 1 to count do
	 (make-instance (get-object-classname oid sc) :from-oid oid :sc sc))))

(defun set-cached-roots (sc)
  (setf (slot-value sc 'root)
	(get-cached-instance sc 1 'prev-btree))
  (setf (slot-value sc 'class-root)
	(get-cached-instance sc 2 'prev-indexed-btree)))

;; =================================================
;; Support Code
;; =================================================

;;
;; Serialization/deserialization to file via memutils
;;

(defun serialize-to-stream (object stream sc)
  (with-buffer-streams (out)
    (serialize object out sc)
    (write-count stream (buffer-stream-size out))
    (write-buffer-stream out stream)
    stream))

(defun deserialize-from-stream (stream sc)
  (with-buffer-streams (in)
    (let ((count (read-count stream)))
      (read-buffer-stream in stream count)
      (deserialize in sc))))

(defun write-buffer-stream (bs stream)
  (loop for i from 1 upto (buffer-stream-size bs)
        for byte = (buffer-read-byte bs)
        do (write-byte byte stream)))

(defun read-buffer-stream (bs stream count)
  (loop for i from 1 upto count
        for byte = (read-byte stream :eof-error-p t)
        do (buffer-write-byte byte bs)))

(defun write-count (stream count)
  (loop for i from 0 below 3
        for byte = (ldb (byte 8 (* i 8)) count)
        do (write-byte byte stream)))

(defun read-count (stream)
  (loop with value = 0
     for i from 0 below 3
     for byte = (read-byte stream)
     do (setf value (dpb byte (byte 8 (* i 8)) value))
     finally (return value)))

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

;;
;; Simple DB test
;;

(defun simple-prevalence-test ()
;;  (open-store '(:PREVALENCE "/Users/eslick/Work/db/prev1/"))
  (add-to-root 'indexed (make-indexed-btree))
  (let* ((indexed (get-from-root 'indexed))
	 (index (add-index indexed :index-name 'length 
			   :key-form '(lambda (i k v)
				       (values t
					(length (symbol-name v)))))))
    (declare (ignorable index))
    (add-to-root 'pfoo100 (make-instance 'ele-tests::pfoo :slot1 100))
    (add-to-root 'pfoo200 (make-instance 'ele-tests::pfoo :slot1 200))
    (setf (get-value 1 indexed) 'one)
    (setf (get-value 4 indexed) 'four)
    (setf (get-value 5 indexed) 'five)
    (setf (get-value 6 indexed) 'six)
    (setf (get-value 3 indexed) 'three)
    (setf (get-value 7 indexed) 'seven)))
