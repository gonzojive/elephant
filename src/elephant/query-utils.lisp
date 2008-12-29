;;; -*- Mode: Lisp; Syntax: ANSI-Common-Lisp; Base: 10 -*-
;;;
;;; query-utils.lisp -- sets, predicates, etc.
;;; 
;;; Copyright (c) 2009 Ian Eslick
;;; <ieslick common-lisp net>
;;;
;;; part of
;;;
;;; Elephant: an object-oriented database for Common Lisp
;;;
;;; Elephant users are granted the rights to distribute and use this software 
;;; as governed by the terms of the Lisp Lesser GNU Public License
;;; (http://opensource.franz.com/preamble.html), also known as the LLGPL.
;;;

(in-package :elephant)

;;; =================================
;;;  Simple query sets
;;; =================================

(defclass simple-set ()
  ((recs :initarg :recs :accessor set-recs :initform nil)
   (seq :initarg :seq :accessor set-sequence
	:initform (make-array 1000 :adjustable nil :fill-pointer 0))))

(defmethod print-object ((set simple-set) stream)
  (format stream "#<SIMPLE-SET:~A ~A>" 
	  (length (set-sequence set)) (set-recs set)))

(defun dump-simple-set (set)
  (map-simple-set #'print set)
  set)

(defun make-simple-set (recs)
  (make-instance 'simple-set :recs recs))

(defparameter *min-simple-set-extension* 1000)

(defun add-row (set values)
  (assert (valid-row set values))
  (vector-push-extend values (set-sequence set) *min-simple-set-extension*))

(defun valid-row (set values)
  (every #'(lambda (i rec)
	     (if (atom rec)
		 (or (integerp i)
		     (subtypep (type-of i) rec))
		 t))
	 values
	 (set-recs set)))

(defun sort-set (set rec &optional (pred #'lisp-compare<))
  (let ((offset (position rec (set-recs set) :test #'equal)))
    (assert offset)
    (setf (set-sequence set)
	  (sort (set-sequence set) pred 
		:key (lambda (values)
		       (nth offset values))))))

(defun intersect-sets (output-recs set1 vrec1 set2 vrec2)
  "The simple is when we are intersecting on two columns.  We back up the
   join pointer when we have dual sets of duplicate elements."
  (let* ((seq1 (set-sequence set1))
	 (v1 (position vrec1 (set-recs set1) :test #'equal))
	 (seq2 (set-sequence set2))
	 (v2 (position vrec2 (set-recs set2) :test #'equal))
	 (imax (length (set-sequence set1)))
	 (jmax (length (set-sequence set2)))
	 (out (make-array (min imax jmax) :fill-pointer 0))
	 (output-map (output-map output-recs (set-recs set1) (set-recs set2)))
	 (left (nth v1 (aref seq1 0)))
	 (right (nth v2 (aref seq2 0)))
	 (next-left nil)
	 (next-right nil)
	 (i 1) (j 1) (bp 0))
    (declare (fixnum i j imax jmax v1 v2 bp)
	     (list left right next-left next-right))
;;	     ((array t *) seq1 seq2 out))
    (unless v1
      (error "Dependency ~A not found in ~A with spec ~A"
	     vrec1 set1 (set-recs set1)))
    (unless v2
      (error "Dependency ~A not found in ~A with spec ~A"
	     vrec2 set2 (set-recs set2)))
    (labels ((maybe-return ()
	       (when (and (= imax i) (= jmax j))
		 (return-from intersect-sets
		   (make-instance 'simple-set :recs output-recs :seq out))))
	     (increment-left ()
	       (setq right right-next)
	       (unless (= imax i)
		 (incf i))
	       (setq right-next (nth v1 (aref seq1 i))))
	     (increment-right ()
	       (setq left left-next)
	       (unless (= jmax j)
		 (incf j))
	       (setq left-next (nth v2 (aref seq2 j))))
	     (track-left-dups ()
	       (if (lisp-compare-equal left next-left)
		   (when (not dups-left)
		     (setf dups-left t))
		   (when dups-left
		     (setf dups-left nil))))
	     (track-right-dups ()
	       (unless dups-right
		 (when (lisp-compare-equal right next-right)
		   (setf dups-right t)
		   (setf bp (1- j)))))
	     (step-on-equal ()
	       (cond ((and (not dups-right) (not dups-left))
		      (increment-right))
		     (dups-right 
		      (increment-right))
		     ((and (not dups-right) dups-left)
		      (increment-left))))
	     (step-on-right-greater ()
	       (cond (dups-left
		      (increment-right))
		     ((and dups-right (not dups-left))
		      (increment-right))
		     ((and (not dups-right) dups-left)
		      (increment-left))
		     ((and dups-right dups-left)
	     (step-on-left-greater ()
	       (cond ((and (not dups-right) (not dups-left))
		      (increment-right))
		     ((and dups-right (not dups-left))
		      (increment-right))
		     ((and (not dups-right) dups-left)
		      (increment-left))
		     ((and dups-right dups-left)
	       
      (loop
	 (when (or (= i imax) (= j jmax))
	 (track-right-dups)
	 (track-left-dups)
	 ;; NOTE: Optimize away comparisons when dealing with dups?
	 (cond ((lisp-compare-equal left right)
		(vector-push
		 (merge-set-rows output-map row1 row2) out)
		(step-on-equal))
	       ((lisp-compare< left right)
		(step-on-right-greater))
	       (t (step-on-left-greater))))

(defun filter-set (fn set rec)
  (let ((offset (position rec (set-recs set) :test #'equal))
	(out (make-array (length (set-sequence set)) :fill-pointer 0)))
    (loop for entry across (set-sequence set) do
	 (when (funcall fn (nth offset entry))
	   (vector-push-extend entry out)))
    (make-instance 'simple-set :recs (set-recs set) :seq out)))

(defun map-simple-set (fn set)
  (loop for entry across (set-sequence set) do
       (funcall fn entry)))

(defun merge-set-rows (map row1 row2)
  (loop for entry in map collect
       (if (car entry)
	   (nth (cdr entry) row2)
	   (nth (cdr entry) row1))))

(defun output-map (out-recs in-recs1 in-recs2)
  (loop for rec in out-recs collect
       (let ((pos1 (position rec in-recs1 :test #'equal))
	     (pos2 (position rec in-recs2 :test #'equal)))
	 (cond (pos1 (cons nil pos1))
	       (pos2 (cons t pos2))
	       (t (error "Cannot find output rec ~A in input recs~%~A, ~A"
			 rec in-recs1 in-recs2))))))

(defun make-simple-recs (class slots)
  (cons class (mapcar (lambda (s) (list s class)) slots)))

(defun get-simple-values (instance slots)
  (if slots
      (cons (oid instance) 
	    (mapcar (lambda (s) 
		      (persistent-slot-reader *store-controller*
					      instance s t))
		    slots))
      (cons (oid instance) nil)))

(defmacro with-simple-set ((constructor recs) &body body)
  "Optimization (support insertion of index value w/o slot access"
  (with-gensyms (r iset)
    `(let* ((,r ,recs)
	    (,iset (make-simple-set ,r)))
       (labels ((,constructor (values)
		  (add-row ,iset values)))
	 ,@body
	 ,iset))))

;;; =================================
;;;  Query sets 
;;; =================================

;; (defparameter *default-instance-set-size* 100)
;; (defparameter *growth-increment* 1000)
;; ;;; An instance set around an array w/ (instance/oid slot-val1 slot-val2 ...)

;; (defstruct instance-set class slots row-size array)

;; (defun make-iset (class slots)
;;   (make-instance-set
;;    :class class
;;    :slots slots
;;    :row-size (1+ (length slots))
;;    :array (make-array (* (1+ (length slots)) 
;; 			 *default-instance-set-size*) 
;; 		      :adjustable t :fill-pointer t)))
   
;; (defun add-row (set instance values)
;;   (let ((array (instance-set-array set)))
;;     (vector-push-extend instance array *growth-increment*)
;;     (dolist (value values)
;;       (vector-push-extend value array *growth-increment*))))

;; (defun get-row (set nth)
;;   (with-slots (row-size array) set
;;     (let ((offset (get-row-pointer set nth)))
;;       (loop for i from offset below (+ offset row-size) collect
;; 	   (aref array i)))))

;; (defun map-instance-set (fn set)
;;   (let ((array (instance-set-array set))
;; 	(rlen (instance-set-row-length set)))
;;     (loop for i from 0 below (/ (length array) rlen) do
;; 	 (let ((offset (* i rlen)))
;; 	   (apply fn 
;; 		  (loop for j from 1 below rlen collect
;; 		       (aref array (+ offset j))))))))

;; (defun get-row-pointer (set offset)
;;   (* offset (instance-set-row-size set)))

;; (defmacro with-instance-set ((constvar class slots) &rest body) 
;;   (with-gensyms (c s iset)
;;     `(let* ((,c ,class)
;; 	    (,s ,slots)
;; 	    (,iset (make-iset ,c ,s)))
;;        (labels ((,constvar (instance)
;; 		  (add-row ,iset instance (slot-values instance slots))))
;; 	 ,@body
;; 	 ,iset))))

;; (defmacro with-index-set ((constvar class slots index-slot) &body body)
;;   (with-gensyms (c s iset islot-p)
;;     `(let* ((,c ,class)
;; 	    (,s ,slots)
;; 	    (,islot-p ,index-slot)
;; 	    (,iset (make-instance-set ,c ,s)))
;;        (labels ((,constvar (instance value)
;; 		  (if ,islot-p
;; 		      (add-row ,iset instance (cons value (slot-values instance ,slots)))
;; 		      (add-row ,iset instance (slot-values instance ,slots)))))
;; 	 ,@body
;; 	 ,iset))))

;; (defun slot-values (instance slots)
;;   (flet ((curried (slot)
;; 	   (slot-value instance slot)))
;;     (mapcar #'curried slots)))


;;; ================================================
;;;  Predicate namespace
;;; ================================================

(defparameter *comparison-functions*
  `((< >= ,#'< :lt)
    (<= > ,#'<= :lt)
    (> <= ,#'> :gt)
    (>= < ,#'>= :gt)
    (= != ,#'= :eq)
    (!= = ,#'(lambda (x y) (not (= x y))) :neq)
    (string< string>= ,#'string< :lt)
    (string<= string> ,#'string<= :lt)
    (string> string<= ,#'string> :gt)
    (string>= string< ,#'string>= :gt)
    (string= string!= ,#'equal :eq)
    (string!= string= ,(lambda (x y) (not (equal x y))) :neq)
    (lt gte lisp-compare< :lt)
    (lte gt lisp-compare<= :lt)
    (gt lte ,#'(lambda (x y)
	       (not (lisp-compare<= x y))) :gt)
    (gte lt ,#'(lambda (x y)
		(not (lisp-compare< x y))) :gt)
    (eq neq ,#'eq :eq)
    (neq eq ,#'(lambda (x y) (not (eq x y))) :neq)
    (equal nequal lisp-compare-equal :eq)
    (nequal equal ,#'(lambda (x y)
		       (not (lisp-compare-equal x y))) :neq)
    (class nil ,#'(lambda (i cname)
		    (eq (type-of i) cname)) nil)
    (subclass nil ,#'(lambda (i cname)
		       (subtypep (type-of i) cname)) nil)))

(defun predicate-function (pred-name expr)
  (aif (assoc pred-name *comparison-functions*)
       (third it)
       (aif (symbol-function pred-name)
	    pred-name
	    (error "Unknown predicate function ~A in constraint expression ~A" 
		   pred-name expr))))

(defun invert-predicate (pred-name expr)
  (aif (find pred-name *comparison-functions* :key #'second)
       (predicate-function (second it) expr)
       (error "Cannot invert predicate name ~A from constraint ~A" pred-name expr)))

(defun predicate-range-type (pred)
  "Is this a less-than, greater-than, equality or non-equality value domain?"
  (awhen (find pred *comparison-functions* :key #'third)
    (fourth it)))
       

;;;
;;; Actual utilities
;;;


(defun pairs (list)
  "Take each set of two elements in the list and make a list one half
   the size containing a pair of each two items"
  (labels ((pairing (list accum num)
		    (cond ((null list)
			   (nreverse accum))
			  ((and (consp list) (null (cdr list)))
			   (nreverse (cons (list (car list) nil) accum)))
			  (t (pairing (cddr list) 
				      (cons (list (car list) (cadr list)) accum)
				      (- num 2))))))
    (pairing list nil (length list))))

