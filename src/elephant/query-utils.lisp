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
	:initform (make-array 1000 :adjustable t :fill-pointer 0))))

(defmethod print-object ((set simple-set) stream)
  (format stream "#<SIMPLE-SET:~A ~A>" 
	  (length (set-sequence set)) (set-recs set)))

(defun dump-simple-set (set)
  (map-simple-set #'print set)
  set)

(defun make-simple-set (recs)
  (make-instance 'simple-set :recs recs))

(defun add-row (set values)
  (assert (valid-row set values))
  (vector-push-extend values (set-sequence set)))

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

(defun intersect-sets (output-recs set1 vrec1 set2 vrec2 &optional (dups :none))
  "The simple is when we are intersecting on two columns w/o duplicate elements.  
   If we have duplicates, we want to "
  (let* ((seq1 (set-sequence set1))
	 (v1 (position vrec1 (set-recs set1) :test #'equal))
	 (seq2 (set-sequence set2))
	 (v2 (position vrec2 (set-recs set2) :test #'equal))
	 (i 0) (j 0)
	 (imax (length (set-sequence set1)))
	 (jmax (length (set-sequence set2)))
	 (out (make-array (min imax jmax) :fill-pointer 0))
	 (output-map (output-map output-recs (set-recs set1) (set-recs set2))))
    (declare (fixnum i j imax jmax v1 v2))
;;	     ((array *) seq1 seq2 out))
    (unless v1
      (error "Dependency ~A not found in ~A with spec ~A"
	     vrec1 set1 (set-recs set1)))
    (unless v2
      (error "Dependency ~A not found in ~A with spec ~A"
	     vrec2 set2 (set-recs set2)))
    (loop
       (when (or (= i imax) (= j jmax))
	 (return (make-instance 'simple-set :recs output-recs :seq out)))
       (let ((row1 (aref seq1 i))
	     (row2 (aref seq2 j)))
	 (let ((velt1 (nth v1 row1))
	       (velt2 (nth v2 row2)))
	   (cond ((lisp-compare-equal velt1 velt2)
		  (vector-push-extend 
		   (merge-set-rows output-map row1 row2) out)
		  (ecase dups
		    (:none (incf i) (incf j))
		    (:right (incf j))
		    (:left (incf i))
		    (:both (error "Not sure what to do here yet"))))
		 ((lisp-compare< velt1 velt2)
		  (incf i))
		 (t (incf j))))))))

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

