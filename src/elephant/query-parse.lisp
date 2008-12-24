;;; -*- Mode: Lisp; Syntax: ANSI-Common-Lisp; Base: 10 -*-
;;;
;;; query-parse.lisp -- Parse expressions to constraints
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

;;; Turn constraint expressions into a grouped constraint table

(in-package :elephant)


;;; ===========================================
;;;  Extract constraints and bindings
;;; ===========================================

(defun extract-query-constraints (bindings constraint-expr)
  (mapcar #'optimize-constraints 
	  (group-constraints 
	   (parse-and-flatten-constraints constraint-expr)
	   bindings)))


;;; ===========================================
;;;  Constraint definitions 
;;; ===========================================

(defclass constraint ()
  ((expr :accessor constraint-expression :initarg :expr :initform nil)))

(defmethod instance-satisfies (instance (constraint constraint))
  "Default method gives us a nice identity constraint as a stub; also
   a constraint with no requirements always satisfies"
  t)

(defmethod value-satisfies (value (constraint constraint))
  t)

(defclass slot-constraint (constraint)
  ((var :accessor constraint-var :initarg :var :initform nil)
   (class :accessor constraint-class :initarg :class :initform nil)
   (pred-fn :accessor constraint-predicate :initarg :predicate-fn)
   (slot :accessor constraint-slot :initarg :slot)
   (const :accessor constraint-constant :initarg :constant)))

(defmethod instance-satisfies (instance (constraint slot-constraint))
  (with-slots (pred-fn slot const) constraint
    (funcall pred-fn (slot-value instance slot) const)))

(defmethod value-satisfies (value (constraint slot-constraint))
  (with-slots (pred-fn const) constraint
    (funcall pred-fn value const)))

(defclass slot-range-constraint (slot-constraint)
  ((pred-fn2 :accessor constraint-predicate2 :initarg :predicate-fn2)
   (const2 :accessor constraint-constant2 :initarg :constant2)))

(defmethod instance-satisfies (instance (constraint slot-range-constraint))
  (with-slots (slot pred-fn2 const2) constraint
    (and (call-next-method)
	 (funcall pred-fn2 (slot-value instance slot) const2))))

(defmethod value-satisfies (value (constraint slot-constraint))
  (with-slots (pred-fn2 const2) constraint
    (and (call-next-method)
	 (funcall pred-fn2 value const2))))

(defclass relation-constraint (constraint)
  ((pred-fn :accessor constraint-predicate :initarg :predicate-fn)
   (left-var :accessor constraint-left-var :initarg :left-var)
   (left-slot :accessor constraint-left-slot :initarg :left-slot)
   (right-var :accessor constraint-right-var :initarg :right-var)
   (right-slot :accessor constraint-right-slot :initarg :right-slot)))

(defclass or-constraint (constraint)
  ((left :accessor left-constraints :initarg :left)
   (right :accessor right-constraint :initarg :right)))

(defun make-or-constraint (expr left right)
  (make-instance 'or-constraint :expr expr :left left :right right))

(defmethod instance-satisfies (instance (constraints cons))
  "Ensures that an instance satisfies a set of instance constraints"
  (every (lambda (c)
	   (instance-satisfies instance c))
	 constraints))


;;; ===========================================
;;;  Constraint parsing and canonicalization
;;; ===========================================

;;; Flatten

(defun parse-and-flatten-constraints (constraint-exprs)
  "Roughly speaking; turn these into modified horne clauses"
  (mapcan #'%pf-constraint constraint-exprs))

(defun %pf-constraint (expr)
  (destructuring-bind (pred arg1 arg2) expr
    (cond ((eq pred 'and)
	   (mapcan '%pf-constraint (cdr expr)))
	  ((eq pred 'or) 
	   (list (make-or-constraint expr
		   (%pf-constraint arg1)
		   (%pf-constraint arg2))))
	  (t (list (make-constraint expr))))))

;;; Build constraint objects

(defun make-constraint (expr)
  "Construct a constraint object from an expression"
  (multiple-value-bind (pred-fn ctype var1 arg1 var2 arg2)
      (parse-constraint-expr expr)
    (case ctype
      (slot (make-instance 'slot-constraint
			   :expr expr
			   :predicate-fn pred-fn
			   :var var1
			   :slot arg1
			   :constant arg2))
      (relation (make-instance 'relation-constraint
			       :expr expr
			       :predicate-fn pred-fn
			       :left-var var1
			       :left-slot arg1
			       :right-var var2
			       :right-slot arg2)))))
			       
	   

(defun parse-constraint-expr (expr)
  "Extract enough information from the constraint object to
   enable the constraint objects to be built"
  (destructuring-bind (pred arg1 arg2) expr
    (let* ((pred-fn (predicate-function pred expr))
	   (a1desc (argument-description arg1))
	   (a2desc (argument-description arg2))
	   (ctype (constraint-type-from-argdesc a1desc a2desc expr)))
      ;; Invert the predicate if argument order is reversed
      (when (and (eq ctype 'slot)
		 (not (slot-argument a1desc)))
	(setf pred-fn (invert-predicate pred expr))
	(let ((temp a1desc))
	  (setf a1desc a2desc)
	  (setf a2desc temp)))
      (values pred-fn  ;; predicate
	      ctype    ;; constraint type
	      (argument-class-var a1desc) ;; class var
	      (argument-slotname a1desc)  ;; slotname
	      (when (slot-argument a2desc)   ;; class var or nil
		(argument-class-var a2desc))
	      (if (slot-argument a2desc)     ;; slotname or constant
		  (argument-slotname a2desc)
		  (argument-constant a2desc))))))
      

(defun constraint-type-from-argdesc (ad1 ad2 expr)
  (cond ((and (slot-argument ad1)
	      (slot-argument ad2))
	 'relation)
	((or (slot-argument ad1)
	     (slot-argument ad2))
	 'slot)
	(t (error "Constraint ~A is nonsensical" expr))))

(defun argument-description (arg)
  (cond ((symbolp arg)
	 (if (constraint-var-p arg)
	     `(slot nil ,arg)
	     `(const ,(type-of arg) ,arg)))
	((atom arg)
	 `(const ,(type-of arg) ,arg))
	(t `(slot ,(first arg) ,(second arg)))))

(defun constraint-var-p (var)
  (eq (char (symbol-name var) 0) #\?))

;; argument descriptions

(defun argument-type (desc)
  (first desc))

;; constant argument type

(defun const-argument (desc)
  (eq (argument-type desc) 'const))

(defun argument-datatype (desc)
  (assert (const-argument desc))
  (second desc))

(defun argument-constant (desc)
  (assert (const-argument desc))
  (third desc))

;; slot argument type

(defun slot-argument (desc)
  (eq (argument-type desc) 'slot))

(defun argument-slotname (desc)
  (assert (slot-argument desc))
  (second desc))

(defun argument-class-var (desc)
  (assert (slot-argument desc))
  (third desc))

;;; Coalesce by group

(defun group-constraints (constraints bindings)
  "Group constraints into appropriate classes according to
   the var to class mappings in bindings ((var class) (var class))
   NOTE: How to handle disjunctions?"
  (let ((table nil))
    (labels ((assoc-push (v c)
	       (aif (assoc v table :test #'equal)
		    (push c (cdr it) )
		    (push (cons v (list c)) table)))
	     (group-constraint (c)
	       (assoc-push (constraint-deps c) c)))
      (mapc #'group-constraint constraints)
      (mapc (lambda (g) (assign-classes g bindings)) table)
      table)))

(defun constraint-deps (c)
  (if (subtypep (type-of c) 'relation-constraint)
      (cons (constraint-left-var c) (constraint-right-var c))
      (constraint-var c)))

(defun assign-classes (g bindings)
  "Set the class slot of the constraints based on bindings
   NOTE: support class constraint predicate as substitute"
  (let* ((var (car g))
	 (class (second (find var bindings :key #'car)))
	 (constraints (cdr g)))
    (unless (consp var)
      (dolist (c constraints)
	(setf (constraint-class c) class)))))

;; ================================================
;;  Optimize and compact constraints if we can
;; ================================================

(defun optimize-constraints (group)
  "For now, just identify ranges (collapse conjunctions?)"
  group)
  
