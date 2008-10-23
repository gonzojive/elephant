;;; -*- Mode: Lisp; Syntax: ANSI-Common-Lisp; Base: 10 -*-
;;;
;;; cached-slots.lisp -- cached persistent slot accesses
;;; 
;;; part of
;;;
;;; Elephant: an object-oriented database for Common Lisp
;;;
;;; Original Copyright (c) 2004 by Andrew Blumberg and Ben Lee
;;; <ablumberg@common-lisp.net> <blee@common-lisp.net>
;;;
;;; Portions Copyright (c) 2005-2007 by Robert Read and Ian Eslick
;;; <rread common-lisp net> <ieslick common-lisp net>
;;;
;;; Elephant users are granted the rights to distribute and use this software
;;; as governed by the terms of the Lisp Lesser GNU Public License
;;; (http://opensource.franz.com/preamble.html), also known as the LLGPL.
;;;

(in-package :elephant)

(defmethod slot-value-using-class
    ((class persistent-metaclass) (instance persistent-object) (slot-def cached-slot-definition))
  (if (= (slot-value instance 'cache-mode) +none-cached+)
      (persistent-slot-reader (get-con instance) instance (slot-definition-name slot-def))
      (call-next-method)))

(defmethod (setf slot-value-using-class)
    (new-value (class persistent-metaclass) (instance persistent-object) (slot-def cached-slot-definition))
  "Always write local slot value; maybe write persistent value if no caching or write-through"
  (let ((mode (slot-value instance 'cache-mode)))
    (if (or (= +none-cached+ mode)
	    (= +all-write-through+ mode))
	(persistent-slot-writer (get-con instance) new-value instance 
				(slot-definition-name slot-def))))
  (call-next-method))

;; Need to define indexed/cached slots in metaclass.lisp to enable this
#+nil
(defmethod (setf slot-value-using-class)
     (new-value (class persistent-metaclass) (instance persistent-object) (slot-def cached-indexed-slot-definition))
   (if (= (cache-mode instance) +index-write-through+)
       (persistent-slot-writer (get-con instance) new-value instance (slot-definition-name slot-def))
       (call-next-method)))

(defmethod slot-boundp-using-class 
    ((class persistent-metaclass) (instance persistent-object) (slot-def cached-slot-definition))
  "Checks if the slot exists in the database."
  (if (= +none-cached+ (slot-value instance 'cache-mode))
      (call-next-method)
      (persistent-slot-boundp (get-con instance) instance (slot-definition-name slot-def))))

(defmethod slot-makunbound-using-class 
    ((class persistent-metaclass) (instance persistent-object) (slot-def cached-slot-definition))
  "Removes the slot value from the database."
  (if (= +all-cached+ (slot-value instance 'cache-mode))
      (call-next-method)
      (persistent-slot-makunbound (get-con instance) instance (slot-definition-name slot-def))))

(defun refresh-cached-slots (instance slots)
  (let ((sc (get-con instance)))
    (dolist (slot slots)
      (when (persistent-slot-boundp sc instance slot)
	(setf (slot-value instance slot)
	      (persistent-slot-reader sc instance slot))))))

(defun flush-cached-slots (instance slots)
  (let ((class (class-of instance))
	(old-mode (slot-value instance 'cache-mode)))
    (unwind-protect 
	 (progn
	   (setf (slot-value instance 'cache-mode) +all-cached+)
	   (dolist (slot slots)
	     (let ((slot-def (find-slot-def-by-name class slot)))
	       (when (slot-boundp-using-class class instance slot-def)
		 (setf (slot-value-using-class class instance slot-def) 
		       (slot-value-using-class class instance slot-def))))))
      (setf (slot-value instance 'cache-mode) old-mode))))


;;
;; Cache mode and instance-level operations
;;

(defmethod refresh-slots ((object persistent-object))
  "Ensures that all memory slots are up-to-date with the repository,
   also creates read locks on all slots for transaction conflict detection"
  (refresh-cached-slots object (cached-slot-names (class-of object))))

(defmethod save-slots ((object persistent-object))
  "Ensures that all cached values are flushed to the store, issues write
   locks on the object for transaction conflict detection"
  (flush-cached-slots object (cached-slot-names (class-of object))))

(defmethod cache-mode ((object persistent-object))
  (let ((imode (slot-value object 'cache-mode)))
    (cond ((= imode +none-cached+)
	   :none-cached)
	  ((= imode +all-cached+)
	   :all-cached)
	  ((= imode +all-write-through+)
	   :write-through)
	  ((= imode +index-write-through+)
	   :index-write-through)
	  (t "Invalid cache mode - please write a valid mode"))))

(defmethod (setf cache-mode) (mode (object persistent-object))
  "Valid modes are: :all-cached, :none-cached, :write-through :index-write-through"
  (if (typep mode 'fixnum)
      (setf (slot-value object 'cache-mode) mode)
      (setf (slot-value object 'cache-mode)
	    (ecase mode
	      (:none-cached 
	       +none-cached+)
	      (:all-cached 
	       +all-cached+)
	      (:write-through 
	       +all-write-through+)
	      (:index-write-through 
	       +index-write-through+))))
  (if (eq (cache-mode object) +all-cached+)
      (refresh-slots object)
      (save-slots object))
  mode)

(defmacro with-cached-instances ((mode &rest instances) &body body)
  (with-gensyms (insts)
    `(let ((,insts ,instances)
	   (old-modes (mapcar #'(lambda (inst)
				  (prog1 
				      (slot-value inst 'cache-mode)
				    (setf (cache-mode inst) ,mode)))
			      ,insts)))
     (unwind-protect 
	  ,@body
       (mapcar #'(lambda (old-mode inst)
		   (setf (slot-value inst 'cache-mode) old-mode))
	       old-modes ,insts)))))
