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

#-elephant-without-optimize 
(declaim (optimize (speed 3) (safety 0) (space 0) (debug 0)))

;;
;; Cached slot access protocol
;;

(defmethod slot-value-using-class
    ((class persistent-metaclass) (instance persistent-object) (slot-def cached-slot-definition))
  (case (%cache-style class)
    (:checkout
     (if (checked-out-p instance)
	 (call-next-method)
	 (persistent-slot-reader (get-con instance) instance (slot-definition-name slot-def))))
    (:txn
     (persistent-slot-reader (get-con instance) instance (slot-definition-name slot-def)))
    (t 
     (persistent-slot-reader (get-con instance) instance (slot-definition-name slot-def)))))

(defmethod (setf slot-value-using-class)
    (new-value (class persistent-metaclass) (instance persistent-object) (slot-def cached-slot-definition))
  "Always write local slot value; maybe write persistent value if no caching or write-through"
  (case (%cache-style class)
    (:checkout
     (if (checked-out-p instance)
	 (call-next-method)
	 (persistent-slot-writer (get-con instance) new-value instance 
				 (slot-definition-name slot-def))))
;;	 (error "Cannot write to checkout-style cached objects when not checked out")))
    (t
     (persistent-slot-writer (get-con instance) new-value instance 
			     (slot-definition-name slot-def)))))

(defmethod slot-boundp-using-class 
    ((class persistent-metaclass) (instance persistent-object) (slot-def cached-slot-definition))
  "Checks if the slot exists in the database."
  (case (%cache-style class)
    (:checkout
     (if (checked-out-p instance)
	 (call-next-method)
	 (persistent-slot-boundp (get-con instance) instance (slot-definition-name slot-def))))
    (t (persistent-slot-boundp (get-con instance) instance (slot-definition-name slot-def)))))

(defmethod slot-makunbound-using-class 
    ((class persistent-metaclass) (instance persistent-object) (slot-def cached-slot-definition))
  "Removes the slot value from the database."
  (case (%cache-style class)
    (:checkout
     (if (checked-out-p instance)
	 (call-next-method)
	 (persistent-slot-makunbound (get-con instance) instance (slot-definition-name slot-def))))
    (t (persistent-slot-makunbound (get-con instance) instance (slot-definition-name slot-def)))))


;;
;; Cache mode and class-level operations
;;

(defmethod caching-style ((class persistent-metaclass))
  (%cache-style class))

(defmethod (setf caching-style) (style (class persistent-metaclass))
  (case style
    ((or :checkout :txn)
     (unless (cached-slot-defs class)
       (error "Cannot enable caching for classes with no cached slots"))
     (setf (%cache-style class) style))
    (:none 
     (setf (%cache-style class) style))
    (t (error "Unknown caching mode ~A" style))))

(defmethod cacheable-class ((class persistent-metaclass))
  (when (cached-slot-defs class) t))

;;
;; Cached instance operations
;;

(defmethod persistent-checked-out-p ((object cacheable-persistent-object))
  (pchecked-out-p object))

(defmethod persistent-checkout ((object cacheable-persistent-object))
  "Set the checkout state and refresh the memory slots"
  (ensure-transaction ()
    (unless (eq (%cache-style (class-of object)) :checkout)
      (error "Class ~A for object ~A is not enabled for checkout.  (mode=~A)"
	     (class-of object) object (%cache-style (class-of object))))
    (when (pchecked-out-p object)
      ;; This should be a condition that can fail silently?
      (error "Object ~A is already checked out" object))
    (setf (pchecked-out-p object) t) ;; grab write lock, rollback parallel txns
    ;; THIS IS BAD / READER ON OBJECT BEFORE CHECKOUT GETS STALE DATA
    ;; CAN WE BYPASS PROTOCOL TO WRITE MEMORY STORAGE DIRECTLY IN REFRESH?
    (setf (checked-out-p object) t) 
    (refresh-cached-slots object (cached-slot-names (class-of object)))
    object))

(defmethod persistent-sync ((object cacheable-persistent-object))
  "Synchronize the slots to the database without a checkin"
  (ensure-transaction ()
    (assert (pchecked-out-p object))
    (flush-cached-slots object (cached-slot-names (class-of object)))
    object))

(defmethod persistent-checkin ((object cacheable-persistent-object))
  "Flush the slot states to the database and release the checkout state.
   NOTE: Can this operation fail under concurrency if user enforces 
   single writer - e.g. checkin parallel with access, checkin parallel
   with attempted checkout?"
  (let ((checked-out t))
    (ensure-transaction ()
      (unless (eq (%cache-style (class-of object)) :checkout)
	(error "Cannot checkin if class caching style is ~A.  Canceling checkout." 
	       (%cache-style (class-of object)))
	(persistent-checkout-cancel object))
      (when (pchecked-out-p object)
	(setf (pchecked-out-p object) t) ;; establish a write lock
	(flush-cached-slots object (cached-slot-names (class-of object)))
	(setf (pchecked-out-p object) nil)
	(setf checked-out nil)))
    (setf (checked-out-p object) checked-out)
    object))

(defmethod persistent-checkout-cancel ((object cacheable-persistent-object))
  (ensure-transaction ()
    (assert (pchecked-out-p object))
    (setf (pchecked-out-p object) nil)
    (setf (checked-out-p object) nil)))

(defmacro with-persistent-checkouts (objects &rest body)
  "Make sure objects are checked out in the body and are
   checked back in when the form returns.  This acts as
   a guard by "
  (with-gensyms (object objs)
    `(let ((,objs (list ,@objects)))
       (unwind-protect 
	    (progn
	      (dolist (,object ,objs)
		(persistent-checkout ,object))
	      ,@body)
	 (dolist (,object ,objs)
	   (persistent-checkin ,object))))))

;;
;; Cached slot value manipulation utilities
;;

(defun refresh-cached-slots (instance slots)
  "Assumes checkout mode is t so side effects are only
   in memory"
  (assert (pchecked-out-p instance))
  (let ((sc (get-con instance)))
    (dolist (slot slots)
      (if (persistent-slot-boundp sc instance slot)
	  (setf (slot-value instance slot)
		(persistent-slot-reader sc instance slot))
	  (slot-makunbound instance slot)))))

(defun flush-cached-slots (instance slots)
  "Assumes object is checked out"
  (assert (pchecked-out-p instance))
  (let ((sc (get-con instance)))
    (dolist (slot slots)
      (if (slot-boundp instance slot)
	  (persistent-slot-writer sc (slot-value instance slot) instance slot)
	  (persistent-slot-makunbound sc instance slot)))))


;;
;; OLD STUFF
;;

#+nil ;; support for old method
 (defmethod cache-mode ((object persistent-object))
  (let ((imode (slot-value object 'cache-mode)))
    (cond ((= imode +none+)
	   :none-cached)
	  ((= imode +all-cached+)
	   :all-cached)
	  ((= imode +all-write-through+)
	   :write-through)
	  ((= imode +index-write-through+)
	   :index-write-through)
	  (t "Invalid cache mode - please write a valid mode"))))

#+nil ;; support for old method
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

#+nil
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

