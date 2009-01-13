;;; -*- Mode: Lisp; Syntax: ANSI-Common-Lisp; Base: 10 -*-
;;;
;;; serializer2.lisp -- convert Lisp data to/from byte arrays
;;; 
;;; By Ian Eslick, <ieslick common-lisp net>
;;; Adapted from serializer1 by Ben Lee <blee@common-lisp.net>
;;; 
;;; part of
;;;
;;; Elephant: an object-oriented database for Common Lisp
;;;
;;; Copyright (c) 2004 by Andrew Blumberg and Ben Lee
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

(defpackage :elephant-serializer2
  (:use :cl :elephant :elephant-memutil :elephant-utils)
  #+cmu
  (:import-from :bignum
		%bignum-ref)
  (:import-from :elephant 
		*circularity-initial-hash-size*
		get-cached-instance
		slot-definition-allocation
		slot-definition-name
		compute-slots
		slots-and-values
		struct-slots-and-values
		oid
;;		make-oid-pair
;;		oid-pair
;;		oid-pair-left
;;		oid-pair-right
		int-byte-spec
		array-type-from-byte
	        byte-from-array-type
		database-version
		translate-and-intern-symbol
		valid-persistent-reference-p
		signal-cross-reference-error
		elephant-type-deserialization-error
		controller-recreate-instance
                recreate-instance-using-class
		controller-marking-p
		gc-mark-new-write))

(in-package :elephant-serializer2)

#-elephant-without-optimize 
(declaim  (optimize (speed 3) (safety 0) (space 0) (debug 0)))

(uffi:def-type foreign-char :char)

;; Constants

(defconstant +fixnum32+              1)
(defconstant +fixnum64+              2)
(defconstant +char+                  3)
(defconstant +single-float+          4)
(defconstant +double-float+          5)
(defconstant +negative-bignum+       6)
(defconstant +positive-bignum+       7)
(defconstant +rational+              8)

;; Save constants by splitting strings and encoding
(defconstant +utf8-string+           9)
(defconstant +utf16-string+         10)
(defconstant +utf32-string+         11)

;; String-based aggregates
(defconstant +pathname+             12)
(defconstant +symbol+               13)

;; Stored by ID (requires instance table)
(defconstant +persistent-ref+       14)
;; Stored by id+classname
(defconstant +persistent+           15)

;; Composite objects
(defconstant +cons+                 16)
(defconstant +hash-table+           17)
(defconstant +object+               18)
(defconstant +array+                19)
(defconstant +struct+               20)
(defconstant +class+                21)
(defconstant +complex+              22)
;;(defconstant +oid-pair+             23)

;; Lispworks support
(defconstant +short-float+          30)

(defconstant +nil+                  #x3F)
(defconstant +reserved-dbinfo+      #xF0)

;; Arrays
(defconstant +fill-pointer-p+     #x20)
(defconstant +adjustable-p+       #x40)

;;
;; Circularity Hash for Serializer
;;

(defparameter *circularity-hash-queue* (make-array 20 :fill-pointer 0 :adjustable t)
  "Circularity ids for the serializer.")

(defparameter *serializer-fast-lock* (ele-make-fast-lock))

(defun get-circularity-hash ()
  "Get a clean hash for object serialization"
  (declare (type fixnum *circularity-initial-hash-size*))
  (or
     (ele-with-fast-lock (*serializer-fast-lock*)
       (and (plusp (length *circularity-hash-queue*))
            (vector-pop *circularity-hash-queue*)))
     (make-hash-table :test 'eq :size *circularity-initial-hash-size*)))

(defun release-circularity-hash (hash)
  "Return the hash to the queue for reuse"
  (unless (= (hash-table-count hash) 0)
    (clrhash hash))
  (ele-with-fast-lock (*serializer-fast-lock*)
    (vector-push-extend hash *circularity-hash-queue*)))

;;
;; Circularity Hash for Deserializer
;;
;; NOTE: this strategy may create GC problems as it maintains references to
;; potentially large objects

(defparameter *circularity-vector-queue* (make-array 20 :fill-pointer 0 :adjustable t)
  "A list of vectors used for linear deserialization.
   This works nicely because all ID's are written
   in integer order to the stream, so we can just write
   the next one into the array already knowing what the
   ID is")

   
(defun get-circularity-vector ()
  "Get a fresh vector"
  (or (ele-with-fast-lock (*serializer-fast-lock*)
	(and (plusp (length *circularity-vector-queue*))
	     (vector-pop *circularity-vector-queue*)))
      (make-array 50 :element-type t :initial-element nil 
		  :fill-pointer 0 :adjustable t)))

(defun release-circularity-vector (vector)
  "Don't need to erase, just reset fill-pointer as it 
   determines extent of valid data"
  (setf (fill-pointer vector) 0)
  (ele-with-fast-lock (*serializer-fast-lock*)
    (vector-push-extend vector *circularity-vector-queue* 20)))

;;
;; SERIALIZER
;;

(defconstant +2^31+ (expt 2 31))
(defconstant +2^32+ (expt 2 32))
(defconstant +2^63+ (expt 2 63))
(defconstant +2^64+ (expt 2 64))

;;(defparameter symbol-package-hash (make-hash-table :size 10000)
;;  "In SBCL the lookup of packages conses like crazy.  This is a workaround")


(defun serialize (frob bs sc)
  "Serialize a lisp value into a buffer-stream."
  (declare (type buffer-stream bs)
	   (ignorable sc))
  (let ((lisp-obj-id -1)
	(circularity-hash 
	 (unless (or (stringp frob) (symbolp frob) (numberp frob))
	   (get-circularity-hash))))
    (declare (type fixnum lisp-obj-id))
    (labels 
	((%next-object-id ()
	   (incf lisp-obj-id))
	 (%serialize (frob)
;;	   (format t "Serializing ~A of type ~A~%" frob (type-of frob))
	   (typecase frob
	     (fixnum 
	      (if (< #.most-positive-fixnum +2^31+) ;; should be compiled away
		  (progn
		    (buffer-write-byte +fixnum32+ bs)
		    (buffer-write-fixnum32 frob bs))
		  (progn
		    (assert (eq (< #.most-positive-fixnum +2^63+) t))
		    (if (< (abs frob) +2^31+)
			(progn
			  (buffer-write-byte +fixnum32+ bs)
			  (buffer-write-fixnum32 frob bs))
			(progn
			  (buffer-write-byte +fixnum64+ bs)
			  (buffer-write-fixnum64 frob bs))))))
	     (null
	      (buffer-write-byte +nil+ bs))
	     (symbol
	      (let ((sym-name (symbol-name frob)))
		(declare (type string sym-name)
			 (dynamic-extent sym-name))
		(buffer-write-byte +symbol+ bs)
		(serialize-string sym-name bs)
 		(let ((package (symbol-package frob)))
 		  (declare (dynamic-extent package)
 			   (type (or null package) package))
 		  (if package
 		      (serialize-string (package-name package) bs)
 		      (buffer-write-byte +nil+ bs)))))
;;		(let ((package-name (gethash frob symbol-package-hash)))
;;		  (unless package-name
;;		    (setq package-name 
;;			  (setf (gethash frob symbol-package-hash)
;;				(package-name (symbol-package frob)))))
;;		  (if package-name
;;		      (serialize-string package-name bs)
;;		      (buffer-write-byte +nil+ bs)))))
	     (string
	      (serialize-string frob bs))
	     (persistent
	      (unless (valid-persistent-reference-p frob sc)
		(signal-cross-reference-error frob sc))
	      (when (controller-marking-p sc)
		(gc-mark-new-write frob))
	      (buffer-write-byte +persistent-ref+ bs)
	      (buffer-write-oid (oid frob) bs))
	     #+lispworks
	     (short-float
	      (buffer-write-byte +short-float+ bs)
	      (buffer-write-float (coerce frob 'single-float) bs))
	     (single-float
	      (buffer-write-byte +single-float+ bs)
	      (buffer-write-float frob bs))
	     (double-float
	      (buffer-write-byte +double-float+ bs)
	      (buffer-write-double frob bs))
	     (standard-object
	      ;; NOTE: Add support for schema validation
	      (buffer-write-byte +object+ bs)
	      (let ((idp (gethash frob circularity-hash)))
		(if idp (buffer-write-int32 idp bs)
		    (progn
		      (let ((id (%next-object-id)))
			(buffer-write-int32 id bs)
			(setf (gethash frob circularity-hash) id))
		      (%serialize (type-of frob))
		      (let ((svs (slots-and-values frob)))
			(%serialize (/ (length svs) 2))
			(loop for item in svs
			   do (%serialize item)))))))
	     (integer
	      (serialize-bignum frob bs))
	     (rational
	      (buffer-write-byte +rational+ bs)
	      (%serialize (numerator frob))
	      (%serialize (denominator frob)))
	     (character
	      (buffer-write-byte +char+ bs)
	      ;; might be wide!
	      (buffer-write-uint32 (char-code frob) bs))
;;	     (oid-pair
;;	      (buffer-write-byte +oid-pair+ bs)
;;	      (buffer-write-int32 (oid-pair-left frob) bs)
;;	      (buffer-write-int32 (oid-pair-right frob) bs))
	     (cons
	      (buffer-write-byte +cons+ bs)
	      (let ((idp (gethash frob circularity-hash)))
		(if idp (buffer-write-int32 idp bs)
		    (progn
		      (let ((id (%next-object-id)))
			(buffer-write-int32 id bs)
			(setf (gethash frob circularity-hash) id))
		      (%serialize (car frob))
		      (%serialize (cdr frob))))))
	     (pathname
	      (let ((pstring (namestring frob)))
		(buffer-write-byte +pathname+ bs)
		(serialize-string pstring bs)))
	     (complex 
	      (buffer-write-byte +complex+ bs)
	      (%serialize (realpart frob))
	      (%serialize (imagpart frob)))
	     (hash-table
	      (buffer-write-byte +hash-table+ bs)
	      (let ((idp (gethash frob circularity-hash)))
		(if idp (buffer-write-int32 idp bs)
		    (progn
		      (let ((id (%next-object-id)))
			(buffer-write-int32 id bs)
			(setf (gethash frob circularity-hash) id))
		      (%serialize (hash-table-test frob))
		      (%serialize (hash-table-rehash-size frob))
		      (%serialize (hash-table-rehash-threshold frob))
		      (%serialize (hash-table-count frob))
		      (loop for key being the hash-key of frob
			 using (hash-value value)
			 do 
			   (%serialize key)
			   (%serialize value))))))
	     (array
	      (buffer-write-byte +array+ bs)
	      (let ((idp (gethash frob circularity-hash)))
		(if idp (buffer-write-int32 idp bs)
		    (progn
		      (let ((id (%next-object-id)))
			(buffer-write-int32 id bs)
			(setf (gethash frob circularity-hash) id))
		      (buffer-write-byte 
		       (logior (byte-from-array-type (array-element-type frob))
			       (if (array-has-fill-pointer-p frob) 
				   +fill-pointer-p+ 0)
			       (if (adjustable-array-p frob) 
				   +adjustable-p+ 0))
		       bs)
		      (let ((rank (array-rank frob)))
			(buffer-write-int32 rank bs)
			(loop for i fixnum from 0 below rank
			   do (%serialize (array-dimension frob i))))
		      (when (array-has-fill-pointer-p frob)
			(%serialize (fill-pointer frob)))
		      (loop for i fixnum from 0 below (array-total-size frob)
			 do
			 (%serialize (row-major-aref frob i)))))))
	     (structure-object 
	      (buffer-write-byte +struct+ bs)
	      (let ((idp (gethash frob circularity-hash)))
		(if idp (buffer-write-int32 idp bs)
		    (progn
		      (buffer-write-int32 (incf lisp-obj-id) bs)
		      (setf (gethash frob circularity-hash) lisp-obj-id)
		      (%serialize (type-of frob))
		      (let ((svs (struct-slots-and-values frob)))
			(%serialize (/ (length svs) 2))
			(loop for item in svs
			   do (%serialize item)))))))
 	     (t (format t "Can't serialize a object: ~A of type ~A~%" frob (type-of frob))))))
      (%serialize frob)
      (when circularity-hash
	(release-circularity-hash circularity-hash))
      bs)))

(defun serialize-bignum (frob bs)
  "Serialize bignum to buffer stream"
  (declare (type integer frob)
	   (type buffer-stream bs))
  (let* ((num (abs frob))
	 (word-size (ceiling (/ (integer-length num) 32)))
	 (needed (* word-size 4))
	 (byte-spec (byte 32 0)))
    (declare (type fixnum word-size needed)
	     (type cons byte-spec)
	     (ignorable byte-spec))
    (if (< frob 0) 
	(buffer-write-byte +negative-bignum+ bs)
	(buffer-write-byte +positive-bignum+ bs))
    (buffer-write-uint32 needed bs)
    (loop for i fixnum from 0 below word-size 
       ;; this ldb is consing on CMUCL/OpenMCL!
       ;; there is an OpenMCL function which should work 
       ;; and non-cons
       do
	 #+cmu
	 (buffer-write-uint32 (%bignum-ref num i) bs) ;; should fail under 64-bit CMU
	 #-cmu
	 (buffer-write-uint32 (ldb (byte 32 (* 32 i)) num) bs)
	 )))

;;;
;;; DESERIALIZER
;;;

(defparameter *trace-deserializer* t)

(defparameter *tag-table*
  `((,+fixnum32+ . "fixnum32")
    (,+fixnum64+ . "fixnum64")
    (,+char+ . "char")
    (,+short-float+ . "short-float")
    (,+single-float+ . "single-float")
    (,+double-float+ . "double float")
    (,+negative-bignum+ . "neg bignum")
    (,+positive-bignum+ . "pos bignum")
    (,+rational+ . "rational number")
    (,+nil+ . "null")
    (,+utf8-string+ . "UTF8 string")
    (,+utf16-string+ . "UTF16le string")
    (,+utf32-string+ . "UTF32le string")
    (,+symbol+ . "symbol")
    (,+pathname+ . "pathname")
    (,+persistent+ . "persistent object (old)")
    (,+persistent-ref+ . "persistent object reference (new)")
;;    (,+oid-pair+ . "oid pair for associations")
    (,+cons+ . "cons cell")
    (,+hash-table+ . "hash table")
    (,+object+ . "standard object")
    (,+array+ . "array")
    (,+struct+ . "struct")
    (,+class+ . "class")
    (,+complex+ . "complex")))

(defun enable-deserializer-tracing ()
  (setf *trace-deserializer* t))

(defun disable-deserializer-tracing ()
  (setf *trace-deserializer* nil))

(defun print-pre-deserialize-tag (tag)
  (when *trace-deserializer*
    (let ((tag-name (assoc tag *tag-table*)))
      (if tag-name
	  (format t "Deserializing type: ~A~%" tag-name)
	  (progn
	    (format t "Unrecognized tag: ~A~%" tag)
	    (break))))))

(defun print-post-deserialize-value (value)
  (when *trace-deserializer*
    (format t "Returned: ~A~%" value)))

(defun deserialize (buf-str sc &optional oid-only)
  "Deserialize a lisp value from a buffer-stream."
  (declare (type (or null buffer-stream) buf-str))
  (let ((circularity-vector (get-circularity-vector)))
    (labels 
      ((lookup-id (id)
	 (if (>= id (fill-pointer circularity-vector)) nil
	     (aref circularity-vector id)))
       (add-object (object)
	 (vector-push-extend object circularity-vector 50)
	 (1- (fill-pointer circularity-vector)))
       (%deserialize (bs)
	 (declare (type buffer-stream bs))
	 (let ((tag (buffer-read-byte bs)))
	   (declare (type foreign-char tag)
		    (dynamic-extent tag))
;;	   (print-pre-deserialize-tag tag)
	   (let ((value  
	   (cond
	     ((= tag +fixnum32+)
	      (buffer-read-fixnum32 bs))
	     ((= tag +fixnum64+)
	      (buffer-read-fixnum64 bs))
	     ((= tag +nil+) nil)
	     ((= tag +utf8-string+)
	      #+lispworks
	      (coerce (deserialize-string :utf8 bs) 'base-string)

	      (deserialize-string :utf8 bs))
	     ((= tag +utf16-string+)
	      #+lispworks
	      (coerce (deserialize-string :utf16le bs) 'lw:text-string)
	      #-lispworks
	      (deserialize-string :utf16le bs))
	     ((= tag +utf32-string+)
	      #+lispworks
	      (coerce (deserialize-string :utf32le bs) 'sys:augmented-string)
	      #-lispworks
	      (deserialize-string :utf32le bs))
	     ((= tag +symbol+)
	      (let ((name (%deserialize bs))
		    (package (%deserialize bs)))
		(translate-and-intern-symbol sc name package)))
	     ((= tag +persistent+)
	      (let ((oid (buffer-read-oid bs))
		    (cname (%deserialize bs)))
		(if oid-only oid
		    (controller-recreate-instance sc oid cname))))
	     ((= tag +persistent-ref+)
	      (let ((oid (buffer-read-oid bs)))
		(if oid-only oid
		    (controller-recreate-instance sc oid))))
	     #+lispworks
	     ((= tag +short-float+)
	      (coerce (buffer-read-float bs) 'short-float))
	     ((= tag +single-float+)
	      (buffer-read-float bs))
	     ((= tag +double-float+)
	      (buffer-read-double bs))
	     ((= tag +char+)
	      (code-char (buffer-read-uint32 bs)))
	     ((= tag +pathname+)
	      (parse-namestring (or (%deserialize bs) "")))
	     ((= tag +positive-bignum+) 
	      (deserialize-bignum bs (buffer-read-uint32 bs) t))
	     ((= tag +negative-bignum+) 
	      (deserialize-bignum bs (buffer-read-uint32 bs) nil))
	     ((= tag +rational+) 
	      (/ (the integer (%deserialize bs)) 
		 (the integer (%deserialize bs))))
;;	     ((= tag +oid-pair+)
;;	      (let ((pair (make-oid-pair)))
;;		(setf (oid-pair-left pair) (buffer-read-fixnum32 bs))
;;		(setf (oid-pair-right pair) (buffer-read-fixnum32 bs))))
	     ((= tag +cons+)
	      (let* ((id (buffer-read-int32 bs))
		     (maybe-cons (lookup-id id)))
		(declare (type fixnum id))
		(if maybe-cons maybe-cons
		    (let ((c (cons nil nil)))
		      (add-object c)
		      (setf (car c) (%deserialize bs))
		      (setf (cdr c) (%deserialize bs))
		      c))))
	     ((= tag +complex+)
	      (let ((rpart (%deserialize bs))
		    (ipart (%deserialize bs)))
		(complex rpart ipart)))
	     ((= tag +hash-table+)
	      (let* ((id (buffer-read-int32 bs))
		     (maybe-hash (lookup-id id)))
		(declare (type fixnum id))
;;		(format t "~A ~A~%" maybe-hash id)
		(if maybe-hash maybe-hash
		    (let* ((test (%deserialize bs))
			   (rehash-size (%deserialize bs))
			   (rehash-threshold (%deserialize bs))
			   (size (%deserialize bs))
			   (h (make-hash-table :test test
					       :rehash-size rehash-size
					       :rehash-threshold rehash-threshold
					       :size (ceiling (* (ceiling (/ (+ size 10) rehash-threshold)) rehash-size)))))
		      (add-object h)
		      (loop for i fixnum from 0 below size
			    do
			    (setf (gethash (%deserialize bs) h)
				  (%deserialize bs)))
		      h))))
	     ((= tag +object+)
	      (let* ((id (buffer-read-int32 bs))
		     (maybe-o (lookup-id id)))
		(if maybe-o maybe-o
		    (let ((typedesig (%deserialize bs)))
		      ;; now, depending on what typedesig is, we might 
		      ;; or might not need to specify the store controller here..
		      (let ((o 
			     (or (handler-case
				   (if (subtypep typedesig 'persistent)
				       (recreate-instance-using-class (find-class typedesig) :sc sc)
				       ;; if the this type doesn't exist in our object
				       ;; space, we can't reconstitute it, but we don't want 
				       ;; to abort completely, we will return a special object...
				       ;; This behavior could be configurable; the user might 
				       ;; prefer an abort here, but I prefer surviving...
				       (make-instance typedesig))
 				   (error (v) (format t "got typedesig error: ~A ~A ~%" v typedesig)
 					  (list 'caught-error v typedesig)))
				 (list 'uninstantiable-object-of-type typedesig))))
			(if (listp o)
			    o
			    (progn
			      (add-object o)
			      (loop for i fixnum from 0 below (%deserialize bs)
				    do
				    (setf (slot-value o (%deserialize bs))
					  (%deserialize bs)))
			      o)))))))
	     ((= tag +array+)
	      (let* ((id (buffer-read-int32 bs))
		     (maybe-array (lookup-id id)))
		(if maybe-array maybe-array
		    (let* ((flags (buffer-read-byte bs))
			   (a (make-array 
			       (loop for i fixnum from 0 below 
				     (buffer-read-int32 bs)
				     collect (%deserialize bs))
			       :element-type (array-type-from-byte 
					      (logand #x1f flags))
			       :fill-pointer (/= 0 (logand +fill-pointer-p+ 
							   flags))
			       :adjustable (/= 0 (logand +adjustable-p+ 
							 flags)))))
		      (when (array-has-fill-pointer-p a)
			(setf (fill-pointer a) (%deserialize bs)))
		      (add-object a)
		      (loop for i fixnum from 0 below (array-total-size a)
			    do
			    (setf (row-major-aref a i) (%deserialize bs)))
		      a))))
	     ((= tag +struct+)
	      (let* ((id (buffer-read-int32 bs))
		     (maybe-o (lookup-id id)))
		(if maybe-o maybe-o
		    (let ((typedesig (%deserialize bs)))
		      (let ((o (or (handler-case
				       (funcall (struct-constructor typedesig))
				     (error (v) (format t "got typedesig error for struct: ~A ~A ~%" v typedesig)
					    (list 'caught-error v typedesig)))
				   (list 'uninstantiable-object-of-type typedesig))))
			(if (listp o) o
			    (progn
			      (add-object o)
			      (loop for i fixnum from 0 below (%deserialize bs) do
				   (let ((name (%deserialize bs))
					 (value (%deserialize bs)))
				     (setf (slot-value o name) value)))
			      o)))))))
	     (t (error 'elephant-type-deserialization-error :tag tag)))))
;;	     (print-post-deserialize-value value)
	     value))))
      (etypecase buf-str 
	(null (return-from deserialize nil))
	(buffer-stream
	 (let ((result (%deserialize buf-str)))
	   (release-circularity-vector circularity-vector)
	   result))))))

(defun deserialize-bignum (bs length positive)
  (declare (type buffer-stream bs)
	   (type fixnum length)
	   (type boolean positive))
  (let ((int-byte-spec (byte 32 0)))
    (declare (dynamic-extent int-byte-spec)
	     (ignorable int-byte-spec))
    (loop for i from 0 below (/ length 4)
       for byte-spec = 
;;	 #+(or allegro) (progn (setf (cdr int-byte-spec) (* 32 i)) int-byte-spec)
	 #+(or allegro sbcl cmu lispworks openmcl) (byte 32 (* 32 i))
       with num of-type integer = 0 
       do
	 (setq num (dpb (buffer-read-uint32 bs) byte-spec num))
       finally 
	 (return (if positive num (- num))))))
