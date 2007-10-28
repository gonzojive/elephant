;;; -*- Mode: Lisp; Syntax: ANSI-Common-Lisp; Base: 10 -*-
;;;
;;; gc.lisp - A wrapper around the migrate interface to support
;;;           stop-and-copy GC at the repository level
;;; 
;;; By Ian Eslick <ieslick at common-lisp.net>
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

;; NOTE: we need to inhibit any operations on the old controller and
;; redirect them to the new one if other threads are accessing
;; can we do this with a lock in get-con?  get-con is used alot, 
;; though, so perhaps we should just rely on the user?

(defgeneric stop-and-copy-gc (sc &key &allow-other-keys)
  (:documentation "Wrap all the migrate machinery in a
   simple top-level gc call.  This will copy all the data
   in the source store to the target store.  Accesses to 
   the store should be inhibited by the user.  
   mapspec saves on memory space by storing oid->object maps on disk
   replace-source means to have the resulting spec be the same 
      as the source using copy-spec.  
   delete-source means do not keep a backup 
   backup means to make a backup with a name derived from the src"))

(defmethod stop-and-copy-gc ((src store-controller) &key target mapspec replace-source delete-source backup)
  (let ((src-spec (controller-spec src))
	(src-type (first (controller-spec src))))
    (when mapspec (set-oid-spec mapspec))
    (unless target (setf target (temp-spec src-type src-spec)))
    (let ((target (gc-open-target target))
	  (global? (eq src *store-controller*)))
      ;; Copy the source before migrate to recover if necessary
      (when backup
	(copy-spec src-type src-spec (temp-spec src-type src-spec)))
      ;; Primary call to migrate
      (migrate target src)
      ;; Cleanup mapspec
      (when mapspec 
	(set-oid-spec nil)
	(delete-spec (first mapspec) mapspec))
      ;; Close 
      (when replace-source
	(unless (eq src-spec (first target))
	  (error "Cannot perform replace-source on specs of different types: ~A -> ~A" target src-spec))
	(copy-spec src-type target src-spec)
	(when delete-source
	  (delete-spec (first target) target)))
      (when global?
	(setf *store-controller* target))
      target)))

(defmethod gc-open-target (spec)
  "Ignore *store-controller*"
  (initialize-user-parameters)
  (let ((controller (get-controller spec)))
    (open-controller controller)
    controller))

(defgeneric temp-spec (type spec)
  (:documentation "Create a temporary specification with source spec as hint")
  (:method (type spec)
    (declare (ignore spec))
    (error "temp-spec not implemented for type: ~A" type)))

(defgeneric delete-spec (type spec)
  (:documentation "Delete the storage associated with spec")
  (:method (type spec)
    (declare (ignore spec))
    (error "delete-spec not implemented for type: ~A" type)))

(defgeneric copy-spec (type src targ)
  (:documentation "Copy files associated with spec from src to targ")
  (:method (type src targ)
    (declare (ignore src targ))
    (error "copy-spec not implemented for type: ~A" type)))
