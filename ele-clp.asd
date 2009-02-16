;;; -*- Mode: Lisp; Syntax: ANSI-Common-Lisp; Base: 10 -*-
;;;
;;; ele-prevalence.asd -- ASDF system definition for elephant prevalence-style data store
;;; 
;;; Initial version 5/01/2007 by Ian Eslick
;;; 
;;; part of
;;;
;;; Elephant: an object-oriented database for Common Lisp
;;;
;;; Copyright (c) 2007 by Ian Eslick <ieslick common-lisp net>
;;;
;;; Elephant users are granted the rights to distribute and use this software
;;; as governed by the terms of the Lisp Lesser GNU Public License
;;; (http://opensource.franz.com/preamble.html), also known as the LLGPL.

(defsystem ele-clp
  :name "Elephant cl-prevalence data store"
  :author "Ian Eslick"
  :version "1.0"
  :maintainer "Ian Eslick <ieslick common-lisp net>"
  :licence "LLGPL"
  :description "Prevalence style database for Elephant based on CL-PREVALENCE"
  :components
  ((:module :src
	    :components
	    ((:module :db-clp
		      :components
		      ((:file "package")
		       (:file "primitives")
		       (:file "clp-controller"))
		      :serial t))))
;   (:file "clprev-slots")
;   (:file "clprev-collections")
;   (:file "clprev-transactions")
  :serial t
  :depends-on (:elephant :cl-prevalence :cl-containers))
