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

(defsystem ele-prevalence
  :name "Elephant prevalence data store"
  :author "Ian Eslick"
  :version "0.9.8"
  :maintainer "Ian Eslick <ieslick common-lisp net>"
  :licence "LLGPL"
  :description "Prevalence style database for Elephant"
  :components
  ((:file "package")
   (:file "prev-controller")
   (:file "prev-slots")
   (:file "prev-collections")
   (:file "prev-transactions")
   )
  :serial t
  :depends-on (:elephant))
