;;; -*- Mode: Lisp; Syntax: ANSI-Common-Lisp; Base: 10 -*-
;;;
;;; db-prevalence/package.lisp -- Package defs for db-prevalence
;;; 
;;; Initial version 1/20/2009 by Ian Eslick
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

(in-package :cl-user)

(defpackage db-clp
  (:shadow size execute-transaction)
  (:use common-lisp elephant elephant-utils elephant-data-store cl-prevalence)
  (:export #:snapshot #:restore))
