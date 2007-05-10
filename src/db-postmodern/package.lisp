;;; -*- Mode: Lisp; Syntax: ANSI-Common-Lisp; Base: 10 -*-

(in-package :elephant)

(defpackage db-postmodern
  (:use :common-lisp
	:elephant :elephant-memutil :elephant-backend
	#+sbcl :sb-thread))


