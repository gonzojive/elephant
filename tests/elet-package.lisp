;;; -*- Mode: Lisp; Syntax: ANSI-Common-Lisp; Base: 10 -*-
;;;
;;; tests.lisp -- package definition
;;; 
;;; Initial version 9/02/2004 by Ben Lee
;;; <blee@common-lisp.net>
;;; 
;;; part of
;;;
;;; Elephant: an object-oriented database for Common Lisp
;;;
;;; Copyright (c) 2004 by Andrew Blumberg and Ben Lee
;;; <ablumberg@common-lisp.net> <blee@common-lisp.net>
;;;
;;; Elephant users are granted the rights to distribute and use this software
;;; as governed by the terms of the Lisp Lesser GNU Public License
;;; (http://opensource.franz.com/preamble.html), also known as the LLGPL.

(defpackage elephant-tests
  (:nicknames :ele-tests)
  #+use-fiveam
  (:use :common-lisp :elephant :5am)
  #-use-fiveam
  (:use :common-lisp :elephant :regression-test)
  (:import-from :elephant
		with-buffer-streams
		serialize
		deserialize)
  #+cmu  
  (:import-from :pcl
		finalize-inheritance
		slot-definition-name
		slot-makunbound-using-class
		class-slots)
  #+sbcl 
  (:import-from :sb-mop 
		finalize-inheritance
		slot-definition-name
		slot-makunbound-using-class
		class-slots)
  #+allegro
  (:import-from :clos
		finalize-inheritance
		slot-definition-name
		slot-makunbound-using-class
		class-slots)
  #+openmcl
  (:import-from :ccl
		finalize-inheritance
		slot-definition-name
		slot-makunbound-using-class
		class-slots)
  #+lispworks  
  (:import-from :clos
		finalize-inheritance
		slot-definition-name
		slot-makunbound-using-class
		class-slots)
  )