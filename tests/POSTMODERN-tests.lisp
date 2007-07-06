;;; POSTMODERN-tests.lisp
;;;
;;; part of
;;;
;;; Elephant: an object-oriented database for Common Lisp
;;;
;;; Copyright (c) 2005,2006 by Robert L. Read
;;; <rread@common-lisp.net>
;;;
;;; Elephant users are granted the rights to distribute and use this software
;;; as governed by the terms of the Lisp Lesser GNU Public License
;;; (http://opensource.franz.com/preamble.html), also known as the LLGPL.

(asdf:operate 'asdf:load-op :elephant-tests)

(in-package "ELEPHANT-TESTS")

(asdf:operate 'asdf:load-op :ele-postmodern)

;; Note:  One almost certainly has to execute "createdb elepm" as the user "postgres"
;; in order for this to work.
;; The Postemodern interface also apparently requries that "plpgsql language be loaded as 
;; a query language into the postgres instance.  This can be created by executing
;; this in a shell:
;;     psql -c 'create language plpgsql' elepm;
(defparameter *testpm-spec* '(:postmodern (:postgresql "127.0.0.1" "elepm" "postgres" "")))

(setf *default-spec* *testpm-spec*)

(do-backend-tests)

