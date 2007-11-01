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
;; The Postmodern interface also requries that "plpgsql language be loaded as 
;; a query language into the postgres instance.  This can be created by executing
;; this in a shell:
;;     psql -c 'create language plpgsql' elepm;

(defparameter *testpm-spec* '(:postmodern (:postgresql "127.0.0.1" "elepm" "postgres" "")))

(setf *default-spec* *testpm-spec*)

(do-backend-tests)

;; Hints for configuring postgresql (Unix biased)
;; ==============================================
;;       As root, switch to the postgres user
;;       
;;           su postgres 
;;       
;;       Configure pg_hba.conf. As postgres user, edit /var/lib/pgsql/data/pg_hba.conf to 
;;       allow password logins.The change is 'md5' instead of 'ident sameuser' in two places
;;           
;;           # TYPE DATABASE USER CIDR-ADDRESS METHOD 
;;           
;;           # "local" is for Unix domain socket connections only 
;;           
;;           local all all ident sameuser 
;;           
;;           # IPv4 local connections: 
;;           
;;           host all all 127.0.0.1/32 md5 
;;           
;;           # IPv6 local connections: 
;;           
;;           host all all ::1/128 md5 
;;       
;;       Create database, as postgres user.
;;       
;;           su postgres
;;           psql -c "create user myuser with password 'mypassword';"
;;
;;      Now you should be able to have run under a specific user:
;;      (defparameter *sample-postmodern-spec* '(:postmodern (:postgresql "127.0.0.1" "elepm" "myuser" "mypassword")))

;; Convenient kill/yank to remake a database, run as postgres user;
;;    "
;;     dropdb elepm;
;;     createdb elepm;
;;     psql -c 'grant all on database elepm to myuser;' postgres;
;;     psql -c 'create language plpgsql' elepm;
;;    "
