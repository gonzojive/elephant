;;; -*- Mode: Lisp; Syntax: ANSI-Common-Lisp; Base: 10 -*-
;;; 
;;; Elephant users are granted the rights to distribute and use this software
;;; as governed by the terms of the Lisp Lesser GNU Public License
;;; (http://opensource.franz.com/preamble.html), also known as the LLGPL.

(defsystem ele-postmodern
  :name "ele-postmodern"
  :author "Henrik Hjelte <hhjelte@common-lisp.net>"
  :version "0.6.0"
  :licence "LLGPL"
  :description "Elephant postmodern postgresql backend"
  
  :components
  ((:module :src
	    :components
            ((:module :db-postmodern
		      :components
		      ((:file "package")
                       (:file "pm-sql")
                       (:file "pm-controller")
	                   (:file "pm-cache")
	           	       (:file "pm-transaction")
   	                   (:file "pm-btree")
                       (:file "pm-cursor")
                       (:file "pm-btree-index")
                       (:file "pm-indexed-btree")
                       (:file "pm-secondary"))
		      :serial t))))
  :depends-on (:postmodern
               :elephant))
