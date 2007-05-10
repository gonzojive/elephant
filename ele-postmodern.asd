
(defsystem ele-postmodern
  :name "ele-postmodern"
  :author "Henrik Hjelte <hhjelte@common-lisp.net"
  :version "0.6.0"
  :licence "GPL"
  :description "Elephant postmodern postgresql backend"
  
  :components
  ((:module :src
	    :components
            ((:module :db-postmodern
		      :components
		      ((:file "package")
		       (:file "pm-controller")
                       (:file "pm-sql")
		       (:file "pm-transaction")
                       (:file "pm-btree")
                       (:file "pm-cursor")
                       (:file "pm-btree-index")
                       (:file "pm-indexed-btree")
                       (:file "pm-secondary"))
		      :serial t))))
  :depends-on (:postmodern
               :elephant))
