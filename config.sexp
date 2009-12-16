
;; Linux defaults
#+(and (or sbcl allegro openmcl lispworks) (not (or mswindows windows win32)) (not (or macosx darwin)))
((:compiler . :gcc)
 (:berkeley-db-version . "4.5")
 (:berkeley-db-include-dir . "/usr/local/BerkeleyDB.4.5/include/")
 (:berkeley-db-lib-dir . "/usr/local/BerkeleyDB.4.5/lib/")
 (:berkeley-db-lib . "/usr/local/BerkeleyDB.4.5/lib/libdb-4.5.so")
 (:berkeley-db-deadlock . "/usr/local/BerkeleyDB.4.5/bin/db_deadlock")
 (:berkeley-db-cachesize . 20971520)
 (:berkeley-db-max-locks . 2000)
 (:berkeley-db-max-objects . 2000)
 (:berkeley-db-max-transactions . 1000)
 (:berkeley-db-map-degree2 . t)
 (:berkeley-db-mvcc . nil)
 (:clsql-lib-paths . nil)
 (:prebuilt-libraries . nil)
 (:warn-when-dropping-persistent-slots . t)
 (:return-null-on-missing-instance . t)
 (:no-deserialization-package-found-action . :warn))

;; OSX Defaults 
#+(and (or sbcl allegro openmcl lispworks) (not (or mswindows windows win32)) (or macosx darwin))
((:compiler . :gcc)
 (:berkeley-db-version . "4.5")
 (:berkeley-db-include-dir . "/usr/local/BerkeleyDB.4.5/include/")
 (:berkeley-db-lib-dir . "/usr/local/BerkeleyDB.4.5/lib/")
 (:berkeley-db-lib . "/usr/local/BerkeleyDB.4.5/lib/libdb-4.5.dylib")
 (:berkeley-db-deadlock . "/usr/local/BerkeleyDB.4.5/bin/db_deadlock")
 (:berkeley-db-cachesize . 20971520)
 (:berkeley-db-max-locks . 2000)
 (:berkeley-db-max-objects . 2000)
 (:berkeley-db-map-degree2 . t)
 (:berkeley-db-mvcc . nil)
 (:clsql-lib-paths . nil)
 (:prebuilt-libraries . nil)
 (:warn-when-dropping-persistent-slots . t)
 (:return-null-on-missing-instance . t)
 (:no-deserialization-package-found-action . :warn))

;; Windows defaults (assumes prebuild libraries)
#+(or mswindows windows win32)
((:compiler . :cygwin)
 (:berkeley-db-version . "4.5")
 (:berkeley-db-include-dir . "C:/Program Files/Oracle/Berkeley DB 4.5.20/include/")
 (:berkeley-db-lib-dir . "C:/Program Files/Oracle/Berkeley DB 4.5.20/bin/")
 (:berkeley-db-lib . "C:/Program Files/Oracle/Berkeley DB 4.5.20/bin/libdb45.dll")
 (:berkeley-db-deadlock . "C:/Program Files/Oracle/Berkeley DB 4.5.20/bin/db_deadlock.exe")
 (:berkeley-db-cachesize . 20971520)
 (:berkeley-db-max-locks . 2000)
 (:berkeley-db-max-objects . 2000)
 (:berkeley-db-map-degree2 . t)
 (:berkeley-db-mvcc . nil)
 (:clsql-lib-paths . nil)
 (:prebuilt-libraries . t)
 (:warn-when-dropping-persistent-slots . t)
 (:return-null-on-missing-instance . t)
 (:no-deserialization-package-found-action . :warn))

;; Berkeley 4.7 is required and each system will have different 
;; settings for these directories, use this as an indication of 
;; what each key means
;;
;; :prebuilt-libraries is true by default for windows machines.  It causes
;; the library loader to look in the elephant root directory for the shared 
;; libraries.  (nil or t)
;;
;; :clsql-lib-paths tell clsql where to look for which ever SQL distribution
;; library files you need it to look for.  For example...
;;
;;  (:clsql-lib-paths . ("/Users/me/Work/SQlite3/" "/Users/me/Work/Postgresql/"))
;;
;; :compiler options are 
;;           :gcc (default: for unix platforms with /usr/bin/gcc)
;;           :cygwin (for windows platforms with cygwin/gcc)
;;           :msvc (unsupported)
;;
;; :warn-when-dropping-persistent-slots
;;            When you redefine a class, you may have changed the name of a function
;;            unless you add a redefinition function to the current db schema, 
;;            which copies the data, that data will be lost.  This determines whether
;;            the system interactively warns you about this.  Defaults to true.
;;
;; :return-null-on-missing-instance 
;;            When the deserializer does not find an instance for a given oid in
;;            the instance table, for now it assumes it is gone (a poor mans 
;;            referential integrity.  Defaults to true
;;
;; :no-deserialization-package-found-action
;;            When a symbol is interned in a package that has not been created,
;;            this determines the action.  :warn, :create, :error
;;
;; Additional supported parameters include:
;;
;; :berkeley-db-version 
;;          Tells the db-bdb backend which version of the 
;;          constants to load to match the header files of 
;;          the specific BDB version.  It's a hack vs. using 
;;          CFFI to do this automatically, but it gives us 
;;          configurability without much pain, maintenance 
;;          or external dependencies
;;
;; :berkeley-db-cachesize
;;          An integer indicating the number of bytes
;;          for the page cache, default 20MB which is
;;          about enough storage for 10k-30k indexed
;;          persistent objects
;;
;; :berkeley-db-map-degree2 
;;          Boolean parameter that indicates whether map
;;          operations lock down the btree for the entire
;;          transaction or whether they allow other
;;          transactions to add/delete/modify values
;;          before the map operation is completed.  The
;;          map operation remains stable and any writes
;;          are kept transactional, see user manual as
;;          well as berkeley DB docs for more details.
;;
;; :berkeley-db-max-locks 
;;          Number of locks to reserve in the transaction environment
;;
;; :berkeley-db-max-objects
;;          Number of locked objects to reserve in the transaction environment
;;          Can be set to less, memory impact is pretty small though.
;;
;; :berkeley-db-mvcc
;;          Determines whether a BDB database is enabled for multiple version 
;;          concurrency controller (DB_MULTIVERSION) and transactions are 
;;          DB_SNAPSHOT by default.  This default can be overridden on a 
;;          per-transaction basis using the :mvcc argument to open-store and 
;;          :snapshot to with-transaction.
