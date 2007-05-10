(in-package :db-postmodern)

(defmethod persistent-slot-writer ((sc sql-store-controller) new-value instance name)
  (with-connection-for-thread (sc)
    (sql-add-to-root
     (form-slot-key (oid instance) name)
     new-value
     sc)))

(defmethod persistent-slot-reader ((sc sql-store-controller) instance name)
  (multiple-value-bind (v existsp)
      (with-connection-for-thread (sc)
        (sql-get-from-root
         (form-slot-key (oid instance) name)
         sc))
    (if existsp
	v
	(error  'unbound-slot :instance instance :name name))))

(defmethod persistent-slot-boundp ((sc sql-store-controller) instance name)
  (with-connection-for-thread (sc)
    (if (sql-from-root-existsp
         (form-slot-key (oid instance) name)
         sc )
        t nil)))

(defmethod persistent-slot-makunbound ((sc sql-store-controller) instance name)
  (with-connection-for-thread (sc)
    (sql-remove-from-root
     (form-slot-key (oid instance) name) 
     sc)
    instance))


;; if add-to-root is a method, then we can make it class dependent...
;; otherwise we have to change the original code.  There is 
;; almost no way to implement this without either changing the existing
;; file.  If we can introduce a layer of class indirectio there, then
;; we can control things properly.  In the meantime, I will implement
;; a proper method myself, but I will give it a name so it doesn't 
;; conflict with 'add-to-root.  'add-to-root can remain a convenience symbol,
;; that will end up calling this routine!
(defun sql-add-to-root (key value sc)
  (sql-add-to-clcn 0 key value sc))

(defun sql-add-to-clcn (clcn key value sc
			&key (insert-only nil))
  (sql-add-to-clcn-plpgsql clcn key value sc
                             :insert-only insert-only))

(defun sql-add-to-clcn-plpgsql (clcn key value sc
                                &key (insert-only nil))
  (assert (integerp clcn))
  (let ((vbs (serialize-to-base64-string value sc))
        (kbs (serialize-to-base64-string key sc)))
    (sp-ins-upd-keybase-valuebase (active-connection)
                                   (integer-to-string clcn)
                                   kbs
                                   vbs
                                   (if insert-only "TRUE" "FALSE")
                                   :row-reader 'cl-postgres:ignore-row-reader))
  value)

(defun sql-get-from-root (key sc)
  (sql-get-from-clcn 0 key sc))

;; This is a major difference betwen SQL and BDB:
;; BDB plans to give you one value and let you iterate, but
;; SQL by nature returns a set of values (when the keys aren't unique.)
;; 
;; I serious problem here is what to do if the things aren't unique.
;; According to the Elepahnt documentation, you should get one value 
;; (not clear which one, the "first" probably, and then use a 
;; cursor to iterate over duplicates.  
;; So although it is moderately clear how the cursor is supposed to 
;; work, I'm not sure how I'm supposed to know what value should be 
;; returend by this non-cursor function.
;; I suspect if I return the value that has the lowest OID, that will
;; match the behavior of the sleepycat function....
;; To do that I have to read in all of the values and deserialized them
;; This could be a good reason to keep the oids out, and separte, in 
;; a separate column.
(defun sql-get-from-clcn (clcn key sc)
  (assert (integerp clcn))
  (sql-get-from-clcn-nth clcn key sc 0))

(defun sql-get-from-clcn-nth (clcn key sc n)
  (assert (and (integerp clcn) (integerp n)))

  (let* ((con (active-connection))
	 (kbs 
	  (serialize-to-base64-string key sc))
         (tuples
          (sp-value-bob-by-clcn-keybase-nr con (integer-to-string clcn) kbs (integer-to-string n) :row-reader 'cl-postgres:list-row-reader)
           ))
    (if tuples
        (values (deserialize-from-database (caar tuples) sc)
                #+nil(deserialize-from-base64-string (caar tuples) sc)
                t)
        (values nil nil))))

(defun sql-from-root-existsp (key sc)
  (sql-from-clcn-existsp 0 key sc))

(defun sql-from-clcn-existsp (clcn key sc)
  (assert (integerp clcn))
  (let* ((con (active-connection))
	 (kbs (with-buffer-streams (out-buf)
		(serialize-to-base64-string key sc)))
	 (tuples (sp-key-clctn-exists (active-connection) (integer-to-string clcn) kbs)))
    (if tuples t nil)))


(defun sql-from-clcn-key-and-value-existsp (clcn key value sc)
  (assert (integerp clcn))
  (let* ((con (active-connection))
	 (kbs (serialize-to-base64-string key sc))
	 (vbs (serialize-to-base64-string value sc))
	 (tuples (sp-key-value-clctn-exists con (active-connection) (integer-to-string clcn) kbs vbs)))
    (if tuples t nil)))

(defun sql-remove-from-root (key sc)
  (sql-remove-from-clcn 0 key sc))

(defun sql-remove-from-clcn (clcn key sc)
  (assert (integerp clcn))
  (let ((con (active-connection))
	(kbs (serialize-to-base64-string key sc)))
    (sp-delete-by-clcn-key con (integer-to-string clcn) kbs :row-reader 'cl-postgres:ignore-row-reader)))


(defun sql-remove-key-and-value-from-clcn (clcn key value sc)
  (assert (integerp clcn))
  (let ((kbs (serialize-to-base64-string key sc))
        (vbs (serialize-to-base64-string value sc)))
    (sp-delete-by-clcn-key-value (active-connection) (integer-to-string clcn) kbs vbs :row-reader 'cl-postgres:ignore-row-reader)))

