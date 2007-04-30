;;
;; Naive translation of the Araneida blog example to Hunchentoot
;; Elephant Beta 0.6.1, Hunchentoot 0.7.2, SBCL 1.0.3 on Linux
;; 2007-04-21 18:22 Mac Chan
;; 
;; Requires :asdf :elephant :hunchentoot :cl-who and :net-telent-date
;; Load this file and start the server at the REPL
;; 
;; (hunchentoot-blog::start)
;; 
;; Then point your browser to
;; 
;; http://localhost:8080/blog
;; Or
;; http://localhost:8080/monthly-blog
;;
;; When you are done, shutdown hunchentoot and close the store with
;; 
;; (hunchentoot-blog::stop)
;;
;; Verdict - both elephant and hunchentoot are pretty cool ;-)
;;

(in-package #:cl-user)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (defun load-modules (&rest modules)
    (dolist (m modules) (asdf:oos 'asdf:load-op m)))
  (load-modules :elephant :hunchentoot
                :cl-who :net-telent-date))

(defpackage #:hunchentoot-blog
  (:use #:cl #:elephant #:hunchentoot
        #:cl-who #:net.telent.date))

(in-package :hunchentoot-blog)

(defvar *hunchentoot-server* nil
  "Hunchentoot server instance")

(defvar *blog-store-spec*
  '(:bdb
    #-(or mswindows win32)
    "/tmp/blogdb/"
    #+(or mswindows win32)
    "c:/temp/blogdb/"))

(defvar *my-blog* nil
  "A B-tree instance in the store that represents the blog.")

(defmacro with-html (&body body)
  `(with-html-output-to-string (*standard-output* nil :prologue nil)
     ,@body))

(defun the-month-year (u)
  "Strip sec, min, hour, day."
  (multiple-value-bind (s m h d mo y)
      (decode-universal-time u)
    (declare (ignore s m h d))
    (encode-universal-time 0 0 0 1 mo y)))
 
(defun this-month-year ()
  "Strip sec, min, hour, day from today."
  (the-month-year (get-universal-time)))

(defun get-month (u)
  "The numeric month of a universal time."
  (multiple-value-bind (s m h d mo)
      (decode-universal-time u)
    (declare (ignore s m h d))
    mo))

(defun get-year (u)
  "The numeric year of a universal time."
  (multiple-value-bind (s m h d mo y)
      (decode-universal-time u)
    (declare (ignore s m h d mo))
    y))

(defun index-by-month-year (s k v)
  (declare (ignore s k))
  (values t (the-month-year (entry-date v))))

(defun make-or-find-blogs (key)
  "Looks for the blog named 'key' in the DB, or else makes it."
  (let ((blogs (get-from-root key)))
    (unless blogs
      (with-transaction ()
	(setq blogs (make-indexed-btree))
	(add-index blogs :index-name 'month :populate nil
		   :key-form 'index-by-month-year)
	(add-to-root key blogs)))
    blogs))

(defclass blog-entry ()
  ((date :accessor entry-date :initarg :date)
   (title :accessor entry-title :initarg :title)
   (text :accessor entry-text :initarg :text))
  (:metaclass persistent-metaclass)
  (:documentation "A basic blog entry."))

(defun print-blog-entry-html (e)
  "Generates html for the blog (for use inside an html form). Returns
a string."
  (with-html
    (:div :class "entry"
          (:h2 (str (entry-title e)))
          (:h4 (str (universal-time-to-http-date (entry-date e))))
          (:p (str (entry-text e))))))

(define-easy-handler (show-blog :uri "/blog")
    ()
 "Displays all the blog entries."
 (with-html 
   (:html
    (:head (:title (str "A Common Lisp Blog"))
	   (:link :rel "stylesheet" :href "my-blog.css"))
     (:body 
      (:div
       :id "box"
       (:div :id "title"
            (:h1 (str "A Blog")))
       (:div :id "main"
            (:h2 (str "All Blog Entries"))
            (with-btree-cursor 
                (curs *my-blog*)
              (loop
               for (m k v) = (multiple-value-list (cursor-first curs))
               then (multiple-value-list (cursor-next curs))
               while m
               collect (print-blog-entry-html v) into entries
               finally (format t "~{~a~%~}" (nreverse entries))))))
      (:div :id "sidebar"
           (:a :href "edit-blog" (str "New Entry")))))))

(define-easy-handler (show-monthly-blog :uri "/monthly-blog")
    ((monthyear :parameter-type 'integer))
  "Displays all the blog entries in a month, given by the monthyear querystring."
  (let ((month-year (or monthyear (this-month-year))))
    (with-html 
      (:html
       (:head (:title (str "A Common Lisp Blog"))
              (:link :rel "stylesheet" :href "my-blog.css"))
       (:body 
        (:div
         :id "box"
         (:div :id "title"
              (:h1 (str "A Blog")))
         (:div :id "main"
              (:h2 (str "Entries from ")
                   (str (monthname nil (get-month month-year) nil nil)))
              ;; Dump all the blog entries from this month
              (with-btree-cursor 
                  (curs (get-index *my-blog* 'month))
                (loop
                 for (m k v) = (multiple-value-list
                                (cursor-set curs month-year))
                 then (multiple-value-list (cursor-next-dup curs))
                 while m
                 collect (print-blog-entry-html v) into entries
                 finally (format t "~{~a~%~}" (nreverse entries)))))
         (:div :id "sidebar"
              (:a :href "edit-blog" (str "New Entry"))
              (:h4 "Archives")
              ;; Dump all the months which have a blog entry
              (with-btree-cursor
                  (curs (get-index *my-blog* 'month))
                (loop
                 for (m k v) = (multiple-value-list (cursor-first curs))
                 then (multiple-value-list (cursor-next-nodup curs))
                 while m
                 collect 
                 (with-html
                   (:p :class "monthyear")
                   (:a :href (format nil "monthly-blog?monthyear=~A" 
                                     (the-month-year (entry-date v)))
                       (fmt "~A/~A" (get-month (entry-date v))
                            (get-year (entry-date v)))))
                 into pages
                 finally (format t "~{~a~%~}" (nreverse pages)))))))))))

(defun save-blog-entry (date title text)
  "Saves the blog entry indexed by date.  If there's a DB error,
return NIL, otherwise return the entry."
  (handler-case 
      (with-transaction ()
	(setf (get-value date *my-blog*) 
	      (make-instance 'blog-entry 
			     :date date
			     :title title
			     :text text)))
    (db-error () nil)))

(define-easy-handler (edit-blog :uri "/edit-blog")
    (date title text)
  "Displays the form to input new entries."
  (no-cache)
  (let* ((date (parse-time date))
         (posted-entry-message
          (when date
            (if (save-blog-entry date title text)
                "Entry Added!" "Failed to Save Entry!"))))
    (with-html 
      (:html
       (:head (:title "Edit Blog")
              (:link :rel "stylesheet" :href "my-blog.css"))
       (:body
        (:h1 (str (or posted-entry-message "Add Entry")))
        (if posted-entry-message
            (htm
             (:a :href "/blog" (str "Click here to return to my blog.")))
            (htm
             (:form :method "post"
                    (:p (str "Date:")
                        (:input :name "date" :type "text" :size "30"
                                :value (universal-time-to-http-date 
                                        (get-universal-time))))
                    (:p (str "Title:")
                        (:input :name "title" :type "text" :size "60"))
                    (:textarea :wrap "virtual" :name "text"
                               :rows "20" :cols "80"
                               (str ""))
                    (:p)
                    (:input :name "Save"
                            :value "Save" :type "submit")))))))))

(defun setup-dispatch-table ()
  (setq *dispatch-table*        
        (list (create-static-file-dispatcher-and-handler
               "/my-blog.css"
               (make-pathname
                :name "my-blog" :type "css"
                :version nil :defaults
                (load-time-value
                 (or #.*compile-file-pathname* *load-pathname*)))
               "text/css")
              'dispatch-easy-handlers
              ;; catch all
              (lambda (request)
                (declare (ignore request))
                (redirect "/blog"))
              #'default-dispatcher)))

(defun start (&optional (port 8080))
  "Opens the store, starts the server."
  (ensure-directories-exist (second *blog-store-spec*))
  (open-store *blog-store-spec*)
  (setq *my-blog* (make-or-find-blogs "blogs"))
  (when *hunchentoot-server*
    (stop-server *hunchentoot-server*))
  (setup-dispatch-table)
  (setq *hunchentoot-server*
        (start-server :port port)))

(defun stop ()
  "Stops the server, closes the store."
  (when *hunchentoot-server*
    (stop-server *hunchentoot-server*)
    (setq *hunchentoot-server* nil))
  (close-store))
