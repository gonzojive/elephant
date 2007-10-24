;;; -*- Mode: Lisp; Syntax: ANSI-Common-Lisp; Base: 10 -*-
;;;
;;; os.lisp -- A set of convenience functions for cross-platform os functions 
;;;            that elephant requires
;;; 
;;; By Ian Eslick, <ieslick common-lisp net>
;;; 
;;; part of
;;;
;;; Elephant: an object-oriented database for Common Lisp
;;;
;;; Copyright (c) 2004 by Andrew Blumberg and Ben Lee
;;; <ablumberg@common-lisp.net> <blee@common-lisp.net>
;;;
;;; Portions Copyright (c) 2005-2007 by Robert Read and Ian Eslick
;;; <rread common-lisp net> <ieslick common-lisp net>
;;;
;;; Elephant users are granted the rights to distribute and use this software
;;; as governed by the terms of the Lisp Lesser GNU Public License
;;; (http://opensource.franz.com/preamble.html), also known as the LLGPL.
;;;

(in-package :elephant-utils)

(defmacro in-directory ((dir) &body body)
  `(progn 
     (#+sbcl sb-posix:chdir 
      #+cmu unix:unix-chdir
      #+allegro excl:chdir
      #+lispworks hcl:change-directory
      #+openmcl ccl:cwd
      ,dir)
     ,@body))

(defun launch-background-program (directory program &key (args nil))
  "Launch a program in a specified directory - not all shell interfaces
   or OS's support this"
  #+(and allegro (not mswindows))
  (multiple-value-bind (in out pid)
      (excl:run-shell-command (concat-separated-strings " " (list program) args)
			      :wait nil
			      :directory directory)
    (declare (ignore in out))
    pid)
  #+(and sbcl unix)
  (in-directory (directory)
    (sb-ext:run-program program args :wait nil))
  #+cmu 
  (in-directory (directory)
      (ext:run-program program args :wait nil))
  #+openmcl
  (in-directory (directory)
    (ccl:run-program program args :wait nil))
  #+lispworks
  (funcall #'sys::call-system
	 (format nil "~a~{ '~a'~} &" program args)
	 :current-directory directory
	 :wait nil)
  )

(defun kill-background-program (process-handle)
  #+(and allegro (not mswindows))
  (progn (excl.osi:kill process-handle 9)
	 (system:reap-os-subprocess :pid process-handle))
  #+(and sbcl unix)
  (sb-ext:process-kill process-handle 9)
  #+openmcl
  (ccl:signal-external-process process-handle 9)
;;  #+lispworks
;;  (apply #'sys::call-system
;;	 (format nil "kill ~A -9" process-handle)
;;	 :current-directory directory
;;	 :wait t)
  )


;; File stuff

(defun create-temp-filename (hint)
  (let ((directory (pathname-directory hint))
	(name (pathname-name hint))
	(type (pathname-type hint)))
    (loop for i from 1 to 10000 do
	 (let ((fname (make-pathname :directory directory :name (format nil "~A~A" name i) :type type)))
	   (when (not (probe-file fname))
	     (return fname))))))

(defun create-temp-dirname (hint)
  (let ((directory (pathname-directory hint)))
    (let ((dirname (first (last directory))))
      (loop for i from 1 upto 10000 do
	   (setf (car (last directory))
		 (format nil "~A~A" dirname i))
	   (let ((dirstring (make-pathname :directory directory)))
	     (when (not (probe-file dirstring))
	       (return (namestring dirstring))))))))

(defun copy-directory (source-dir target-dir)
  (ensure-directories-exist target-dir)
  #+allegro
  (excl:copy-directory source-dir target-dir :quiet t)
  #-allegro
  (loop for file in (directory source-dir) do
       (copy-file-to-dir file target-dir)))

(defun copy-file-to-dir (source-path target-dir)
  (let ((target-path (make-pathname :directory target-dir :name (pathname-name source-path) 
				    :type (pathname-type source-path))))
    (copy-file source-path target-path)))

(defun copy-file (source-path target-path)
  #+allegro
  (excl.osi:copy-file source-path target-path :overwrite t :if-does-not-exist :create :if-exists :overwrite)
  #-allegro
  (with-open-file (src source-path :direction :input :if-does-not-exist :error)
    (with-open-file (targ target-path :direction :output :if-exists :supersede
			  :if-does-not-exist :create)
      (handler-case 
	  (loop (write-byte (read-byte src :eof-error-p t) targ))
	(end-of-file () t)))))

