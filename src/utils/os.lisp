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

#+(and sbcl win32)
(eval-when (:compile-toplevel :load-toplevel :execute)
  (use-package :sb-alien))

#+(and sbcl win32)
(load-shared-object "Kernel32")

#+(and sbcl win32)
(declaim (inline SetCurrentDirectoryA))
#+(and sbcl win32)
(declaim (inline GetCurrentDirectoryA))

#+(and sbcl win32)
(define-alien-routine ("SetCurrentDirectoryA" SetCurrentDirectoryA) int
  (directory (* unsigned-char)))

#+(and sbcl win32)
(define-alien-routine ("GetCurrentDirectoryA" GetCurrentDirectoryA) unsigned-int
  (buffer-length unsigned-int)
  (buffer (* unsigned-char)))

#+(and sbcl win32)
(defun char-string->lisp-string (cstring &optional (length -1))
  "
  Arguments: /cstring/ is a pointer to a zero-terminated
    array of chars (unsigned bytes)
    /length/, if supplied, truncates the translation after
    /length/ number of characters if a zero-termination is not
    encountered before that
  Semantics: translates /cstring/ to a lisp string.  If
    /cstring/ is a null pointer, returns nil
  Returns: a lisp string or nil.
  "
  (declare (type integer length))
  (when (null-alien cstring)
    (return-from char-string->lisp-string nil))
  ;; calculate the length
  (when (minusp length)
    (do ((index 0 (incf index)))
        ((= (deref cstring index) 0) (setq length index))))
  (do* ((index 0 (incf index))
        (char (deref cstring index) (deref cstring index))
        (lisp-string (make-array length :element-type 'character)))
       ((or (= index length) (zerop char)) (subseq lisp-string 0 index))
    (declare (type integer index)
             (type (unsigned-byte 8) char)
             (type string lisp-string))
    (setf (aref lisp-string index) (code-char char))))

#+(and sbcl win32)
(defun lisp-string->char-string (string)
  "
  Semantics: converts the lisp /string/ into an alien pointer to
    a zero-terminated array of 8-bit chars.  Any
    characters in /string/ with a unicode code point
    > #xFF are given a representation as #x1A -- the
    standard unicode Substitute control
  Returns: Two values: an alien pointer to a zero-terminated array of
    8-bit chars suitable for passing to a Windows API;
    and the length of the string
  "
  (let* ((size (length string))
         (char-string (make-alien (unsigned 8) (1+ size)))
         (unicode 0)
         (char 0))
    (declare (type integer size unicode)
             (type (unsigned-byte 8) char))
    (dotimes (index size)
      (setq unicode (char-code (elt string index)))
      (when (> unicode #xFF) (setq unicode #x1A)) ; coerce to 8-bits
      (setq char (coerce unicode '(unsigned-byte 8)))
      (setf (deref char-string index) char))
    (setf (deref char-string size) 0)
    (return-from lisp-string->char-string
      (values char-string size))))

#+(and sbcl win32)
(defun get-cwd ()
  "
  Semantics: returns pathname for the current working (execution) directory.
    The current working directory is the directory the operating system
    believes the program is running in.  Shell commands execute here.
  Arguments:
  Returns: pathname of the current working directory
  "
  (let* ((size (GetCurrentDirectoryA 0 nil))
         (cwd (make-alien unsigned-char size)))
    (declare (type integer size))
    (GetCurrentDirectoryA size cwd)
    (char-string->lisp-string cwd size)
    (truename (char-string->lisp-string cwd size))))

#+(and sbcl win32)
(defun cwd (&optional dir)
  "
  Semantics: Set the current working directory to /dir/.
    If /dir/ is nil or omitted, get the current working directory.
  Arguments: /dir/ is a slash-terminated lisp string or directory
    pathspec
  Returns: the current working directory after execution
  Error Conditions: signals 'file-error if not possible
  "
  (when (null dir)
    (return-from cwd (get-cwd)))
  (let ((result (SetCurrentDirectoryA (lisp-string->char-string (namestring dir)))))
    (declare (type integer result))
    (if (zerop result)
        (error (make-condition 'file-error :pathname dir))
        (get-cwd))))

(defmacro in-directory ((dir) &body body)
  `(progn 
     (#+sbcl #-win32 sb-posix:chdir #+win32 cwd
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

