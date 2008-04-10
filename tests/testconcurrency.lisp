(in-package :ele-tests)

;; not enabled by default
;(in-suite* testthreads :in elephant-tests)

;;;
;;; Leslie P. Polzer <leslie.polzer@gmx.net> 2008
;;;
;;; These tests will (as of March 2008) fail horribly on
;;;   * BDB without deadlock detection enabled.
;;;   * CLSQL/SQLite
;;;   * Probably other CLSQL backends
;;;
;;; The Postmodern backend should handle them correctly,
;;; and BDB as well (although I noticed a major slowdown).
;;;


(defpclass zork ()
  ((slot1 :accessor slot1 :initarg :slot1 :initform nil :index t)
   (slot2 :accessor slot2 :initarg :slot2 :initform nil :index t)))


; A basic simulation of a web application using Elephant
; This is also a test showing whether database connections get cleaned up
; correctly.

#-sbcl
(test threaded-idx-access
  (dotimes (i 10)
    (make-instance 'zork :slot1 i :slot2 i))

  (dotimes (batch 20)
    (dotimes (i 5)
      (bt:make-thread (lambda ()
                        (dotimes (i 5)
                          (format t "thread ~A: batch ~A, run ~A~%" (bt:current-thread) batch i)
                          (dolist (obj (elephant::get-instances-by-class 'zork))
                            (format t "now handling obj ~A~%" obj)
                            (setf (slot-value obj 'slot1) i)
                            (setf (slot-value obj 'slot2) (slot-value obj 'slot1)))))))
    #+sbcl(dolist (thr (bt:all-threads))
            (format t "waiting for thread ~A to finish...~%" thr)
            (unless (eq thr (bt:current-thread))
              (sb-thread:join-thread thr)))
    (format t "batch finished!~%"))

  (drop-instances (get-instances-by-class 'zork))
  (format t "test finished!~%"))

#-sbcl
(test provoke-deadlock ;; sometimes throws a 23505 (primary key constraint violation)
                       ;; I have not tracked this down, yet.
  (dotimes (i 10)
    (make-instance 'zork :slot1 i :slot2 i))

  (dotimes (i 30)
    (bt:make-thread
      (lambda ()
        (format t "thread no ~A starting~%" i)
        (let ((obj (car (get-instances-by-class 'zork))))
          (setf (slot-value obj 'slot1) i)) ;; this only provokes deadlocks when
                                            ;; the slot in question is indexed.
        (format t "thread finished.~%"))))

  (drop-instances (get-instances-by-class 'zork)))

