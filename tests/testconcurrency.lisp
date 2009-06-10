(in-package :ele-tests)

;; not enabled by default
(in-suite* testthreads)

;;;
;;; Leslie P. Polzer <leslie.polzer@gmx.net> 2008
;;; Alex Mizrahi <alex.mizrahi@gmail.com> 2008
;;;
;;; These tests will (as of March 2008) fail horribly on
;;;   * BDB without deadlock detection enabled.
;;;   * CLSQL/SQLite
;;;   * Probably other CLSQL backends
;;;
;;; The Postmodern backend should handle them correctly,
;;; and BDB as well (although I noticed a major slowdown).
;;;

;;; These tests imply quite strict levels of atomicity and transaction isolation:
;;; in no way concurrent transactions should affect each other, and in no way information
;;; should be lost due to concurrent updates.

(defvar *zork-count* 10)
(defvar *thread-count* 10)
(defvar *thread-batches* 1)
(defvar *thread-runs* 2)

(defun setup-zork (&key initially-zero (zork-count *zork-count*) (with-indices t))
  (wipe-class 'zork)

  (if with-indices 
      (defpclass zork ()
	((slot1 :initarg :slot1 :initform 0 :index t)
	 (slot2 :initarg :slot2 :initform 0 :index t)))
      (defpclass zork ()
	((slot1 :initarg :slot1 :initform 0)
	 (slot2 :initarg :slot2 :initform 0))))
  
  (loop for i from 0 below zork-count
	collect (let ((v (if initially-zero 0 i)))
		  (make-instance 'zork :slot1 v :slot2 v))))

(defun report-retry (condition sc)
  sc
  (format t "retrying txn due to:~a~%" condition))

(defmacro do-threaded-tests ((&key (thread-count 5))
			     &body body)
  "run computation being tested in multiple threads. *store-controller* gets automatically propagated.
   thread-count threads will be spawned. 
   returns two values -- first is one of errors occured in threads, or NIL if there were none.
   second is a list of results OR errors.

   thread-id from range [0, thread-count) is available inside thread body."
  `(let ((_sc *store-controller*) ; save controller into lexical var so closure can capture it
	 (_cv (bt:make-condition-variable))
	 (_lock (bt:make-lock))
	 _results _error)
    (bt:with-lock-held (_lock)
      
      (dotimes (thread-id ,thread-count) ; spawn threads
	(let ((thread-id thread-id)) ; rebind loop variable so it can be captured in closure
	  (declare (ignorable thread-id))
	  (bt:make-thread  
	   (lambda ()
	     (let ((*store-controller* _sc)
		   _my-result _my-error)
	       (unwind-protect 
		    (handler-case 
			(setf _my-result 
			    (progn ,@body))
		      ;; should we also look for other conditions?
		      (serious-condition (_e) (setf _my-error _e)))
		 (bt:with-lock-held (_lock)
		   (when _my-error
		   (setf _my-result _my-error
			 _error _my-error))
		   (push _my-result _results)
		   (bt:condition-notify _cv)
		   (format t "thread ~a notify sent~%" thread-id))))))))
      ;; now wait for threads to finish. if condvars are not good it can hang forever :(
      (loop while (< (length _results) ,thread-count)
	do (bt:condition-wait _cv _lock)))
    (values _error _results)))
  
(defmacro maybe-report-failure (block-name &body body)
  "if body returns object, it's reported as fail and execution goes out of block"
  `(let ((error (progn ,@body)))
    (if error
	(progn (fail "~a" error)
	       (return-from ,block-name error))
	(pass))))

(test threaded-idx-access
  "test verifies that reads and writes of indexed slots does not yield errors and are consistent.
 aditionally verifies transactional consistency of threaded operations."
  (block check-block
    (setup-zork)

    (dotimes (batch *thread-batches*)
      (maybe-report-failure check-block
	(do-threaded-tests (:thread-count *thread-count*)
	  (dotimes (i *thread-runs*)
	    (format t "thread ~A: batch ~A, run ~A~%" (bt:current-thread) batch i)
	    (dolist (obj (get-instances-by-class 'zork))
	      (format t "now handling obj ~A~%" obj)
	      (ele:with-transaction (:retry-cleanup-fn #'report-retry)
		;; check if obj can be found via index read
		(unless (member obj (get-instances-by-value 'zork 'slot1 (slot-value obj 'slot1)))
		  (error "Failed to find object via index")) ; 5am does not work in threads so we are using ad-hoc constructs
		
		(get-instance-by-value 'zork 'slot2 (slot-value obj 'slot2)) ; just check it does not signal errors
		
		(unless (= (slot-value obj 'slot1) (slot-value obj 'slot2))
		  (error "slot1 and slot2 are not equal in zork: ~a and ~a" (slot-value obj 'slot1) (slot-value obj 'slot2)))

		(setf (slot-value obj 'slot1) (random 50000)
		      (slot-value obj 'slot2) (slot-value obj 'slot1))))))))

    (let ((zorks (elephant::get-instances-by-class 'zork)))
      (is (= *zork-count* (length zorks)))
      (dolist (z zorks)
	(ele:with-transaction ()
	  (is (= (slot-value z 'slot1) (slot-value z 'slot2))) ; verify that both slots were transactionally updated
	  
	  ;; now verify that indices were not damaged -- retrieve zorks from indices and veryify that all is well.
	  (let ((same-zorks1 (ele:get-instances-by-value 'zork 'slot1 (slot-value z 'slot1)))
		(same-zorks2 (ele:get-instances-by-value 'zork 'slot2 (slot-value z 'slot2))))
	    (is (member z same-zorks1))
	    (is (member z same-zorks2))
	    (loop for z1 in same-zorks1 do (is (= (slot-value z1 'slot1) (slot-value z 'slot1))))
	    (loop for z2 in same-zorks2 do (is (= (slot-value z2 'slot2) (slot-value z 'slot2))))))))))

(test threaded-increments
  "performs slot increments in multiple threads and verifies that all increments get applied"
  (block check-block
    (loop for with-indices in (list nil t)
	  do (let ((zorks (setup-zork :initially-zero t :with-indices with-indices)))
	       (maybe-report-failure check-block
		 (do-threaded-tests (:thread-count 5)
		   (dotimes (run 5)
		     (dolist (z zorks)
		       (ele:with-transaction ()
			 (unless (= (slot-value z 'slot1) (slot-value z 'slot2))
			   (error "zork slots values do not match (pre)"))
			 (incf (slot-value z 'slot1))
			 (incf (slot-value z 'slot2))
			 (unless (= (slot-value z 'slot1) (slot-value z 'slot2))
			   (error "zork slots values do not match (post)")))))))

	       ;; verify that each zork slot was incremented to 25 (5 runs * 5 threads)
	       (loop for z in zorks
		     do (is (= 25 (slot-value z 'slot1))) 
		     do (is (= 25 (slot-value z 'slot2))))))))

(test (threaded-idx-hardcore :depends-on threaded-idx-access)
  "abusive number of threads (30) simultaneously trying to update poor single slot.
 formely known as 'provoke-deadlock'"
  (block check-block
    (setup-zork :zork-count 1)
  
    (maybe-report-failure check-block
      (do-threaded-tests (:thread-count 30)
	(let ((obj (car (get-instances-by-class 'zork))))
	  (setf (slot-value obj 'slot1) 42))))
    t))

(test cross-update-deadlock
  "update slots in criss-cross manner from two threads. 
should yield deadlock which should be retried."

  (block check-block
    (loop for with-indices in (list nil t)
	  do (let ((z (first (setup-zork :zork-count 1 :with-indices with-indices))))
	       (maybe-report-failure check-block 
		 (do-threaded-tests (:thread-count 2)
		   (with-transaction (:retry-cleanup-fn #'report-retry)
		     (cond
		       ((= thread-id 0) 
			(setf (slot-value z 'slot1) 2)
			(sleep 1)
			(setf (slot-value z 'slot2) 4))
		       ((= thread-id 1) 
			(sleep 0.5)
			(setf (slot-value z 'slot2) 7
			      (slot-value z 'slot1) 3))))))
	       
	       (let ((s1 (slot-value z 'slot1))
		     (s2 (slot-value z 'slot2)))
		 (is-true (or (and (= s1 2) (= s2 4))
			      (and (= s1 3) (= s2 7)))))))))
     
(test (threaded-random-order-increments :depends-on (and threaded-increments cross-update-deadlock))
  "update zorks in random order. this can produce deadlocks."
  (block check-block
    (loop for with-indices in (list nil t)
	  do (let ((zorks (setup-zork :initially-zero t :with-indices with-indices)))
	       
	       (maybe-report-failure check-block
		 (do-threaded-tests (:thread-count 5)
		   (dotimes (run 5)
		     (with-transaction (:retry-cleanup-fn #'report-retry)
		       (dotimes (i 3)
			 (incf (slot-value
				(nth (random 10) zorks) ; pick random zork
				'slot1))
			 (incf (slot-value
				(nth (random 10) zorks) ; pick random zork
				'slot2)))))
		   (format t "thread ~a finished~%" thread-id)))

	       (is (= (* 5 5 3 2)
		      (loop for z in zorks
			    summing (+ (slot-value z 'slot1)
				       (slot-value z 'slot2)))))))))

(defun test-threaded-object-creation (stable-bootstrap indexed) 
  (block check-block

    (setup-zork :zork-count (if stable-bootstrap 1 0) :initially-zero t)
    
    (maybe-report-failure check-block
      (do-threaded-tests (:thread-count 10)
	(make-instance 'zork :slot1 (1+ thread-id) :slot2 (1+ thread-id))))

    (is (= (if stable-bootstrap 11 10)
	   (length (get-instances-by-class 'zork))))
    (when indexed
      (loop for i from (if stable-bootstrap 0 1) to 10
	    for found = (get-instance-by-value 'zork 'slot1 i)
	    do (is-true found)
	    do (is (= i (slot-value found 'slot1)))))))

(test threaded-object-creation-1 
  "test creation of objects in threads: with initial object, w/o indices"
  (test-threaded-object-creation t nil))

(test (threaded-object-creation-1-i :depends-on threaded-object-creation-1)
  "test creation of objects in threads: with initial object, w/indices"
  (test-threaded-object-creation t t))
#|
(test threaded-object-creation-0
  "test creation of objects in threads: w/o initial object, w/o indices"
  (test-threaded-object-creation nil nil))

(test (threaded-object-creation-0-i :depends-on threaded-object-creation-0)
  "test creation of objects in threads: w/o initial object, w/indices"
  (test-threaded-object-creation nil t))
|#
