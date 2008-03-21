(in-package :ele-tests)

(in-suite* testassociations :in elephant-tests)

(defparameter NUM_JOBS 100)
(defparameter NUM_PERSONS 10000)
(defparameter PERSONS_PER_JOB 100)

;; Create a one-to-many association from job to person...
;; one job can have up to 10 persons, in any order.  
;; In this test I will do this with an explict association class,
;; and then maybe Ian and the Marc, the new guy, can beat it 
;; with an explicit association....
(test simple-explicit-assoc-setup
  (when (find-class 'person nil)
    (drop-instances (get-instances-by-class 'person) :txn-size 500))
  (when (find-class 'job nil)
    (drop-instances (get-instances-by-class 'job) :txn-size 500))
  (when (find-class 'pjassoc nil)
    (drop-instances (get-instances-by-class 'pjassoc) :txn-size 100))

 (defpclass person ()
   ((name :accessor names-of :initarg :name :index t)))

 (defpclass job ()
   ((title :accessor title-of :initarg :title :index t)
    (company :accessor company-of :initarg :company)))

 (defpclass pjassoc ()
   ((person :accessor person-of :initarg :person :index t)
    (job :accessor job-of :initarg :job :index t)))

 (let ((job-oids nil)
       (person-oids nil))
   (with-transaction ()
     (dotimes (x NUM_JOBS)
       (push (elephant::oid (make-instance 'job  :title (string (gensym)))) job-oids)))
   (dotimes (r (/ NUM_PERSONS 500))
     (with-transaction ()
       (dotimes (x 500)
	 (push (elephant::oid (make-instance 'person  :name (string (gensym)))) person-oids))))
   (dotimes (i NUM_JOBS)
     (with-transaction ()
       (dotimes (k PERSONS_PER_JOB)
	 (let ((p (nth (random NUM_PERSONS) person-oids))
	       (j (nth (random NUM_JOBS) job-oids)))
	   (make-instance 'pjassoc :person p :job j)))))
   (is (eq t t))))


(defun count-explicit-persons ()
  (flet ((find-all-persons-by-job (j)
	   (get-instances-by-value (find-class 'pjassoc) 'job (elephant::oid j))))
    (let ((total 0))
      (map-class #'(lambda (job) 
		     (incf total (length (find-all-persons-by-job job))))
		 (find-class 'job))
      total)))

(test (simple-explicit-assoc :depends-on simple-explicit-assoc-setup)
      (is (= (count-explicit-persons) (* NUM_JOBS PERSONS_PER_JOB))))



(test simple-slot-assoc-setup
  (when (find-class 'person nil)
    (drop-instances (get-instances-by-class 'person) :txn-size 500))
  (when (find-class 'job nil)
    (drop-instances (get-instances-by-class 'job) :txn-size 500))

 (defpclass person ()
   ((name :accessor names-of :initarg :name :index t)
    (jobs :accessor jobs :associate (job holders) :many-to-many t)))

 (defpclass job ()
   ((title :accessor title-of :initarg :title :index t)
    (company :accessor company-of :initarg :company)
    (holders :accessor holders :associate (person jobs) :many-to-many t)))

 (let ((jobs nil)
       (people nil))
   (with-transaction ()
     (dotimes (x NUM_JOBS)
       (push (make-instance 'job  :title (string (gensym))) jobs)))
   (dotimes (r (/ NUM_PERSONS 500))
     (with-transaction ()
       (dotimes (x 500)
	 (push (make-instance 'person  :name (string (gensym))) people))))

   (dotimes (i NUM_JOBS)
     (with-transaction ()
       (dotimes (k PERSONS_PER_JOB)
	 (let ((p (nth (+ k (* i PERSONS_PER_JOB)) people))
	       (j (nth i jobs)))
	   (setf (jobs p) j)))))
   (is (eq t t))))

(defun count-slot-persons ()
  (let ((total 0))
    (map-class #'(lambda (job) 
		   (incf total (length (holders job))))
	       'job)
    total))

(defun count-persons ()
  (let ((total 0))
    (map-class #'(lambda (person) (incf total)) 
	       'person :oids t)
    total))

(test (simple-slot-assoc :depends-on simple-slot-assoc-setup)
  (is (= (count-slot-persons) (* NUM_JOBS PERSONS_PER_JOB))))
  

(defun assoc-timing-comparison ()
  (when (find-class 'person nil)
    (drop-instances (get-instances-by-class 'person) :txn-size 500))
  (when (find-class 'job nil)
    (drop-instances (get-instances-by-class 'job) :txn-size 500))
  (when (find-class 'pjassoc nil)
    (drop-instances (get-instances-by-class 'pjassoc) :txn-size 100))

 (format t "~%~%Explicit assoc setup~%")
 (time (do-test 'simple-explicit-assoc-setup))
 (format t "~%~%Explicit assoc retrieve~%")
 (elephant::flush-instance-cache *store-controller*)
 (time (count-explicit-persons))

 (drop-instances (get-instances-by-class 'person) :txn-size 500)
 (drop-instances (get-instances-by-class 'job) :txn-size 500)

 (format t "~%~%Slot assoc setup~%")
 (time (do-test 'simple-slot-assoc-setup))
 (format t "~%~%Slot assoc retrieve~%")
 (elephant::flush-instance-cache *store-controller*)
 (time (count-slot-persons)))
