;;; testcollections.lisp
;;;
;;; part of
;;;
;;; Elephant: an object-oriented database for Common Lisp
;;;
;;; Copyright (c) 2004 by Andrew Blumberg and Ben Lee
;;; <ablumberg@common-lisp.net> <blee@common-lisp.net>
;;;
;;; Elephant users are granted the rights to distribute and use this software
;;; as governed by the terms of the Lisp Lesser GNU Public License
;;; (http://opensource.franz.com/preamble.html), also known as the LLGPL.
;;;

(in-package :ele-tests)

;;  Tests are best run as complete suites,
;;  ie. (do-test-spec 'collections-indexed)
;;  When running single tests, sometimes
;;  later tests may have affected some global state.
;;  This is not captured by the :depends-on parameter,
;;  so when running single tests be aware of this.
;;  
;;  However, since november 2007 a :before option
;;  was added to FiveAM darcs branch. Someday later
;;  maybe add :before dependencies were needed.


(in-suite* testcollections :in elephant-tests)

(in-suite* collections-basic :in testcollections)

(test basicpersistence
  (let ((x (gensym)))
    (add-to-root "x" x)
    ;; Clear instances
    (flush-instance-cache *store-controller*)
    ;; Are gensyms equal across db instantiations?
    ;; This forces a refetch of the object from db
    (is (equal (format nil "~A" x)
               (format nil "~A" (get-from-root "x"))))))

(test keysizetest
  (let ((key "0123456789"))
    ;; This should create a key of size 10* (2 ** x)
    ;; On SBCL and postmodern, this fails at 16...
    (dotimes (x 14)
      (setf key (concatenate 'string key key)))
    
    (format t "Testing length = ~A~%" (length key))
    (add-to-root key key)
    (setq rv (equal (format nil "~A" key)
                    (format nil "~A" (get-from-root key))))
    (remove-from-root key)
    (is-true rv)))

(test testoid
  (ele::next-oid *store-controller*)
  (let ((oid (ele::next-oid *store-controller*)))
    (is (< oid (ele::next-oid *store-controller*)))))

(defclass blob ()
  ((slot1 :accessor slot1 :initarg :slot1)
   (slot2 :accessor slot2 :initarg :slot2)))

(defmethod print-object ((blob blob) stream)
  (format stream "#<BLOB ~A ~A>" (slot1 blob) (slot2 blob)))

(defvar keys (loop for i from 1 to 1000 
		   collect (concatenate 'base-string "key-" (prin1-to-string i))))

(defvar objs (loop for i from 1 to 1000
		   collect (make-instance 'blob
					  :slot1 i
					  :slot2 (* i 100))))

(defvar bt)

(in-suite* collections-btree :in testcollections )

(test btree-make
  (5am:finishes (setq bt (make-btree *store-controller*))))

(test (btree-put :depends-on btree-make)
  (5am:finishes
    (with-transaction (:store-controller *store-controller*)
      (loop for obj in objs
         for key in keys
         do (setf (get-value key bt) obj)))))

(test (btree-get :depends-on (and btree-make btree-put))
  (is-true
   (loop for key in keys
      for i from 1 to 1000
      for obj = (get-value key bt)
      always
      (and (= (slot1 obj) i)
           (= (slot2 obj) (* i 100))))))

(defvar first-key (first keys))


;; For some unkown reason, this fails on my server unless
;; I put the variable "first-key" here rather than use the string
;; "key-1".  I need to understand this, but don't at present....
(test (remove-kv :depends-on btree-get)
  (5am:finishes 
    (with-transaction (:store-controller *store-controller*)
      (remove-kv first-key bt)))
  (is-true (not (get-value first-key bt))))

;; (deftest removed) moved into remove-kv

(test (map-btree :depends-on remove-kv)
  (let ((ks nil)
        (vs nil))
    (flet ((mapper (k v) (push k ks) (push v vs)))
      (map-btree #'mapper bt))
    (is-true (and (subsetp ks (cdr keys) :test #'equalp) 
                  (subsetp (cdr keys) ks :test #'equalp)))))

(test (btree-cursor :depends-on (and remove-kv map-btree))
  (with-transaction (:store-controller *store-controller*)
    (with-btree-cursor (cur bt)
      (cursor-set cur "key-100")
      (multiple-value-bind (has k v)
	  (cursor-current cur)
	(is-true (and has (= (slot1 v) 100))))
      (multiple-value-bind (has k v)
	  (cursor-next cur)
	(is-true (and has (string= k "key-1000") (= (slot1 v) 1000))))
      (multiple-value-bind (has k v)
	  (cursor-prev cur)
	(is-true (and has (= (slot1 v) 100))))
      (multiple-value-bind (has k v)
	  (cursor-last cur)
	(is-true (and has (string= k "key-999") (= (slot1 v) 999))))
      (multiple-value-bind (has k v)
	  (cursor-set-range cur "key-1001")
	(is-true (and has (string= k "key-101") (= (slot1 v) 101)))
;; ISE: 10/25/07
;; Get both relies on equality for the value, which is a standard object
;; and standard objects fetched at two different times are not eq nor equal.
;; We could implement a deep, value-oriented equality but I'm not sure that's
;; a good idea, especially for CL-SQL performance.  (This works on BDB due
;; to the comparison happening lexically inside BDB)
;;
;;	(multiple-value-bind (has k2 v2)
;;	    (cursor-get-both cur "key-101" v)
;;	  (is-true (and has (string= k2 "key-101") (= (slot1 v2) 101)))
;;	  (multiple-value-bind (has k3 v3)
;;	      (cursor-get-both-range cur "key-101" v)
;	    (is-true (and has (string= k3 "key-101") (= (slot1 v3) 101)))))))))
	))))

(test (map-btree-remove :depends-on (and btree-make btree-cursor map-btree))
  (flet ((mapper (k v) 
	   (declare (ignore k))
	   (when (and (>= (slot1 v) 100) (< (slot1 v) 200))
	     (remove-current-kv))))
    (map-btree #'mapper bt)
    (is-false (get-value "key-100" bt))
    (is-false (get-value "key-199" bt))
    (is-false (get-value "key-150" bt))
    (is (= (slot1 (get-value "key-200" bt)) 200))
    (is (= (slot1 (get-value "key-99" bt)) 99))))

;;------------------------------------------------------------------------------

(in-suite* collections-indexed :in testcollections )

;; I hate global variables!  Yuck!
(defvar indexed)
(defvar index1)
(defvar index2)

(test indexed-btree-make
  (5am:finishes (with-transaction (:store-controller *store-controller*)
                  (setq indexed (make-indexed-btree *store-controller*)))))

(defun key-maker (s key value)
  (declare (ignore s key))
  (values t (slot1 value)))

(test (add-indices :depends-on indexed-btree-make)
  (5am:finishes
    (with-transaction (:store-controller *store-controller*)
      (setf index1
            (add-index indexed :index-name 'slot1 :key-form 'key-maker))
      (setf index2
            (add-index indexed :index-name 'slot2
                       :key-form '(lambda (s key value) 
                                   (declare (ignore s key))
                                   (values t (slot2 value))))))))

;; ISE NOTE: indices accessor is not portable across backends in current
;; system so I'm using alternate access (map-indices) instead
(deftest (test-indices :depends-on add-indices)
    (values
     ;; (= (hash-table-count (indices indexed)) 2)
     (let ((count 0))
       (map-indices (lambda (x y) (declare (ignore x y)) (incf count)) indexed)
       (eq count 2))
     ;; (gethash 'slot1 (indices indexed)))
     (eq index1 (get-index indexed 'slot1))
     ;; (eq index2 (gethash 'slot2 (indices indexed))))
     (eq index2 (get-index indexed 'slot2)))
  t t t)

#|
(deftest safe-indexed-put
    (finishes
     (loop for i from 1 to 1000
	   for obj in objs
	   for key in keys
	   do
	   (setf (get-value key indexed) obj)
	   (loop for j from 1 to i
		 for key2 in keys
		 for obj2 = (get-value key2 indexed)
		 always
		 (and (= (slot1 obj2) j)
		      (= (slot2 obj2) (* j 100))))))
  t) |#
   
(test (indexed-put :depends-on add-indices)
  (5am:finishes
    (with-transaction (:store-controller *store-controller*)
      (loop for obj in objs
         for key in keys
         do (setf (get-value key indexed) obj)))))

(test (indexed-get :depends-on indexed-put)
  (is-true
   (loop for key in keys
      for i from 1 to 1000
      for obj = (get-value key indexed)
      always 
      (and (= (slot1 obj) i)
           (= (slot2 obj) (* i 100))))))

(test (simple-slot-get :depends-on indexed-put)
  (setf (get-value (nth 0 keys) indexed)
        (nth 0 objs)) ;; Henrik comment 20070720: Why? 
  (let ((obj (get-value 1 index1)))
    (is (= (slot1 obj) 1))
    (is (= (slot2 obj) (* 1 100)))))

(test (indexed-get-from-slot1 :depends-on indexed-put)
  (is-true
   (loop with index = (get-index indexed 'slot1)
      for i from 1 to 1000
      for obj = (get-value i index)
      always
      (= (slot1 obj) i))))
	  
(test (indexed-get-from-slot2 :depends-on indexed-put)
  (is-true
   (loop with index = (get-index indexed 'slot2)
      for i from 1 to 1000
      for obj = (get-value (* i 100) index)
      always
      (= (slot2 obj) (* i 100)))))

(test (remove-kv-tests :depends-on (and indexed-put indexed-get simple-slot-get
					indexed-get-from-slot1 indexed-get-from-slot2))
  (5am:finishes (remove-kv first-key indexed))
  (is-false (get-value first-key indexed))
  (is-false (get-primary-key 1 index1))
  (is-false (get-primary-key 100 index2))
  ;; Remove from slot-1
  (5am:finishes (remove-kv 2 index1))
  ;; No key-nor-indices-slot1
  (is-false (get-value (second keys) indexed))
  (is-false (get-primary-key 2 index1))
  (is-false (get-primary-key 200 index2))
  ;; Remove from slot2
  (5am:finishes (remove-kv 300 index2))
  ;; No key-nor-indices-slot2
  (is-false (get-value (third keys) indexed))
  (is-false (get-primary-key 3 index1))
  (is-false (get-primary-key 300 index2)))


(deftest (map-indexed :depends-on (and btree-make remove-kv-tests))
    (let ((ks nil)
	  (vs nil))
      (flet ((mapper (k v) (push k ks) (push v vs)))
	(map-btree #'mapper indexed))
      (values
       (and (subsetp ks (cdddr keys) :test #'equalp) 
	    (subsetp (cdddr keys) ks :test #'equalp))))
  t)

;; This is "4" below because the first three keys are removed by remove-kv-tests

(deftest (get-first :depends-on remove-kv-tests)
    (with-transaction (:store-controller *store-controller*)
      (with-btree-cursor (c index1)
	(multiple-value-bind (has k v)
	    (cursor-first c)
	  (declare (ignore has v))
	  (= k 4))))
  t)

(deftest (get-first2 :depends-on remove-kv-tests)
    (with-transaction (:store-controller *store-controller*)
      (with-btree-cursor (c index2)
	(multiple-value-bind (has k v)
	    (cursor-first c)
	  (declare (ignore has v))
	  (= k 400))))
  t)

(deftest (get-last :depends-on remove-kv-tests)
    (with-transaction (:store-controller *store-controller*)
      (with-btree-cursor (c index1)
	(multiple-value-bind (has k v)
	    (cursor-last c)
	  (declare (ignore has v))
	  (= k 1000))))
  t)

(deftest (get-last2 :depends-on remove-kv-tests)
    (with-transaction (:store-controller *store-controller*)
      (with-btree-cursor (c index2)
	(multiple-value-bind (has k v)
	    (cursor-last c)
	  (declare (ignore has v))
	  (= k 100000))))
  t)

(deftest (set :depends-on remove-kv-tests)
    (with-transaction (:store-controller *store-controller*)
      (with-btree-cursor (c index1)
	(multiple-value-bind (has k v)
	    (cursor-set c 200)
	  (declare (ignore has k))
	  (= (slot1 v) 200))))
  t)

(deftest (set2 :depends-on remove-kv-tests)
    (with-transaction (:store-controller *store-controller*)
      (with-btree-cursor (c index2)
	(multiple-value-bind (has k v)
	    (cursor-set c 500)
	  (declare (ignore has k))
	  (= (slot2 v) 500))))
  t)

(deftest (set-range :depends-on remove-kv-tests)
    (with-transaction (:store-controller *store-controller*)
      (with-btree-cursor (c index1)
	(multiple-value-bind (has k v)
	    (cursor-set-range c 199.5)
	  (declare (ignore has k))
	  (= (slot1 v) 200))))
  t)

(deftest (set-range2 :depends-on remove-kv-tests)
    (with-transaction (:store-controller *store-controller*)
      (with-btree-cursor (c index2)
	(multiple-value-bind (has k v)
	    (cursor-set-range c 501)
	  (declare (ignore has k))
	  (= (slot2 v) 600))))
  t)

(deftest (map-indexed-index :depends-on (and set set2 set-range set-range2 map-indexed))
    (let ((sum 0))
      (flet ((collector (key value pkey)
	       (declare (ignore key pkey))
	       (incf sum (slot1 value))))
	(map-index #'collector index1 :start nil :end 10)
	(map-index #'collector index1 :start 990 :end nil)
	(map-index #'collector index1 :start 400 :end 410))
      sum)
  #.(+ 49 ;; sum 4-10 inclusive (1-3 removed by here)
       4455 ;; sum 690-700 inclusive
       10945 ;; sum 990 to 1000 inclusive
       ))

(deftest (map-index-from-end :depends-on (and set set2 set-range set-range2 map-indexed))
    (let ((sum 0))
      (flet ((collector (key value pkey)
	       (declare (ignore key pkey))
	       (incf sum (slot1 value))))
	(map-index #'collector index1 :start nil :end 10 :from-end t)
	(map-index #'collector index1 :start 990 :end nil :from-end t)
	(map-index #'collector index1 :start 400 :end 410 :from-end t))
      sum)
  #.(+ 49 ;; sum 4-10 inclusive (1-3 removed by here)
       4455 ;; sum 690-700 inclusive
       10945 ;; sum 990 to 1000 inclusive
       ))

(test (map-index-value-param :depends-on map-indexed-index)
  (let ((sum 0))
    (flet ((collector (key value pkey)
	     (declare (ignore key pkey))
             (incf sum (slot1 value))))
      (map-index #'collector index1 :value 990))
    (is (= sum 990))))

(test (map-index-collect-param :depends-on  map-indexed-index)
  (flet ((returner (key value pkey)
           (slot1 value)))
    (is (equal (map-index #'returner index1 :value 990 :collect t)
               (list 990)))))

(test (map-indexed-remove :depends-on (and indexed-btree-make test-indices map-indexed-index))
  (flet ((mapper (k v)
	   (declare (ignore k))
	   (when (and (> (slot1 v) 100) (< (slot1 v) 110))
	     (remove-current-kv))))
    (map-btree #'mapper indexed)
    (is-false (get-value 101 index1))
    (is-false (get-value 10100 index2))
    (is-false (get-value "key-101" indexed))
    (is (= (slot1 (get-value "key-110" indexed)) 110))))

(test (map-index-remove :depends-on map-indexed-remove)
  (flet ((mapper (k v pk) 
	   (declare (ignore v pk))
	   (when (and (>= k 110) (< k 115))
	     (remove-current-kv))))
    (map-index #'mapper index1)
    (is-false (get-value 110 index1))
    (is-false (get-value 11000 index2))
    (is-false (get-value "key-110" indexed))
    (is-false (get-value "key-114" indexed))
    (is-false (get-value 114 index1))
    (is-false (get-value 11400 index2))
    (is-false (get-value "key-112" indexed))
    (is (= (slot1 (get-value "key-115" indexed)) 115))
    (is (= (slot1 (get-value "key-99" indexed)) 99))))

(deftest rem-kv 
    (with-transaction (:store-controller *store-controller*)
      (let ((ibt (make-indexed-btree *store-controller*)))
	(loop for i from 0 to 10
	      do
	      (setf (get-value i ibt) (* i i)))
	(remove-kv 0 ibt)
	(remove-kv 1 ibt)
	(remove-kv 10 ibt)
	(equal (list 
		(get-value 0 ibt)
		(get-value 1 ibt)
		(get-value 10 ibt)
		(get-value 5 ibt)
		)
	       '(nil nil nil 25))
	))
  t
  )

(defun odd (s k v)
  (declare (ignore s k))
  (values t (mod v 2)))

(defun twice (s k v)
  (declare (ignore s k))
  (values t (* v 2)))

(defun half-floor (s k v)
  (declare (ignore s v))
  (values t (floor (/ k 2))))

(deftest rem-idexkv 
    (with-transaction (:store-controller *store-controller*)
    (let* ((ibt (make-indexed-btree *store-controller*))
	   (id1 (add-index ibt :index-name 'idx1 :key-form 'odd)))
      (loop for i from 0 to 10
	 do
	 (setf (get-value i ibt) (* i i)))

      (with-btree-cursor (c id1)
	(cursor-first c)
	(dotimes (i 10)
	  (multiple-value-bind (has key value)
	      (cursor-next c)
	    ))
	)
      (remove-kv 4 ibt)
      (remove-kv 5 ibt)

      (equal (list
       (get-value 4 ibt)
       (get-value 5 ibt)
       (get-value 6 ibt)
       (with-btree-cursor (c ibt)
	 (cursor-first c)
	 (dotimes (i 4)
	   (multiple-value-bind (has key value)
	     (cursor-next c)
	   value))
       (multiple-value-bind (has key value)
	   (cursor-next c)
	 value
	 )
	 ))
	     '(nil nil 36 49)
      )))
    t
  )

(defun index-cdr (bt pk val)
  (declare (ignore bt pk))
  (values t (cdr val)))

(deftest test-indexed-character-values
    (with-transaction (:store-controller *store-controller*)
      (let ((ibt (make-indexed-btree *store-controller*)))
	(add-index ibt :index-name 'name :key-form 'index-cdr)
	(setf (get-value 1 ibt) (cons 1 #\A))
	(setf (get-value 2 ibt) (cons 2 #\B))
	(setf (get-value 3 ibt) (cons 3 #\C))
	(length (map-index (lambda (k v pk)
			     (declare (ignore v pk))
			     k)
			   (get-index ibt 'name)
			   :start #\B
			   :end #\C
			   :collect t))))
  2)

(in-suite* indexed2-tests :in testcollections)

(defvar indexed2)
(defvar index3)

(deftest make-indexed2
    (finishes (with-transaction (:store-controller *store-controller*)
		(setq indexed2 (make-indexed-btree *store-controller*))))
  t)

(defun crunch (s k v)
  (declare (ignore s k))
  (values t (floor (/ (- v) 10))))

(deftest (add-indices2 :depends-on make-indexed2)
    (finishes
      (with-transaction (:store-controller *store-controller*) 
	(setq index3
	      (add-index indexed2 :index-name 'crunch :key-form 'crunch))))
  t)

(deftest (put-indexed2 :depends-on add-indices2)
    (finishes
      (with-transaction (:store-controller *store-controller*) 
	(loop for i from 0 to 10000
	      do
	      (setf (get-value i indexed2) (- i)))))
  t)

(deftest (get-indexed2 :depends-on put-indexed2)
    (loop for i from 0 to 10000
	  always (= (- i) (get-value i indexed2)))
  t)

(deftest (get-from-index3 :depends-on put-indexed2)
    (let ((v))
;;    (trace get-value)
;;    (trace crunch)
    (unwind-protect 
    (setf v (loop for i from 0 to 1000
;;	  always (= (- i) (floor (/ (get-value i index3) 10)))))
	  always 
	  (multiple-value-bind (bool res)
	      (crunch nil nil (get-value i index3))
	    (= res i))))
;;    (untrace))
      )
    v)
  t)

(deftest (dup-test :depends-on put-indexed2)
    (with-transaction (:store-controller *store-controller*)
      (with-btree-cursor (curs index3)
	(null (set-difference
	       (loop for (more k v) = (multiple-value-list
				       (cursor-first curs))
		  then (multiple-value-list (cursor-next-dup curs))
		  while more
		  collect v)
	       '(0 -1 -2 -3 -4 -5 -6 -7 -8 -9)))))
  t)

(deftest (nodup-test :depends-on put-indexed2)
    (with-transaction (:store-controller *store-controller*)
      (with-btree-cursor (curs index3)
	(let ((pk nil))
	  (loop for (m k v) = (multiple-value-list (cursor-next-nodup curs))
	     for i from 0 downto -9990 by 10
	     while m
	     always (prog1 
			(and (or (not pk) (not (= pk k)))
			     (and (<= v i) (> v (- i 10))))
		      (setf pk k))))))
  t)

(deftest (prev-nodup-test :depends-on put-indexed2)
    (with-transaction (:store-controller *store-controller*)
      (with-btree-cursor (curs index3)
        (cursor-last curs)
;;          (assert (= -10000 v) nil "precondition for this test is wrong (~a), check dependencies between tests" v))
	(let ((pk nil))
	(loop for (m k v) = (multiple-value-list (cursor-prev-nodup curs))
	      for i from -9999 to -9 by 10
	      while m
	      always (prog1
			(and (or (not pk) (not (= pk k)))
			     (and (>= v i) (< v (+ i 10))))
		       (setf pk k))))))
  t)

(deftest (pnodup-test :depends-on put-indexed2)
    (with-transaction (:store-controller *store-controller*)
      (with-btree-cursor (curs index3)
	(let ((pk nil))
	(loop for (m k v p) = (multiple-value-list (cursor-pnext-nodup curs))
	      for i from 0 to 9990 by 10
	      while m
	      always (prog1
			 (and (or (not pk) (not (= pk k)))
			      (and (>= p i) (< v (+ i 10))))
		       (setf pk k))))))
  t)

(deftest (pprev-nodup-test :depends-on put-indexed2)
    (with-transaction (:store-controller *store-controller*)
      (with-btree-cursor (curs index3)
	(cursor-last curs)
;;          (assert (= -10000 v) nil "precondition for this test is wrong, check dependencies between tests"))
	(let ((pk nil))
	  (loop for (m k v p) = (multiple-value-list (cursor-pprev-nodup curs))
	     for i from 9999 downto 9 by 10
	     while m
	     always (prog1
			(and (or (not pk) (not (= pk k)))
			     (and (<= p i) (> p (- i 10))))
		      (setf pk k))))))
  t)

(deftest (cur-del1 :depends-on (and put-indexed2 get-indexed2 get-from-index3 dup-test
				    nodup-test prev-nodup-test pnodup-test pprev-nodup-test))
			
    (with-transaction (:store-controller *store-controller*)
      (let* ((ibt (make-indexed-btree *store-controller*))
	     (id1 (add-index ibt :index-name 'idx1 :key-form 'odd)))
	(labels ((deleted (key others)
		   (and (null (get-value key ibt))
			(every #'(lambda (k2)
				   (= (get-value k2 ibt) (* k2 k2)))
			       others))))
	  (loop for i from 0 to 5 do
	       (setf (get-value i ibt) (* i i)))
	  (with-btree-cursor (c id1)
	    (cursor-last c)
	    (cursor-delete c))
	  (or (deleted 5 '(3 1))
	      (deleted 3 '(5 1))
	      (deleted 1 '(5 3))))))
  t)

(deftest (indexed-delete :depends-on cur-del1) 
    (finishes
      (with-transaction (:store-controller *store-controller*)
	(with-btree-cursor (curs index3)
	  (cursor-last curs)
	  (cursor-delete curs))))
  t)

(deftest (test-deleted :depends-on indexed-delete)
    (values
     (get-value 10000 indexed2)
     (get-value 1000 index3))
  nil nil)
		      
(deftest (indexed-delete2 :depends-on test-deleted)
    (finishes
      (with-transaction (:store-controller *store-controller*)
	(with-btree-cursor (curs index3)
	  (cursor-first curs)
	  (cursor-next-dup curs)
	  (cursor-delete curs))))
  t)

(deftest (test-deleted2 :depends-on indexed-delete2)
    (values
     (get-value 0 indexed2)
     (get-value 0 index3)
     (get-value 1 indexed2)
     (with-btree-cursor (c index3)
       (cursor-first c)
       (multiple-value-bind (m k v) (cursor-next c)
	 v)))
  0 0 nil -2)


(deftest (cur-del2 :depends-on test-deleted2) 
    (with-transaction (:store-controller *store-controller*)
      (let* ((ibt (make-indexed-btree *store-controller*))
	     (id1 (add-index ibt :index-name 'idx1 :key-form 'half-floor)))
	(loop for i from 0 to 10
	   do
	     (setf (get-value i ibt) (* i i)))
	(with-btree-cursor (c id1)
	  (cursor-first c)
	  (cursor-next-dup c)
	  (cursor-delete c)
	  )
	(or (and (null (get-value 1 ibt))
		 (eq (get-value 0 ibt) 0))
	    (and (null (get-value 0 ibt))
		 (eq (get-value 1 ibt) 1)))))
  t)



(deftest (get-both :depends-on cur-del2) 
    (with-btree-cursor (c indexed2)
      (cursor-get-both c 200 -200))
  t 200 -200)

(deftest (pget-both :depends-on cur-del2) 
    (with-btree-cursor (c index3)
      (multiple-value-bind (m k v p)
	  (cursor-pget-both c 10 107)
	(values k v p)))
  10 -107 107)

(deftest (pget-both-range :depends-on cur-del2)
    (with-btree-cursor (c index3)
      (multiple-value-bind (m k v p)
	  (cursor-pget-both-range c 10 106.5)
	(values k v p)))
  10 -107 107)

(defmacro pcursor-pkey (form)
  `(multiple-value-bind (m k v p)
    ,form
    (declare (ignore m k v))
    p))

(defmacro pcursor-value (form)
  `(multiple-value-bind (m k v p)
       ,form
     (declare (ignore m k p))
     v))

(defmacro pcursor-key (form)
  `(multiple-value-bind (m k v p)
       ,form
     (declare (ignore m v p))
     k))

(deftest (pcursor :depends-on cur-del2)
    (with-btree-cursor (c index3)
      (values
       (pcursor-pkey (cursor-pfirst c))
       (pcursor-pkey (cursor-pnext c))
       (pcursor-pkey (cursor-pnext-nodup c))

       (pcursor-pkey (cursor-pnext-dup c))
       (pcursor-pkey (cursor-pprev c))
       (pcursor-pkey (cursor-pprev-nodup c))

       (pcursor-pkey (cursor-plast c))
       (pcursor-pkey (cursor-pset c 300))
       (pcursor-pkey (cursor-pset-range c 199.5))

       (pcursor-pkey (cursor-pget-both c 10 101))
       (pcursor-pkey (cursor-pget-both-range c 11 111.4))))
      
  0 2 10 11 10 9 9999 3000 2000 101 112)

(defvar index4)

(deftest (newindex :depends-on cur-del2)
    (finishes
     (with-transaction (:store-controller *store-controller*) 
       (setq index4
	     (add-index indexed2 :index-name 'crunch :key-form 'crunch
			:populate t))))
  t)

(test (pcursor2 :depends-on newindex)
    (with-btree-cursor (c index4)
      (is (= 0 (pcursor-pkey (cursor-pfirst c))))
      (is (= 2 (pcursor-pkey (cursor-pnext c))))
      (is (= 10 (pcursor-pkey (cursor-pnext-nodup c))))
      (is (= 11 (pcursor-pkey (cursor-pnext-dup c))))
      (is (= 10 (pcursor-pkey (cursor-pprev c))))
      (is (= 9 (pcursor-pkey (cursor-pprev-nodup c))))
      (is (= 9999 (pcursor-pkey (cursor-plast c))))
      (is (= 3000 (pcursor-pkey (cursor-pset c 300))))
      (is (= 3010 (pcursor-pkey (cursor-pnext-nodup c))))      
      (is (= 2000 (pcursor-pkey (cursor-pset-range c 199.5))))
      (is (= 101 (pcursor-pkey (cursor-pget-both c 10 101))))
      (is (= 112 (pcursor-pkey (cursor-pget-both-range c 11 111.4))))))

(defvar index-string)

(defun crunch-string (s k v)
  (multiple-value-bind (index? value)
      (crunch s k v)
    (values index? (princ-to-string value))))

(test (crunch-as-string :depends-on pcursor2)
    (5am:finishes
     (with-transaction (:store-controller *store-controller*) 
       (setq index-string
	     (add-index indexed2 :index-name 'crunch-string
                        :key-form 'crunch-string
			:populate t)))))

(test (pcursor2-on-string :depends-on crunch-as-string)
    (with-btree-cursor (c index-string)
      (is (= 0 (pcursor-pkey (cursor-pfirst c))))
      (is (= 2 (pcursor-pkey (cursor-pnext c))))
      (is (= 10 (pcursor-pkey (cursor-pnext-nodup c))))
      (is (= 11 (pcursor-pkey (cursor-pnext-dup c))))
      (is (= 10 (pcursor-pkey (cursor-pprev c))))
      (is (= 9 (pcursor-pkey (cursor-pprev-nodup c))))
      (is (= 9999 (pcursor-pkey (cursor-plast c))))
      (is (= 3000 (pcursor-pkey (cursor-pset c "300"))))
      (is (= 3010 (pcursor-pkey (cursor-pnext-nodup c))))
      (is (= 3000 (pcursor-pkey (cursor-pset-range c "300"))))))


(in-suite* from-and-to-root :in testcollections)

(deftest add-get-remove
    (let ((r1 '())
	  (r2 '()))
      (add-to-root "x1" "y1")
      (add-to-root "x2" "y2")
      (setf r1 (get-from-root "x1"))
      (setf r2 (get-from-root "x2"))
      (remove-from-root "x1")
      (remove-from-root "x2")
      (and 
       (equal "y1" r1)
       (equal "y2" r2)
       (equal nil (get-from-root "x1"))
       (equal nil (get-from-root "x2"))
       ))
  t)

(deftest add-get-remove-symbol
    (let ((foo (cons nil nil))
	  (bar (cons 'a 'b))
	  (f1 '())
	  (f2 '())
	  (b1 '())
	  (b2 '()))
      (add-to-root "my key" foo)
      (add-to-root "my other key" foo)
      (setf f1 (get-from-root "my key"))
      (setf f2 (get-from-root "my other key"))
      (add-to-root "my key" bar)
      (add-to-root "my other key" bar)
      (setf b1 (get-from-root "my key"))
      (setf b2 (get-from-root "my other key"))	
      (and 
       (equal f1 f2)
       (equal b1 b2)
       (equal f1 foo)
       (equal b1 bar)))
  t)

(deftest existsp
    (let ((exists1 '())
	  (exists2 '())
	  (exists3 '())
	  (key "my key"))
      (remove-from-root key)
      (setf exists1 (root-existsp key))
      (add-to-root key 'a)
      (setf exists2 (root-existsp key))
      (remove-from-root key)
      (setf exists3 (root-existsp key))
      (values exists1 exists2 exists3))
  nil t nil
  )



(defparameter test-items '(1 2 3 (1) (2) test1 test2))

(deftest pset
    (let ((pset1 (make-pset)))
      (mapc (lambda (item)
	      (insert-item item pset1))
	    test-items)
      (remove-item (list 2) pset1)
      (remove-item 'test2 pset1)
      (let ((list (pset-list pset1)))
	(values
	 (= (length (pset-list pset1)) 5)
	 (not (find-item 'test2 pset1))
	 (is-not-null (find-item 'test1 pset1))
	 (is-not-null (find-item 1 pset1 :key (lambda (x) (when (consp x) (car x))) :test #'eq)))))
  t t t t)
	  

;; This test not only does not work, it appears to 
;; hang BDB forcing a recovery!?!?!?!
;; (deftest cursor-put
;;     (let* ((ibt (make-indexed-btree *store-controller*)))
;;       (let (
;; 	    (index
;; 	     (add-index ibt :index-name 'crunch :key-form 'crunch
;; 			:populate t))
;; 	    )
;; 	(loop for i from 0 to 10
;; 	   do
;; 	   (setf (get-value i ibt) (* i i)))
;; 	;; Now create a cursor, advance and put...
;; 	(let ((c (make-cursor ibt)))
;; 	  (cursor-next c)
;; 	  (cursor-next c)
;; 	  (cursor-put c 4 :key 10)
;; 	  (equal (get-value 10 ibt) 4)))
;;       )
;;   t)



;; (deftest class-change-deletion
;;      (progn
;;        (defclass blob-tbc ()
;;  	((slot1 :accessor slot1 :initarg :slot1)
;;  	 (slot2 :accessor slot2 :initarg :slot2)))
;;        (add-to-root "blob" (make-instance 'blob-tbc))
;;        (defclass blob-tbc ()
;;  	((slot1 :accessor slot1 :initarg :slot1)
;;  	 (slot3 :accessor slot3 :initarg :slot3)))
;;        (remove-from-root "blob")
;;        (get-from-root "blob")
;;        )
;;    nil nil)

;; The following machinery is lifted from the dcm project 
;; in the contrib/rread directory.  This is a way to 
;; reproduce an error in the unicde2.lisp file, and 
;; to prove it works.  Since this particular bugs
;; relates to stream position, it cannot be easily
;; reproduced by the "in-out-equal" mechanism in the 
;; testserializer.lisp file.  There must be a simpler
;; way to exercixe this particular bug, but I don't know 
;; how. -- rlr 

(defclass key ()
  ((id :type integer
       :initform -1
       :initarg :id
       :accessor k)))

(defclass managed-object ()
  ((mid :type key
	:initform nil
	:initarg :mid
	:accessor mid)
   )
  )

(defclass Message (managed-object)
  ((msgid :type list :initform "" :accessor :msgd :initarg :msgid)
   (value :type (or list string) :initform "" :accessor :vl :initarg :value)))

(deftest unicodepositiontest
    (let* ((bt (make-btree *store-controller*))
	   (utf16string (format nil "~au sugeston?" (code-char 264)))
	   (mo 
	    (make-instance 'Message :msgid '("Got a suggestion?" "eo")
			   :value   utf16string)))
      (setf (mid mo) (make-instance 'key :id 1000))
      (setf (get-value (mid mo) bt) mo)
      (values (equal utf16string (:vl (get-value (mid mo) bt)))))
  t
  )
