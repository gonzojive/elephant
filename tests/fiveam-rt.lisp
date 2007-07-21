(in-package :elephant-tests)

;; A wrapper around the old RT test functions to make them compatible with Five-AM.
;; 
;; This is a first step to switch testing framework to FiveAM from RT.
;; The tests that use the deftest macro doesn't show the advantages FiveAM has over RT,
;; but is a way to migrate to a more modern test framework.
;; Future tests may be written using the FiveAM features such as the is macro,
;; dependencies between tests and more.

(in-suite* elephant-tests)

(defmacro deftest (name &rest body)
  (let* ((p body)
	 (properties
	  (loop while (keywordp (first p))
             unless (cadr p)
             do (error "Poorly formed deftest: ~A~%"
                       (list* 'deftest name body))
             append (list (pop p) (pop p))))
	 (form (pop p))
	 (vals p)
         (result (gensym "RESULT")))
    (declare (ignorable properties))
    `(fiveam:test ,name
      (let ((,result (multiple-value-list ,form)))
        ,@(loop for v in vals
             collect `(fiveam:is (equalp ',v (pop ,result))))))))

(defun do-test (&optional (name *test*))
  (fiveam:run! name))

(defun do-tests ()
  (fiveam:run! 'elephant-tests))
