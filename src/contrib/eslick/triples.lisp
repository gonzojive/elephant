(in-package :elephant-user)

;; THINGS

(defpclass thing ()
  ((name :accessor thing-name :initarg :name :index t)
   (data :accessor thing-data :initform nil :initarg :data)))

(defmethod print-object ((obj thing) stream)
  (format stream "#<THING: ~A>" (thing-name obj)))

;; TRIPLES

(defpclass triple (thing)
  ((predicate :accessor predicate :index t :initform nil :initarg :predicate)
   (source :accessor source :index t :initform nil :initarg :source)
   (target :accessor target :index t :initform nil :initarg :target)))

(defun sources (triples)
  (mapcar #'source triples))

(defun targets (triples)
  (mapcar #'target triples))

(defmethod print-object ((tr triple) stream)
  (format stream "#<TRIPLE: ~A ~A ~A>" (predicate tr) (source tr) (target tr)))

;; IN CASE WE WANT UIDs

(defun id (obj)
  "Get a unique ID for things and triples"
  (elephant::oid obj))

;; FIND THINGS BY NAME

(defun thingp (thing)
  (subtypep (type-of thing) 'thing))

(defun as-thing (name)
  (if (thingp name)
      name
      (find-thing name)))

(defun find-thing (name &optional (error t))
  (let ((obj (get-instance-by-value 'thing 'name name)))
    (if obj obj
	(when error 
	  (cerror "Make thing?"
		  "Can't find thing ~A" name)
	  (add-thing name)))))
;;
;; FIND TRIPLES BY COMPONENTS
;;

(defun triple-match (triple predicate source target)
  (and (or (null predicate) (eq (predicate triple) predicate))
       (or (null source) (eq (source triple) source))
       (or (null target) (eq (target triple) target))))

(defun find-triple (pred src targ)
  (let ((pred (when pred (as-thing pred)))
	(src (when src (as-thing src)))
	(targ (when targ (as-thing targ))))
    (let ((triple 
	   (catch 'found 
	     (map-inverted-index 
	      (lambda (key triple)
		(declare (ignore key))
		(when (triple-match triple pred nil targ)
		  (throw 'found triple)))
	      'triple 'source :value src))))
      triple)))

(defun find-triples (predicate source target)
  (let ((idx (or (and predicate 'predicate)
		 (and source 'source)
		 (and target 'target)))
	(predicate (when predicate (as-thing predicate)))
	(source (when source (as-thing source)))
	(target (when target (as-thing target)))
	results)
    (assert idx)
    (flet ((find-triple (key triple)
		  (declare (ignore key))
		  (when (triple-match triple predicate source target)
		    (push triple results))))
      (declare (dynamic-extent (function find-triple)))
      (map-inverted-index #'find-triple 'triple idx 
			  :value (or predicate source target)))
    results))

;;;
;;; Convenience meta-level operations
;;;

(defun add-triple (pred source target)
  (with-transaction ()
    (let ((triple (find-triple pred source target)))
      (if triple triple
	  (make-instance 'triple 
			 :predicate (as-thing pred)
			 :source (as-thing source) 
			 :target (as-thing target))))))

(defun add-thing (name &optional data)
  (with-transaction ()
    (let ((thing (find-thing name nil)))
      (if thing thing
	  (make-instance 'thing :name name :data data)))))

(defun triple-exists-p (pred src targ)
  (when (find-triple (as-thing pred) (as-thing src) (as-thing targ)) t))

;;
;; A simple relation ontology
;;

;; In this cheap and sleazy ontology our root concepts are: primitive, type, relation and instance
;; - things you know; here they can be primitives, types or relations
;; - types can have things or types as members and are defined by (isa member type)
;; - relations is a special type R where (isa R relation) is also a triple
;; - instances are triples with (R A B) where A and B are any thing (primitive, type, relation, instance)
;;
;; This is not terribly clean...

(defun exists-p (thing)
  "Make sure a thing exists"
  (when (find-thing thing nil) t))

(defun type-p (type)
  (when (find-triples 'isa nil type) t))

(defun assert-type (thing type)
  (add-thing thing)
  (add-thing type)
  (add-triple 'isa thing type))

(defun has-type (thing type)
  (when (find-triple 'isa thing type) t))

(defun make-relation (rel)
  "Make a relation thing"
  (add-triple 'isa rel 'relation))

(defun relation-p (thing)
  "Is this thing a relation?"
  (has-type thing 'relation))

(defun add-instance (rel source target)
  "Make an instance by ensure that rel is a relation and adding source and target to it"
  (unless (relation-p rel)
    (make-relation (as-thing rel)))
  (add-triple rel source target))

(defun instance-exists-p (rel source target)
  "Is there a relation between these things?"
  (triple-exists-p rel source target))

(defun instance-p (instance)
  "Ensure that this object is a valid instance in our ontology"
  (and (subtypep (type-of instance) 'triple)
       (relation-p (predicate instance))
       (thingp (source instance))
       (thingp (target instance))))

(defun all-instances (relation source target)
  (when (relation-p relation)
    (find-triples relation source target)))

;;
;; A family and friends ontology
;;

(defun setup-family-ontology ()
  (add-thing 'parent)
  (add-thing 'person)
  (make-relation 'parent)
  (make-relation 'person))

;; Our family is made up of people

(defun add-person (person)
  (assert-type person 'person))

(defun personp (person)
  (has-type person 'person))

(defun ensure-person (person)
  (unless (personp person)
    (cerror "Make them a person?" 
	    "~A is not a person!"
	    person)
    (add-person person))
  t)

;; We have one basic relationship

(defun add-parent (parent child)
  (ensure-person parent)
  (ensure-person child)
  (add-instance 'parent parent child))

(defun parent-of-p (parent child)
  (instance-exists-p 'parent parent child))

(defun get-parents (child)
  (sources (all-instances 'parent nil child)))

(defun get-children (parent)
  (targets (all-instances 'parent parent nil)))

;; But we can compute all sorts of things

(defun thing-in-common-p (list1 list2)
  (mapcar (lambda (elt)
	    (when (member elt list2)
	      (return-from thing-in-common-p t)))
	  list1)
  nil)
	  
(defun sibling-p (child1 child2)
  "Validate a relationship"
  (thing-in-common-p (get-parents child1)
		     (get-parents child2)))

(defun get-siblings (child)
  "Search for all elements satisfying a relationship"
  (remove (as-thing child)
	  (remove-duplicates 
	   (mapcan #'get-children
		   (get-parents child)))))

(defun get-grandparents (person)
  (remove-duplicates
   (mapcan #'get-parents
	   (get-parents person))))

(defun get-step-siblings (person)
  (remove-if (lambda (sibling)
	       (= (length (intersection (get-parents sibling) (get-parents person))) 2))
             (get-siblings person)))

;; So this is a lot of specialized work...
;;
;; This is where we want a macro system so that these kinds
;; of combinations of sets can be automatically computing using
;; varables...give it a shot if the rest of this makes sense or
;; ask me about it and if I have some time I'll generalize this to
;; declarative instead of functional representations.
;;
;; This is also where Allegro's use of prolog for queries is useful!

;;
;; Some test data
;;

(defun make-family ()
  "Use symbols - it's easier"
  (with-transaction ()
    (setup-family-ontology)
    (mapc #'add-person 
	  '(ian mary-kelly anne claire matthew karen brian dan judy danjr))
    (mapc (lambda (set) 
	    (add-parent (first set) (third set))
	    (add-parent (second set) (third set)))
	  '((karen brian ian) (karen brian matthew) (ian mary-kelly anne)
	    (ian mary-kelly claire) (dan judy mary-kelly) (dan judy danjr)
	    (dan sandy susan)))
    t))

;; Try these:
;; (get-children 'ian)
;; (get-siblings 'mary-kelly)
;; (get-parents 'anne)
;; (get-grandparents 'anne)
;; (get-step-siblings 'mary-kelly)
