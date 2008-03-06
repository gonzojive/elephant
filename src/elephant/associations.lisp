(in-package :elephant)

;; ===============================
;;  Association API
;; ===============================

;; Separate association methods from metaprotocol issues

;;(defgeneric build-association (classname1 classname2 type sc)
;;  (:documentation "Allow backend-specific overrides by creating instances
;;                   via this interface"))

(defgeneric add-association (inst1 inst2 assoc)
  (:documentation "Validates class types and adds the association"))

(defgeneric remove-association (inst1 inst2 assoc)
  (:documentation "Remove the association between inst1 and inst2 in assoc"))

(defgeneric remove-all-associations (type inst assoc)
  (:documentation "Remove all associations.  Valid types are :primary and :secondary"))

(defgeneric associated-p (inst1 inst2 assoc)
  (:documentation "Predicate: are the instances in the association set"))

(defgeneric get-association-list (from inst assoc)
  (:documentation "Get a freshly consed list of the associated objects"))

(defgeneric get-association-pset (from inst assoc)
  (:documentation "Return a proxy pset object for the associations. Lighter weight 
                   than the "))
  
(defgeneric map-associations (fn from inst assoc)
  (:documentation "What it says"))


;; ======================================
;;  Association Objects
;; ======================================

(defclass association (persistent-collection)
  ((type :accessor association-type :initarg :type :type (member :1-n :n-m)))
  (:documentation "Base class for pairwise associations between persistent objects."))

(defmethod valid-association-pair ((primary persistent) (secondary persistent) (assoc association))
  t)

(defmethod valid-association-pair ((primary t) (secondary t) (assoc association))
  (error "Associations must be between persistent objects"))

(defun make-association (type &optional (sc *store-controller*))
  (build-association type sc))

;; =======================================
;;  Generic association implementation
;; =======================================

(defstruct oid-pair left right)

(defpclass default-association (default-pset association)
  ((next-id :accessor next-id :initarg :start-id :initform 1)
   (table :accessor association-table :initarg :table)
   (type :accessor association-type :initarg :type))
  (:documentation "Contains references to the underlying persistent
                   data so backends can override build-association
                   to create their own interfaces for improved performance
                   or backend specific features."))

;; (defmethod build-association (type sc)
;;   (let ((table (make-indexed-btree sc)))
;;     (add-index table :index-name :primary)
;;     (make-instance 'default-association)))



;; ====================================================

;;
;; Metaprotocol support for class associations
;;

(defmacro def-association (type class1def class2def)
  (destructuring-bind (classname1 &key slot1 initarg1) class1def
    (destructuring-bind (classname2 &key slot2 initarg2) class2def
      (let ((class1 (find-class classname1))
	    (class2 (find-class classname2)))
	))))

;;
;; Slot generic functions for backends
;;

;; raw slot value is pset?
;; setf raw slot value requires pset; union or destructive?

;; (defgeneric insert-item (instance slot item))

;; (defmethod insert-item (assoc slot item))

;; (defgeneric delete-item (instance slot item))

;; (defgeneric find-item (class slot item))

;; (defgeneric list-of (class slot))

;; (defgeneric list-of-oids (class slot))

;; (defgeneric (setf list-of) (class slot))

;; (defgeneric size (class slot))



;;
;; Small utilities
;;

;;(defun safe-find-class 

