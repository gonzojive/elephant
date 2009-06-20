;;
;; support inheriting a transient class from persistent classes.
;;
;; parts are SBCL-specific but can be ported easily.
;;

(defmethod validate-superclass ((class persistent-proxy-metaclass) (super persistent-metaclass))
  "Proxy classes may inherit from persistent classes."
  t)

(defclass persistent-proxy-metaclass (standard-class) nil)

(defclass player-proxy (player) nil
  (:metaclass persistent-proxy-metaclass))

(defmethod persistent-effective-slot-definition->standard-effective-slot-definition ((sd ele::persistent-effective-slot-definition))
  (make-instance 'standard-effective-slot-definition
                 :name (slot-definition-name sd)
                 :initform (slot-definition-initform sd)
                 :initfunction (slot-definition-initfunction sd)
                 :type (sb-mop:slot-definition-type sd)
                 :allocation :instance ; FIXME
                 :initargs (slot-definition-initargs sd)
                 :readers (sb-mop:slot-definition-readers sd)
                 :writers (sb-mop:slot-definition-writers sd)
                 :documentation (documentation sd t)))

(defmethod persistent-effective-slot-definition->standard-effective-slot-definition
             ((sd standard-effective-slot-definition))
  (setf (slot-definition-allocation sd) :instance)
  sd)

(defmethod compute-slots :around ((class persistent-proxy-metaclass))
  (let ((slots (call-next-method)))
    (sb-pcl::std-compute-slots-around
      class
      (mapcar #'persistent-effective-slot-definition->standard-effective-slot-definition slots))))

(defmethod make-player-proxy ((player player))
  (let ((proxy (allocate-instance (find-class 'player-proxy)))
        (slotnames (mapcar #'slot-definition-name (class-slots (class-of player)))))
    (dolist (sn slotnames proxy)
      (when (slot-boundp player sn)
        (let ((value (slot-value player sn)))
          (setf (slot-value proxy sn) value))))))

(defmethod write-back-proxy (proxy original &key include-slotnames)
  ;; TODO we can figure out the original from the proxy by OID.
  (let* ((slotnames (mapcar #'slot-definition-name (class-slots (class-of proxy))))
         (slotnames (remove-if-not (lambda (slotname)
                                     (member slotname include-slotnames))
                                   slotnames)))
    (format t "INFO: writing back slots ~S~%" slotnames)
    (dolist (sn slotnames (values original slotnames))
      (when (slot-boundp proxy sn)
        (setf (slot-value original sn) (slot-value proxy sn))))))

