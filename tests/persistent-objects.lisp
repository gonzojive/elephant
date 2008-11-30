
(in-package :ele-tests)

(in-suite* persistent-objects :in elephant-tests)


(defpclass zork187 nil nil)


(deftest drop-instances-atom
  (drop-instances (make-instance 'zork187)))

(deftest drop-instances-list
  (drop-instances (list (make-instance 'zork187))))

