;; $Id: package.lisp,v 1.14 2008/02/19 22:44:06 alemmens Exp $

#-(or allegro lispworks sbcl openmcl)
  (error "Unsupported implementation: ~A" (lisp-implementation-type))

(defpackage :rucksack
  (:nicknames :rs)

   (:use :queue :cl
    #+allegro :mop
    #+lispworks :clos
    #+sbcl :sb-mop
    #+openmcl :openmcl-mop)

  (:export

   ;; Cache
   #:cache #:standard-cache
   #:open-cache #:close-cache #:with-cache
   #:cache-size #:cache-count
   #:cache-create-object #:cache-get-object #:cache-touch-object
   #:cache-commit #:cache-rollback #:cache-recover
   #:open-transaction #:close-transaction #:map-transactions

   ;; MOP related
   #:persistent-class
   #:update-persistent-instance-for-redefined-class

   ;; Objects
   #:persistent-object
   #:persistent-data #:persistent-array #:persistent-cons
   #:object-id
   #:p-cons #:p-array
   #:p-eql
   #:p-car #:p-cdr #:p-list #:p-last
   #:p-endp #:p-consp
   #:p-caar #:p-cadr #:p-cdar #:p-cddr
   #:unwrap-persistent-list
   #:p-mapcar #:p-mapc #:p-maplist #:p-mapl
   #:p-member-if
   #:p-pop #:p-push
   #:p-make-array #:p-aref #:p-array-dimensions
   #:p-length #:p-find #:p-replace #:p-delete-if #:p-position

   ;; Heaps
   #:heap #:free-list-heap #:mark-and-sweep-heap #:simple-free-list-heap
   #:open-heap #:close-heap
   #:heap-stream #:heap-end

   ;; Rucksacks
   #:*rucksack*
   #:open-rucksack #:close-rucksack #:with-rucksack #:current-rucksack
   #:rucksack #:standard-rucksack
   #:rucksack-cache
   #:rucksack-directory
   #:rucksack-commit #:rucksack-rollback
   #:add-rucksack-root #:map-rucksack-roots #:rucksack-roots
   #:commit #:rollback

   ;; Class and slot indexing
   #:add-class-index #:add-slot-index
   #:remove-class-index #:remove-slot-index
   #:map-class-indexes #:map-slot-indexes
   #:rucksack-add-class-index #:rucksack-add-slot-index
   #:rucksack-make-class-index
   #:rucksack-remove-class-index #:rucksack-remove-slot-index
   #:rucksack-class-index #:rucksack-slot-index
   #:rucksack-map-class-indexes #:rucksack-map-slot-indexes
   #:rucksack-maybe-index-changed-slot #:rucksack-maybe-index-new-object
   #:rucksack-map-class #:rucksack-map-slot
   #:rucksack-do-class #:rucksack-do-slot
   #:rucksack-delete-object

   ;; Transactions
   #:current-transaction

   #:transaction-start #:transaction-commit #:transaction-rollback
   #:with-transaction #:*transaction*
   #:transaction #:standard-transaction
   #:transaction-start-1 #:transaction-commit-1
   #:transaction-id

   ;; Conditions
   #:rucksack-error #:simple-rucksack-error #:transaction-conflict
   #:internal-rucksack-error
   #:duplicate-slot-value #:slot-error 

   ;; Indexes
   #:map-index #:index-insert #:index-delete #:make-index
   #:define-index-spec #:find-index-spec

   ;; Btrees
   #:btree
   #:btree-key< #:btree-key<= #:btree-key= #:btree-key>= #:btree-key>
   #:btree-value=
   #:btree-max-node-size #:btree-unique-keys-p
   #:btree-key-type #:btree-value-type
   #:btree-node-class #:btree-node
   #:btree-nr-keys #:btree-nr-values
   ;; Functions
   #:btree-search #:btree-insert #:btree-delete #:btree-delete-key
   #:map-btree #:map-btree-keys
   ;; Conditions
   #:btree-error #:btree-search-error #:btree-insertion-error
   #:btree-key-already-present-error #:btree-type-error
   #:btree-error-btree #:btree-error-key #:btree-error-value))


