
(in-package :db-bdb)

;;
;; Constants and Flags for versions of BDB
;;

(defun import-all-symbols (srcpkg dstpkg)
  (do-symbols (sym srcpkg)
    (shadowing-import (list sym) dstpkg)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; BDB 4.5
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defpackage :db-bdb-c45 
  (:use :cl :uffi :elephant-utils))

(in-package :db-bdb-c45)

;; Current header file version required: Berkeley DB 4.5

;; I don't like the UFFI syntax for enumerations
(defconstant DB-BTREE                 1)
(defconstant DB-HASH                  2)
(defconstant DB-RECNO                 3)
(defconstant DB-QUEUE                 4)
(defconstant DB-UNKNOWN               5)

(defconstant DB_LOCK_NOWAIT   #x00000002)

(defconstant DB_CREATE        #x00000001)
(defconstant DB_FORCE         #x00000004)
(defconstant DB_MULTIVERSION  #x00000008)
(defconstant DB_NOMMAP        #x00000010)
(defconstant DB_RDONLY        #x00000020)
(defconstant DB_RECOVER       #x00000040)
(defconstant DB_THREAD        #x00000080)
(defconstant DB_TRUNCATE      #x00000100)
(defconstant DB_TXN_NOSYNC    #x00000200)
(defconstant DB_TXN_NOT_DURABLE #x00000400)
(defconstant DB_TXN_WRITE_NOSYNC #x00000800)
(defconstant DB_MULTIPLE      #x10000000)
(defconstant DB_MULTIPLE_KEY  #x20000000)

(defconstant DB_EXCL          #x00004000)

(defconstant DB_TXN_NOWAIT    #x00004000)
(defconstant DB_TXN_SYNC      #x00008000)

(defconstant DB_DUP           #x00008000)
(defconstant DB_DUPSORT       #x00010000)

(defconstant DB_JOINENV          #x00000000)
(defconstant DB_INIT_CDB         #x00004000)
(defconstant DB_INIT_LOCK        #x00008000)
(defconstant DB_INIT_LOG         #x00010000)
(defconstant DB_INIT_MPOOL       #x00020000)
(defconstant DB_INIT_REP         #x00040000)
(defconstant DB_INIT_TXN         #x00080000)
(defconstant DB_LOCKDOWN         #x00100000)
(defconstant DB_PRIVATE          #x00200000)
(defconstant DB_RECOVER_FATAL    #x00400000)
(defconstant DB_REGISTER         #x00800000)
(defconstant DB_SYSTEM_MEM       #x01000000)
(defconstant DB_AUTO_COMMIT      #x02000000)
(defconstant DB_READ_COMMITTED   #x04000000)
(defconstant DB_DEGREE_2         #x04000000) ;; DEPRECATED, now called DB_READ_COMMITTED
(defconstant DB_READ_UNCOMMITTED #x08000000)
(defconstant DB_DIRTY_READ       #x08000000) ;; DEPRECATED, now called DB_READ_UNCOMMITTED
(defconstant DB_SNAPSHOT         #x00400000)
(defconstant DB_TXN_SNAPSHOT     #x10000000)

(defconstant DB_AFTER		      1)
(defconstant DB_BEFORE		      3)
(defconstant DB_CURRENT		      6)
(defconstant DB_FIRST		      7)
(defconstant DB_GET_BOTH	      8)
(defconstant DB_GET_BOTH_RANGE	     10)
(defconstant DB_KEYFIRST	     13)
(defconstant DB_KEYLAST		     14)
(defconstant DB_LAST		     15)
(defconstant DB_NEXT		     16)
(defconstant DB_NEXT_DUP	     17)
(defconstant DB_NEXT_NODUP	     18)
(defconstant DB_PREV		     23)
(defconstant DB_PREV_NODUP	     24)
(defconstant DB_SET		     25)
(defconstant DB_SET_RANGE	     27)

(defconstant DB_NODUPDATA	     19)
(defconstant DB_NOOVERWRITE	     20)
(defconstant DB_NOSYNC		     21)

(defconstant DB_POSITION	     22)

(defconstant DB_SEQ_DEC	     #x00000001)
(defconstant DB_SEQ_INC	     #x00000002)
(defconstant DB_SEQ_WRAP     #x00000008)

(defconstant DB_SET_LOCK_TIMEOUT     26)
(defconstant DB_SET_TXN_TIMEOUT      30)

(defconstant DB_FREELIST_ONLY  #x00004000)
(defconstant DB_FREE_SPACE     #x00008000)

(defconstant DB_KEYEMPTY         -30997)
(defconstant DB_KEYEXIST	 -30996)
(defconstant DB_LOCK_DEADLOCK    -30995)
(defconstant DB_LOCK_NOTGRANTED  -30994)
(defconstant DB_NOTFOUND         -30989)

(defconstant DB_LOCK_DEFAULT	     1)
(defconstant DB_LOCK_EXPIRE	     2)
(defconstant DB_LOCK_MAXLOCKS        3)
(defconstant DB_LOCK_MAXWRITE        4)
(defconstant DB_LOCK_MINLOCKS        5)
(defconstant DB_LOCK_MINWRITE        6)
(defconstant DB_LOCK_OLDEST	     7)
(defconstant DB_LOCK_RANDOM	     8)
(defconstant DB_LOCK_YOUNGEST        9)


(def-enum DB-LOCKOP ((:DUMP 0) :GET :GET-TIMEOUT :INHERIT 
		     :PUT :PUT-ALL :PUT-OBJ :PUT-READ
		     :TIMEOUT :TRADE :UPGRADE-WRITE))

(def-enum DB-LOCKMODE ((:NG 0) :READ :WRITE :WAIT 
		       :IWRITE :IREAD :IWR :DIRTY :WWRITE))

(def-struct DB-LOCK
    (off :unsigned-int)
  (ndx :unsigned-int)
  (gen :unsigned-int)
  (mode DB-LOCKMODE))

#+openmcl
(ccl:def-foreign-type DB-LOCK (:struct DB-LOCK))

(def-struct DB-LOCKREQ
    (op DB-LOCKOP)
  (mode DB-LOCKMODE)
  (timeout :unsigned-int)
  (obj (:array :char))
  (lock (* DB-LOCK)))

#+openmcl
(ccl:def-foreign-type DB-LOCKREQ (:struct DB-LOCKREQ))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; BDB 4.6
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defpackage :db-bdb-c46
  (:use :cl :uffi))

(in-package :db-bdb-c46)

;; from cffi.lisp of cl-berkeley-db
(cl:defconstant DB_CREATE #x0000001)

(cl:defconstant DB_DURABLE_UNKNOWN #x0000002)

(cl:defconstant DB_FORCE #x0000004)

(cl:defconstant DB_MULTIVERSION #x0000008)

(cl:defconstant DB_NOMMAP #x0000010)

(cl:defconstant DB_RDONLY #x0000020)

(cl:defconstant DB_RECOVER #x0000040)

(cl:defconstant DB_THREAD #x0000080)

(cl:defconstant DB_TRUNCATE #x0000100)

(cl:defconstant DB_TXN_NOSYNC #x0000200)

(cl:defconstant DB_TXN_NOWAIT #x0000400)

(cl:defconstant DB_TXN_NOT_DURABLE #x0000800)

(cl:defconstant DB_TXN_WRITE_NOSYNC #x0001000)

(cl:defconstant DB_SPARE_FLAG #x0002000)

(cl:defconstant DB_IGNORE_LEASE #x01000000)

(cl:defconstant DB_AUTO_COMMIT #x02000000)

(cl:defconstant DB_READ_COMMITTED #x04000000)

(cl:defconstant DB_DEGREE_2 #x04000000)

(cl:defconstant DB_READ_UNCOMMITTED #x08000000)

(cl:defconstant DB_DIRTY_READ #x08000000)

(cl:defconstant DB_TXN_SNAPSHOT #x10000000)

(cl:defconstant DB_CXX_NO_EXCEPTIONS #x0000001)

(cl:defconstant DB_RPCCLIENT #x0000002)

(cl:defconstant DB_XA_CREATE #x0000002)

(cl:defconstant DB_USE_ENVIRON #x0004000)

(cl:defconstant DB_USE_ENVIRON_ROOT #x0008000)

(cl:defconstant DB_INIT_CDB #x0010000)

(cl:defconstant DB_INIT_LOCK #x0020000)

(cl:defconstant DB_INIT_LOG #x0040000)

(cl:defconstant DB_INIT_MPOOL #x0080000)

(cl:defconstant DB_INIT_REP #x0100000)

(cl:defconstant DB_INIT_TXN #x0200000)

(cl:defconstant DB_LOCKDOWN #x0400000)

(cl:defconstant DB_PRIVATE #x0800000)

(cl:defconstant DB_RECOVER_FATAL #x1000000)

(cl:defconstant DB_REGISTER #x2000000)

(cl:defconstant DB_SYSTEM_MEM #x4000000)

(cl:defconstant DB_JOINENV #x0)

(cl:defconstant DB_EXCL #x0004000)

(cl:defconstant DB_FCNTL_LOCKING #x0008000)

(cl:defconstant DB_NO_AUTO_COMMIT #x0010000)

(cl:defconstant DB_RDWRMASTER #x0020000)

(cl:defconstant DB_WRITEOPEN #x0040000)

(cl:defconstant DB_IMMUTABLE_KEY #x0004000)

(cl:defconstant DB_TXN_SYNC #x0004000)

(cl:defconstant DB_TXN_WAIT #x0008000)

(cl:defconstant DB_CKP_INTERNAL #x0004000)

(cl:defconstant DB_ENCRYPT_AES #x0000001)

(cl:defconstant DB_CDB_ALLDB #x00004000)

(cl:defconstant DB_DIRECT_DB #x00008000)

(cl:defconstant DB_DIRECT_LOG #x00010000)

(cl:defconstant DB_DSYNC_DB #x00020000)

(cl:defconstant DB_DSYNC_LOG #x00040000)

(cl:defconstant DB_LOG_AUTOREMOVE #x00080000)

(cl:defconstant DB_LOG_INMEMORY #x00100000)

(cl:defconstant DB_NOLOCKING #x00200000)

(cl:defconstant DB_NOPANIC #x00400000)

(cl:defconstant DB_OVERWRITE #x00800000)

(cl:defconstant DB_PANIC_ENVIRONMENT #x01000000)

(cl:defconstant DB_REGION_INIT #x20000000)

(cl:defconstant DB_TIME_NOTGRANTED #x40000000)

(cl:defconstant DB_YIELDCPU #x80000000)

(cl:defconstant DB_UPGRADE #x0000001)

(cl:defconstant DB_VERIFY #x0000002)

(cl:defconstant DB_FREELIST_ONLY #x00004000)

(cl:defconstant DB_FREE_SPACE #x00008000)

(cl:defconstant DB_COMPACT_FLAGS (cl:logior #x00004000 #x00008000))

(cl:defconstant DB_DIRECT #x0004000)

(cl:defconstant DB_EXTENT #x0008000)

(cl:defconstant DB_ODDFILESIZE #x0010000)

(cl:defconstant DB_CHKSUM #x00004000)

(cl:defconstant DB_DUP #x00008000)

(cl:defconstant DB_DUPSORT #x00010000)

(cl:defconstant DB_ENCRYPT #x00020000)

(cl:defconstant DB_INORDER #x00040000)

(cl:defconstant DB_RECNUM #x00080000)

(cl:defconstant DB_RENUMBER #x00100000)

(cl:defconstant DB_REVSPLITOFF #x00200000)

(cl:defconstant DB_SNAPSHOT #x00400000)

(cl:defconstant DB_FAST_STAT #x0000001)

(cl:defconstant DB_STAT_ALL #x0000002)

(cl:defconstant DB_STAT_CLEAR #x0000004)

(cl:defconstant DB_STAT_LOCK_CONF #x0000008)

(cl:defconstant DB_STAT_LOCK_LOCKERS #x0000010)

(cl:defconstant DB_STAT_LOCK_OBJECTS #x0000020)

(cl:defconstant DB_STAT_LOCK_PARAMS #x0000040)

(cl:defconstant DB_STAT_MEMP_HASH #x0000080)

(cl:defconstant DB_STAT_NOERROR #x0000100)

(cl:defconstant DB_STAT_SUBSYSTEM #x0000200)

(cl:defconstant DB_JOIN_NOSORT #x0000001)

(cl:defconstant DB_AGGRESSIVE #x0000001)

(cl:defconstant DB_NOORDERCHK #x0000002)

(cl:defconstant DB_ORDERCHKONLY #x0000004)

(cl:defconstant DB_PR_PAGE #x0000008)

(cl:defconstant DB_PR_RECOVERYTEST #x0000010)

(cl:defconstant DB_PRINTABLE #x0000020)

(cl:defconstant DB_SALVAGE #x0000040)

(cl:defconstant DB_UNREF #x0000080)

(cl:defconstant DB_REP_ANYWHERE #x0000001)

(cl:defconstant DB_REP_NOBUFFER #x0000002)

(cl:defconstant DB_REP_PERMANENT #x0000004)

(cl:defconstant DB_REP_REREQUEST #x0000008)

(cl:defconstant DB_MUTEX_ALLOCATED #x01)

(cl:defconstant DB_MUTEX_LOCKED #x02)

(cl:defconstant DB_MUTEX_LOGICAL_LOCK #x04)

(cl:defconstant DB_MUTEX_PROCESS_ONLY #x08)

(cl:defconstant DB_MUTEX_SELF_BLOCK #x10)


(cl:defconstant DB_THREADID_STRLEN 128)

(cl:defconstant DB_LOCKVERSION 1)

(cl:defconstant DB_FILE_ID_LEN 20)

(cl:defconstant DB_LOCK_NORUN 0)

(cl:defconstant DB_LOCK_DEFAULT 1)

(cl:defconstant DB_LOCK_EXPIRE 2)

(cl:defconstant DB_LOCK_MAXLOCKS 3)

(cl:defconstant DB_LOCK_MAXWRITE 4)

(cl:defconstant DB_LOCK_MINLOCKS 5)

(cl:defconstant DB_LOCK_MINWRITE 6)

(cl:defconstant DB_LOCK_OLDEST 7)

(cl:defconstant DB_LOCK_RANDOM 8)

(cl:defconstant DB_LOCK_YOUNGEST 9)

(cl:defconstant DB_LOCK_ABORT #x001)

(cl:defconstant DB_LOCK_NOWAIT #x002)

(cl:defconstant DB_LOCK_RECORD #x004)

(cl:defconstant DB_LOCK_SET_TIMEOUT #x008)

(cl:defconstant DB_LOCK_SWITCH #x010)

(cl:defconstant DB_LOCK_UPGRADE #x020)

(cl:defconstant DB_SET_LOCK_TIMEOUT 1)

(cl:defconstant DB_SET_TXN_NOW 2)

(cl:defconstant DB_SET_TXN_TIMEOUT 3)

(cl:defconstant DB_RENAMEMAGIC #x030800)

(cl:defconstant DB_BTREEVERSION 9)

(cl:defconstant DB_BTREEOLDVER 8)

(cl:defconstant DB_BTREEMAGIC #x053162)

(cl:defconstant DB_HASHVERSION 9)

(cl:defconstant DB_HASHOLDVER 7)

(cl:defconstant DB_HASHMAGIC #x061561)

(cl:defconstant DB_QAMVERSION 4)

(cl:defconstant DB_QAMOLDVER 3)

(cl:defconstant DB_QAMMAGIC #x042253)

(cl:defconstant DB_SEQUENCE_VERSION 2)

(cl:defconstant DB_SEQUENCE_OLDVER 1)

(cl:defconstant DB_AFTER 1)

(cl:defconstant DB_APPEND 2)

(cl:defconstant DB_BEFORE 3)

(cl:defconstant DB_CONSUME 4)

(cl:defconstant DB_CONSUME_WAIT 5)

(cl:defconstant DB_CURRENT 6)

(cl:defconstant DB_FIRST 7)

(cl:defconstant DB_GET_BOTH 8)

(cl:defconstant DB_GET_BOTHC 9)

(cl:defconstant DB_GET_BOTH_RANGE 10)

(cl:defconstant DB_GET_RECNO 11)

(cl:defconstant DB_JOIN_ITEM 12)

(cl:defconstant DB_KEYFIRST 13)

(cl:defconstant DB_KEYLAST 14)

(cl:defconstant DB_LAST 15)

(cl:defconstant DB_NEXT 16)

(cl:defconstant DB_NEXT_DUP 17)

(cl:defconstant DB_NEXT_NODUP 18)

(cl:defconstant DB_NODUPDATA 19)

(cl:defconstant DB_NOOVERWRITE 20)

(cl:defconstant DB_NOSYNC 21)

(cl:defconstant DB_POSITION 22)

(cl:defconstant DB_PREV 23)

(cl:defconstant DB_PREV_DUP 24)

(cl:defconstant DB_PREV_NODUP 25)

(cl:defconstant DB_SET 26)

(cl:defconstant DB_SET_RANGE 27)

(cl:defconstant DB_SET_RECNO 28)

(cl:defconstant DB_UPDATE_SECONDARY 29)

(cl:defconstant DB_WRITECURSOR 30)

(cl:defconstant DB_WRITELOCK 31)

(cl:defconstant DB_OPFLAGS_MASK #x000000ff)

(cl:defconstant DB_MULTIPLE #x10000000)

(cl:defconstant DB_MULTIPLE_KEY #x20000000)

(cl:defconstant DB_RMW #x40000000)

(cl:defconstant DB_BUFFER_SMALL -30999)

(cl:defconstant DB_DONOTINDEX -30998)

(cl:defconstant DB_KEYEMPTY -30997)

(cl:defconstant DB_KEYEXIST -30996)

(cl:defconstant DB_LOCK_DEADLOCK -30995)

(cl:defconstant DB_LOCK_NOTGRANTED -30994)

(cl:defconstant DB_LOG_BUFFER_FULL -30993)

(cl:defconstant DB_NOSERVER -30992)

(cl:defconstant DB_NOSERVER_HOME -30991)

(cl:defconstant DB_NOSERVER_ID -30990)

(cl:defconstant DB_NOTFOUND -30989)

(cl:defconstant DB_OLD_VERSION -30988)

(cl:defconstant DB_PAGE_NOTFOUND -30987)

(cl:defconstant DB_REP_DUPMASTER -30986)

(cl:defconstant DB_REP_HANDLE_DEAD -30985)

(cl:defconstant DB_REP_HOLDELECTION -30984)

(cl:defconstant DB_REP_IGNORE -30983)

(cl:defconstant DB_REP_ISPERM -30982)

(cl:defconstant DB_REP_JOIN_FAILURE -30981)

(cl:defconstant DB_REP_LEASE_EXPIRED -30980)

(cl:defconstant DB_REP_LOCKOUT -30979)

(cl:defconstant DB_REP_NEWSITE -30978)

(cl:defconstant DB_REP_NOTPERM -30977)

(cl:defconstant DB_REP_UNAVAIL -30976)

(cl:defconstant DB_RUNRECOVERY -30975)

(cl:defconstant DB_SECONDARY_BAD -30974)

(cl:defconstant DB_VERIFY_BAD -30973)

(cl:defconstant DB_VERSION_MISMATCH -30972)

(cl:defconstant DB_ALREADY_ABORTED -30899)

(cl:defconstant DB_DELETED -30898)

(cl:defconstant DB_EVENT_NOT_HANDLED -30897)

(cl:defconstant DB_NEEDSPLIT -30896)

(cl:defconstant DB_REP_BULKOVF -30895)

(cl:defconstant DB_REP_EGENCHG -30894)

(cl:defconstant DB_REP_LOGREADY -30893)

(cl:defconstant DB_REP_NEWMASTER -30892)

(cl:defconstant DB_REP_PAGEDONE -30891)

(cl:defconstant DB_SURPRISE_KID -30890)

(cl:defconstant DB_SWAPBYTES -30889)

(cl:defconstant DB_TIMEOUT -30888)

(cl:defconstant DB_TXN_CKP -30887)

(cl:defconstant DB_VERIFY_FATAL -30886)
(cl:defconstant DB_DBT_APPMALLOC        #x001   "Callback allocated memory.")
(cl:defconstant DB_DBT_DUPOK            #x002   "Insert if duplicate.")
(cl:defconstant DB_DBT_ISSET            #x004   "Lower level calls set value.")
(cl:defconstant DB_DBT_MALLOC           #x008   "Return in malloc'd memory.")
(cl:defconstant	DB_DBT_MULTIPLE		#x010	"References multiple records.")
(cl:defconstant DB_DBT_PARTIAL          #x020   "Partial put/get.")
(cl:defconstant DB_DBT_REALLOC          #x040   "Return in realloc'd memory.")
(cl:defconstant DB_DBT_USERCOPY         #x080   "Use the user-supplied callback.")
(cl:defconstant DB_DBT_USERMEM          #x100   "Return in user's memory.")

(defconstant DB-BTREE                 1)
(defconstant DB-HASH                  2)
(defconstant DB-RECNO                 3)
(defconstant DB-QUEUE                 4)
(defconstant DB-UNKNOWN               5)

(defconstant	DB_SEQ_DEC		#x00000001)
(defconstant	DB_SEQ_INC		#x00000002)
(defconstant	DB_SEQ_RANGE_SET	#x00000004)
(defconstant	DB_SEQ_WRAP		#x00000008)
(defconstant	DB_SEQ_WRAPPED		#x00000010)

(def-enum DB-LOCKOP ((:DUMP 0) :GET :GET-TIMEOUT :INHERIT 
		     :PUT :PUT-ALL :PUT-OBJ :PUT-READ
		     :TIMEOUT :TRADE :UPGRADE-WRITE))

(def-enum DB-LOCKMODE ((:NG 0) :READ :WRITE :WAIT 
		       :IWRITE :IREAD :IWR :DIRTY :WWRITE))

(def-struct DB-LOCK
    (off :unsigned-int)
  (ndx :unsigned-int)
  (gen :unsigned-int)
  (mode DB-LOCKMODE))

#+openmcl
(ccl:def-foreign-type DB-LOCK (:struct DB-LOCK))

(def-struct DB-LOCKREQ
    (op DB-LOCKOP)
  (mode DB-LOCKMODE)
  (timeout :unsigned-int)
  (obj (:array :char))
  (lock (* DB-LOCK)))

#+openmcl
(ccl:def-foreign-type DB-LOCKREQ (:struct DB-LOCKREQ))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; BDB 4.7
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defpackage :db-bdb-c47
  (:use :cl :uffi))

(in-package :db-bdb-c47)


(defconstant DB_THREADID_STRLEN 128)

(defconstant DB_LOCKVERSION 1)

(defconstant DB_FILE_ID_LEN 20)

(defconstant DB_LOCK_NORUN 0)

(defconstant DB_LOCK_DEFAULT 1)

(defconstant DB_LOCK_EXPIRE 2)

(defconstant DB_LOCK_MAXLOCKS 3)

(defconstant DB_LOCK_MAXWRITE 4)

(defconstant DB_LOCK_MINLOCKS 5)

(defconstant DB_LOCK_MINWRITE 6)

(defconstant DB_LOCK_OLDEST 7)

(defconstant DB_LOCK_RANDOM 8)

(defconstant DB_LOCK_YOUNGEST 9)

(defconstant DB_LOGVERSION 14)

(defconstant DB_LOGOLDVER 8)

(defconstant DB_LOGMAGIC #x040988)

(defconstant DB_user_BEGIN 10000)

(defconstant DB_debug_FLAG #x80000000)

(defconstant DB_TXNVERSION 1)

(defconstant DB-BTREE                 1)
(defconstant DB-HASH                  2)
(defconstant DB-RECNO                 3)
(defconstant DB-QUEUE                 4)
(defconstant DB-UNKNOWN               5)

(defconstant DB_RENAMEMAGIC #x030800)

(defconstant DB_BTREEVERSION 9)

(defconstant DB_BTREEOLDVER 8)

(defconstant DB_BTREEMAGIC #x053162)

(defconstant DB_HASHVERSION 9)

(defconstant DB_HASHOLDVER 7)

(defconstant DB_HASHMAGIC #x061561)

(defconstant DB_QAMVERSION 4)

(defconstant DB_QAMOLDVER 3)

(defconstant DB_QAMMAGIC #x042253)

(defconstant DB_SEQUENCE_VERSION 2)

(defconstant DB_SEQUENCE_OLDVER 1)

(defconstant DB_AFTER 1)

(defconstant DB_APPEND 2)

(defconstant DB_BEFORE 3)

(defconstant DB_CONSUME 4)

(defconstant DB_CONSUME_WAIT 5)

(defconstant DB_CURRENT 6)

(defconstant DB_FIRST 7)

(defconstant DB_GET_BOTH 8)

(defconstant DB_GET_BOTHC 9)

(defconstant DB_GET_BOTH_RANGE 10)

(defconstant DB_GET_RECNO 11)

(defconstant DB_JOIN_ITEM 12)

(defconstant DB_KEYFIRST 13)

(defconstant DB_KEYLAST 14)

(defconstant DB_LAST 15)

(defconstant DB_NEXT 16)

(defconstant DB_NEXT_DUP 17)

(defconstant DB_NEXT_NODUP 18)

(defconstant DB_NODUPDATA 19)

(defconstant DB_NOOVERWRITE 20)

(defconstant DB_NOSYNC 21)

(defconstant DB_POSITION 22)

(defconstant DB_PREV 23)

(defconstant DB_PREV_DUP 24)

(defconstant DB_PREV_NODUP 25)

(defconstant DB_SET 26)

(defconstant DB_SET_RANGE 27)

(defconstant DB_SET_RECNO 28)

(defconstant DB_UPDATE_SECONDARY 29)

(defconstant DB_WRITECURSOR 30)

(defconstant DB_WRITELOCK 31)

(defconstant DB_OPFLAGS_MASK #x000000ff)

(defconstant DB_BUFFER_SMALL -30999)

(defconstant DB_DONOTINDEX -30998)

(defconstant DB_FOREIGN_CONFLICT -30997)

(defconstant DB_KEYEMPTY -30996)

(defconstant DB_KEYEXIST -30995)

(defconstant DB_LOCK_DEADLOCK -30994)

(defconstant DB_LOCK_NOTGRANTED -30993)

(defconstant DB_LOG_BUFFER_FULL -30992)

(defconstant DB_NOSERVER -30991)

(defconstant DB_NOSERVER_HOME -30990)

(defconstant DB_NOSERVER_ID -30989)

(defconstant DB_NOTFOUND -30988)

(defconstant DB_OLD_VERSION -30987)

(defconstant DB_PAGE_NOTFOUND -30986)

(defconstant DB_REP_DUPMASTER -30985)

(defconstant DB_REP_HANDLE_DEAD -30984)

(defconstant DB_REP_HOLDELECTION -30983)

(defconstant DB_REP_IGNORE -30982)

(defconstant DB_REP_ISPERM -30981)

(defconstant DB_REP_JOIN_FAILURE -30980)

(defconstant DB_REP_LEASE_EXPIRED -30979)

(defconstant DB_REP_LOCKOUT -30978)

(defconstant DB_REP_NEWSITE -30977)

(defconstant DB_REP_NOTPERM -30976)

(defconstant DB_REP_UNAVAIL -30975)

(defconstant DB_RUNRECOVERY -30974)

(defconstant DB_SECONDARY_BAD -30973)

(defconstant DB_VERIFY_BAD -30972)

(defconstant DB_VERSION_MISMATCH -30971)

(defconstant DB_ALREADY_ABORTED -30899)

(defconstant DB_DELETED -30898)

(defconstant DB_EVENT_NOT_HANDLED -30897)

(defconstant DB_NEEDSPLIT -30896)

(defconstant DB_REP_BULKOVF -30895)

(defconstant DB_REP_EGENCHG -30894)

(defconstant DB_REP_LOGREADY -30893)

(defconstant DB_REP_NEWMASTER -30892)

(defconstant DB_REP_PAGEDONE -30891)

(defconstant DB_SURPRISE_KID -30890)

(defconstant DB_SWAPBYTES -30889)

(defconstant DB_TIMEOUT -30888)

(defconstant DB_TXN_CKP -30887)

(defconstant DB_VERIFY_FATAL -30886)

(defconstant DB_DBM_HSEARCH 0)

(defconstant DB_AGGRESSIVE #x00000001)

(defconstant DB_ARCH_ABS #x00000001)

(defconstant DB_ARCH_DATA #x00000002)

(defconstant DB_ARCH_LOG #x00000004)

(defconstant DB_ARCH_REMOVE #x00000008)

(defconstant DB_AUTO_COMMIT #x00000100)

(defconstant DB_CDB_ALLDB #x00000004)

(defconstant DB_CHKSUM #x00000004)

(defconstant DB_CKP_INTERNAL #x00000002)

(defconstant DB_CREATE #x00000001)

(defconstant DB_CXX_NO_EXCEPTIONS #x00000002)

(defconstant DB_DIRECT #x00000002)

(defconstant DB_DIRECT_DB #x00000040)

(defconstant DB_DSYNC_DB #x00000080)

(defconstant DB_DUP #x00000008)

(defconstant DB_DUPSORT #x00000002)

(defconstant DB_DURABLE_UNKNOWN #x00000020)

(defconstant DB_ENCRYPT #x00000001)

(defconstant DB_ENCRYPT_AES #x00000001)

(defconstant DB_EXCL #x00000400)

(defconstant DB_EXTENT #x00000004)

(defconstant DB_FAST_STAT #x00000001)

(defconstant DB_FCNTL_LOCKING #x00001000)

(defconstant DB_FLUSH #x00000001)

(defconstant DB_FORCE #x00000001)

(defconstant DB_FOREIGN_ABORT #x00000001)

(defconstant DB_FOREIGN_CASCADE #x00000002)

(defconstant DB_FOREIGN_NULLIFY #x00000004)

(defconstant DB_FREELIST_ONLY #x00000001)

(defconstant DB_FREE_SPACE #x00000002)

(defconstant DB_IGNORE_LEASE #x00001000)

(defconstant DB_IMMUTABLE_KEY #x00000002)

(defconstant DB_INIT_CDB #x00000020)

(defconstant DB_INIT_LOCK #x00000040)

(defconstant DB_INIT_LOG #x00000080)

(defconstant DB_INIT_MPOOL #x00000100)

(defconstant DB_INIT_REP #x00000200)

(defconstant DB_INIT_TXN #x00000400)

(defconstant DB_INORDER #x00000010)

(defconstant DB_JOIN_NOSORT #x00000001)

(defconstant DB_LOCKDOWN #x00000800)

(defconstant DB_LOCK_NOWAIT #x00000001)

(defconstant DB_LOCK_RECORD #x00000002)

(defconstant DB_LOCK_SET_TIMEOUT #x00000004)

(defconstant DB_LOCK_SWITCH #x00000008)

(defconstant DB_LOCK_UPGRADE #x00000010)

(defconstant DB_LOG_AUTO_REMOVE #x00000004)

(defconstant DB_LOG_CHKPNT #x00000002)

(defconstant DB_LOG_COMMIT #x00000004)

(defconstant DB_LOG_DIRECT #x00000001)

(defconstant DB_LOG_DSYNC #x00000002)

(defconstant DB_LOG_IN_MEMORY #x00000008)

(defconstant DB_LOG_NOCOPY #x00000008)

(defconstant DB_LOG_NOT_DURABLE #x00000010)

(defconstant DB_LOG_WRNOSYNC #x00000020)

(defconstant DB_LOG_ZERO #x00000010)

(defconstant DB_MPOOL_CREATE #x00000001)

(defconstant DB_MPOOL_DIRTY #x00000002)

(defconstant DB_MPOOL_DISCARD #x00000001)

(defconstant DB_MPOOL_EDIT #x00000004)

(defconstant DB_MPOOL_FREE #x00000008)

(defconstant DB_MPOOL_LAST #x00000010)

(defconstant DB_MPOOL_NEW #x00000020)

(defconstant DB_MPOOL_NOFILE #x00000001)

(defconstant DB_MPOOL_NOLOCK #x00000002)

(defconstant DB_MPOOL_UNLINK #x00000002)

(defconstant DB_MULTIPLE #x00002000)

(defconstant DB_MULTIPLE_KEY #x00000100)

(defconstant DB_MULTIVERSION #x00000008)

(defconstant DB_MUTEX_ALLOCATED #x00000001)

(defconstant DB_MUTEX_LOCKED #x00000002)

(defconstant DB_MUTEX_LOGICAL_LOCK #x00000004)

(defconstant DB_MUTEX_PROCESS_ONLY #x00000008)

(defconstant DB_MUTEX_SELF_BLOCK #x00000010)

(defconstant DB_NOLOCKING #x00000200)

(defconstant DB_NOMMAP #x00000010)

(defconstant DB_NOORDERCHK #x00000002)

(defconstant DB_NOPANIC #x00000400)

(defconstant DB_NO_AUTO_COMMIT #x00002000)

(defconstant DB_ODDFILESIZE #x00000040)

(defconstant DB_ORDERCHKONLY #x00000004)

(defconstant DB_OVERWRITE #x00001000)

(defconstant DB_PANIC_ENVIRONMENT #x00002000)

(defconstant DB_PRINTABLE #x00000008)

(defconstant DB_PRIVATE #x00001000)

(defconstant DB_PR_PAGE #x00000010)

(defconstant DB_PR_RECOVERYTEST #x00000020)

(defconstant DB_RDONLY #x00000080)

(defconstant DB_RDWRMASTER #x00004000)

(defconstant DB_READ_COMMITTED #x00000400)

(defconstant DB_READ_UNCOMMITTED #x00000200)

(defconstant DB_RECNUM #x00000020)

(defconstant DB_RECOVER #x00000010)

(defconstant DB_RECOVER_FATAL #x00002000)

(defconstant DB_REGION_INIT #x00004000)

(defconstant DB_REGISTER #x00004000)

(defconstant DB_RENUMBER #x00000040)

(defconstant DB_REPMGR_CONF_2SITE_STRICT #x00000001)

(defconstant DB_REPMGR_PEER #x00000001)

(defconstant DB_REP_ANYWHERE #x00000001)

(defconstant DB_REP_CLIENT #x00000001)

(defconstant DB_REP_CONF_BULK #x00000002)

(defconstant DB_REP_CONF_DELAYCLIENT #x00000004)

(defconstant DB_REP_CONF_LEASE #x00000008)

(defconstant DB_REP_CONF_NOAUTOINIT #x00000010)

(defconstant DB_REP_CONF_NOWAIT #x00000020)

(defconstant DB_REP_ELECTION #x00000004)

(defconstant DB_REP_MASTER #x00000002)

(defconstant DB_REP_NOBUFFER #x00000002)

(defconstant DB_REP_PERMANENT #x00000004)

(defconstant DB_REP_REREQUEST #x00000008)

(defconstant DB_REVSPLITOFF #x00000080)

(defconstant DB_RMW #x00000800)

(defconstant DB_RPCCLIENT #x00000001)

(defconstant DB_SALVAGE #x00000040)

(defconstant DB_SA_SKIPFIRSTKEY #x00010000)

(defconstant DB_SEQ_DEC #x00000001)

(defconstant DB_SEQ_INC #x00000002)

(defconstant DB_SEQ_RANGE_SET #x00000004)

(defconstant DB_SEQ_WRAP #x00000008)

(defconstant DB_SEQ_WRAPPED #x00000010)

(defconstant DB_SET_LOCK_TIMEOUT #x00000002)

(defconstant DB_SET_TXN_NOW #x00000004)

(defconstant DB_SET_TXN_TIMEOUT #x00000001)

(defconstant DB_SNAPSHOT #x00000100)

(defconstant DB_STAT_ALL #x00000002)

(defconstant DB_STAT_CLEAR #x00000001)

(defconstant DB_STAT_LOCK_CONF #x00000004)

(defconstant DB_STAT_LOCK_LOCKERS #x00000008)

(defconstant DB_STAT_LOCK_OBJECTS #x00000010)

(defconstant DB_STAT_LOCK_PARAMS #x00000020)

(defconstant DB_STAT_MEMP_HASH #x00000004)

(defconstant DB_STAT_MEMP_NOERROR #x00000008)

(defconstant DB_STAT_SUBSYSTEM #x00000004)

(defconstant DB_ST_DUPOK #x00000100)

(defconstant DB_ST_DUPSET #x00000200)

(defconstant DB_ST_DUPSORT #x00000400)

(defconstant DB_ST_IS_RECNO #x00000800)

(defconstant DB_ST_OVFL_LEAF #x00001000)

(defconstant DB_ST_RECNUM #x00002000)

(defconstant DB_ST_RELEN #x00004000)

(defconstant DB_ST_TOPLEVEL #x00008000)

(defconstant DB_SYSTEM_MEM #x00008000)

(defconstant DB_THREAD #x00000004)

(defconstant DB_TIME_NOTGRANTED #x00008000)

(defconstant DB_TRUNCATE #x00008000)

(defconstant DB_TXN_NOSYNC #x00000001)

(defconstant DB_TXN_NOT_DURABLE #x00000200)

(defconstant DB_TXN_NOWAIT #x00000002)

(defconstant DB_TXN_SNAPSHOT #x00000800)

(defconstant DB_TXN_SYNC #x00000004)

(defconstant DB_TXN_WAIT #x00000008)

(defconstant DB_TXN_WRITE_NOSYNC #x00000020)

(defconstant DB_UNREF #x00000080)

(defconstant DB_UPGRADE #x00000001)

(defconstant DB_USE_ENVIRON #x00000002)

(defconstant DB_USE_ENVIRON_ROOT #x00000008)

(defconstant DB_VERB_DEADLOCK #x00000001)

(defconstant DB_VERB_FILEOPS #x00000002)

(defconstant DB_VERB_FILEOPS_ALL #x00000004)

(defconstant DB_VERB_RECOVERY #x00000008)

(defconstant DB_VERB_REGISTER #x00000010)

(defconstant DB_VERB_REPLICATION #x00000020)

(defconstant DB_VERB_REPMGR_CONNFAIL #x00000040)

(defconstant DB_VERB_REPMGR_MISC #x00000080)

(defconstant DB_VERB_REP_ELECT #x00000100)

(defconstant DB_VERB_REP_LEASE #x00000200)

(defconstant DB_VERB_REP_MISC #x00000400)

(defconstant DB_VERB_REP_MSGS #x00000800)

(defconstant DB_VERB_REP_SYNC #x00001000)

(defconstant DB_VERB_WAITSFOR #x00002000)

(defconstant DB_VERIFY #x00000002)

(defconstant DB_WRITEOPEN #x00010000)

(defconstant DB_XA_CREATE #x00000800)

(defconstant DB_YIELDCPU #x00010000)

(defconstant DB_DBT_APPMALLOC        #x001   "Callback allocated memory.")
(defconstant DB_DBT_DUPOK            #x002   "Insert if duplicate.")
(defconstant DB_DBT_ISSET            #x004   "Lower level calls set value.")
(defconstant DB_DBT_MALLOC           #x008   "Return in malloc'd memory.")
(defconstant DB_DBT_MULTIPLE	     #x010	"References multiple records.")
(defconstant DB_DBT_PARTIAL          #x020   "Partial put/get.")
(defconstant DB_DBT_REALLOC          #x040   "Return in realloc'd memory.")
(defconstant DB_DBT_USERCOPY         #x080   "Use the user-supplied callback.")
(defconstant DB_DBT_USERMEM          #x100   "Return in user's memory.")

(defconstant DB_DEGREE_2 DB_READ_COMMITTED)
(defconstant DB_DIRTY_READ DB_READ_UNCOMMITTED)
(defconstant DB_JOINENV #x0)

(def-enum DB-LOCKOP ((:DUMP 0) :GET :GET-TIMEOUT :INHERIT 
		     :PUT :PUT-ALL :PUT-OBJ :PUT-READ
		     :TIMEOUT :TRADE :UPGRADE-WRITE))

(def-enum DB-LOCKMODE ((:NG 0) :READ :WRITE :WAIT 
		       :IWRITE :IREAD :IWR :DIRTY :WWRITE))

(def-struct DB-LOCK
    (off :unsigned-int)
  (ndx :unsigned-int)
  (gen :unsigned-int)
  (mode DB-LOCKMODE))

#+openmcl
(ccl:def-foreign-type DB-LOCK (:struct DB-LOCK))

(def-struct DB-LOCKREQ
    (op DB-LOCKOP)
  (mode DB-LOCKMODE)
  (timeout :unsigned-int)
  (obj (:array :char))
  (lock (* DB-LOCK)))

#+openmcl
(ccl:def-foreign-type DB-LOCKREQ (:struct DB-LOCKREQ))
