#!/bin/bash
#clears logs used for caching in db-postmodern backend (when *cache-mode* is :global-sync-cache)
#replace "elepm" with a working database name and "610" with a value of max-resync-time if non-standard is used

psql -c "DELETE FROM transaction_log WHERE commit_time < (extract(epoch from current_timestamp) - 610);" elepm
psql -c "DELETE FROM update_log WHERE txn_id NOT IN (SELECT txn_id FROM transaction_log);" elepm

#this script might be suboptimal in case of large number of transactions.
#possibly a better one:
# BEGIN;
# DELETE FROM update_log USING transaction_log WHERE (update_log.txn_id = transaction_log.txn_id) AND (commit_time < (extract(epoch from current_timestamp) - 610));
# DELETE FROM transaction_log WHERE commit_time < (extract(epoch from current_timestamp) - 610);
# COMMIT;


