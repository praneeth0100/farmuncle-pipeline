-- This exact fix was made earlier the same day (see migration
-- 20260725160100's own comment about batch_id needing to reference
-- raw_api_batches, not ingestion_batches), but the ALTER statements were
-- bundled in the same transaction as a DROP INDEX that failed on a wrong
-- name. The whole transaction rolled back silently, undoing the FK fix too
-- -- and it was never re-verified after the index name was corrected
-- separately. First real live_tick page-2 write confirmed the FK was still
-- wrong: "insert or update on table raw_price_entries violates foreign key
-- constraint raw_price_entries_first_seen_batch_id_fkey ... Key
-- (first_seen_batch_id)=(...) is not present in table ingestion_batches."

alter table raw_price_entries drop constraint raw_price_entries_first_seen_batch_id_fkey;
alter table raw_price_entries drop constraint raw_price_entries_last_seen_batch_id_fkey;
alter table raw_price_entries add constraint raw_price_entries_first_seen_batch_id_fkey
    foreign key (first_seen_batch_id) references raw_api_batches(id);
alter table raw_price_entries add constraint raw_price_entries_last_seen_batch_id_fkey
    foreign key (last_seen_batch_id) references raw_api_batches(id);
