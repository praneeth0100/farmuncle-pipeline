-- Where no copy had warehouse prices, the merge kept the backfill-made row (higher id, old district label).
-- The live feed made the lower id first, so its district is the current one: swap the two labels. The MERGED
-- row keeps the old district, which is what find_or_create_mandi step 1b matches old feed rows on.
with pairs as (
  select s.id sid, s.district sd, l.id lid, l.district ld
  from mandis l join mandis s on s.id = l.merged_into_id
  where l.merge_reason = 'district_split_moved_2026-11-06' and l.id < s.id
    and not exists (select 1 from mandi_daily_prices p where p.mandi_id = s.id)
    and (select count(*) from mandis x where x.merged_into_id = s.id and x.merge_reason = 'district_split_moved_2026-11-06') = 1
)
update mandis m set district = case when m.id = pairs.sid then pairs.ld else pairs.sd end
from pairs where m.id in (pairs.sid, pairs.lid);
