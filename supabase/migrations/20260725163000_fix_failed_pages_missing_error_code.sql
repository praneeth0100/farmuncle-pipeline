-- The first real live_tick run against this project hit a single ordinary,
-- expected page-level failure (a normal network hiccup on one page out of ~33
-- states' worth of resource_1 pages) -- exactly what failed_pages exists to
-- record without aborting the whole batch. But insert_failed_page() itself
-- crashed: `error_code` was missing from this table (my original migration
-- file for this table included it; what actually got applied that day
-- didn't -- a real slip, not a design gap). One benign page hiccup ended up
-- crashing the entire run because the failure-recording path was broken.

alter table failed_pages add column error_code text;
