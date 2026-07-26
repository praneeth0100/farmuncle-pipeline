-- Applied directly against flxjrcbhmcuaynctokpv (Claude session, 2026-07-25).
-- Home Screen needs a 1D/7D/1M toggle that changes trend arrows/deltas on
-- every card instantly, with no client-side computation across hundreds of
-- cards. These columns are precomputed by the ingestion pipeline's cache
-- refresh step, not calculated by the app.

alter table price_cache
  add column if not exists change_1d numeric,
  add column if not exists change_1d_pct numeric,
  add column if not exists change_7d numeric,
  add column if not exists change_7d_pct numeric,
  add column if not exists change_1m numeric,
  add column if not exists change_1m_pct numeric,
  add column if not exists last_recomputed_at timestamptz;

comment on column price_cache.change_1d is 'Modal price delta vs nearest available price within a 3-day tolerance window, ~1 day back. NULL if no comparable price found in window.';
comment on column price_cache.change_7d is 'Modal price delta vs nearest available price within a 3-day tolerance window, ~7 days back. NULL if no comparable price found in window.';
comment on column price_cache.change_1m is 'Modal price delta vs nearest available price within a 5-day tolerance window, ~30 days back. NULL if no comparable price found in window.';
comment on column price_cache.last_recomputed_at is 'When change_1d/7d/1m were last calculated for this row - set by the incremental per-batch recompute job, not by refreshed_at.';
