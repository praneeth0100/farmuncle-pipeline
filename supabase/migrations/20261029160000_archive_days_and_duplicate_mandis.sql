-- Record-only bookkeeping. Nothing in the pipeline waits on or reads these tables to decide anything.

-- One row per archived day: where it stands and whether R2 holds as many rows as were sent.
create table if not exists public.archive_days (
  day            date primary key,
  status         text not null check (status in ('ARCHIVED', 'DELETED', 'FAILED')),
  rows_sent      integer,
  bytes          bigint,
  quarantined    integer,
  r2_rows        integer,           -- rows read back from the R2 file
  rows_match     boolean,           -- r2_rows = rows_sent (null when the read-back could not run)
  attempts       integer not null default 1,
  last_error     text,
  updated_at     timestamptz not null default now()
);
alter table public.archive_days enable row level security;
revoke all on public.archive_days from anon, authenticated;

create or replace function public.record_archive_day(
  p_day date, p_status text, p_rows integer default null, p_bytes bigint default null,
  p_quarantined integer default null, p_r2_rows integer default null, p_error text default null
) returns void language sql as $$
  insert into archive_days (day, status, rows_sent, bytes, quarantined, r2_rows, rows_match, last_error)
  values (p_day, p_status, p_rows, p_bytes, p_quarantined, p_r2_rows,
          case when p_r2_rows is null or p_rows is null then null else p_r2_rows = p_rows end, p_error)
  on conflict (day) do update set
    status      = excluded.status,
    rows_sent   = coalesce(excluded.rows_sent, archive_days.rows_sent),
    bytes       = coalesce(excluded.bytes, archive_days.bytes),
    quarantined = coalesce(excluded.quarantined, archive_days.quarantined),
    r2_rows     = coalesce(excluded.r2_rows, archive_days.r2_rows),
    rows_match  = coalesce(excluded.rows_match, archive_days.rows_match),
    attempts    = archive_days.attempts + 1,
    last_error  = excluded.last_error,
    updated_at  = now();
$$;
revoke all on function public.record_archive_day(date, text, integer, bigint, integer, integer, text) from public, anon, authenticated;
grant execute on function public.record_archive_day(date, text, integer, bigint, integer, integer, text) to service_role;

-- Pairs of mandis in the same state whose names match once noise words are removed. The district often differs
-- because it was renamed or spelt differently (Hoshangabad / Narmadapuram), so it is shown, not required.
-- Found automatically, reviewed later by hand; merging them is never done here.
create table if not exists public.mandi_duplicate_candidates (
  mandi_a     bigint not null references public.mandis(id),
  mandi_b     bigint not null references public.mandis(id),
  match_key   text not null,
  status      text not null default 'PENDING' check (status in ('PENDING', 'SAME', 'DIFFERENT', 'MERGED')),
  note        text,
  districts   text,
  found_at    timestamptz not null default now(),
  primary key (mandi_a, mandi_b),
  check (mandi_a < mandi_b)
);
alter table public.mandi_duplicate_candidates enable row level security;
revoke all on public.mandi_duplicate_candidates from anon, authenticated;

create or replace function public.find_mandi_duplicates() returns integer language plpgsql as $$
declare n integer;
begin
  insert into mandi_duplicate_candidates (mandi_a, mandi_b, match_key, districts)
  select a.id, b.id, _mandi_match_key(a.normalized_name), concat_ws(' / ', a.district, b.district)
    from mandis a join mandis b
      on a.id < b.id and a.state = b.state
     and _mandi_match_key(a.normalized_name) = _mandi_match_key(b.normalized_name)
   where a.merged_into_id is null and b.merged_into_id is null
  on conflict do nothing;
  get diagnostics n = row_count;
  return n;
end $$;
revoke all on function public.find_mandi_duplicates() from public, anon, authenticated;
grant execute on function public.find_mandi_duplicates() to service_role;
