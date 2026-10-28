-- 1. The canonical key is the only unique key now (the writer on main uses it).
alter table mandi_daily_prices drop constraint if exists uq_prices_business_key_v2;

-- 2. Reference statistics that survive wipes and archiving: crop medians (for peer_scale) and the
--    market+crop pairs learned to report per kg. Without them a freshly emptied warehouse has no peers.
create table if not exists crop_price_reference (
  crop_id bigint primary key references crops(id),
  median numeric not null,
  n int not null,
  updated_at timestamptz not null default now()
);
create table if not exists per_kg_pair_reference (
  mandi_id bigint not null references mandis(id),
  crop_id bigint not null references crops(id),
  updated_at timestamptz not null default now(),
  primary key (mandi_id, crop_id)
);
alter table crop_price_reference enable row level security;
alter table per_kg_pair_reference enable row level security;

-- live-only versions (renamed bodies), then wrappers that add the reference tables.
create or replace function live_crop_medians(p_days int default 60, p_min_rows int default 30)
returns table (crop_id bigint, median numeric) language sql stable as $$
  select p.crop_id, percentile_cont(0.5) within group (order by p.modal_price)::numeric
    from mandi_daily_prices p
   where p.unit = 'kg' and p.modal_price >= 1 and p.price_date >= current_date - p_days
   group by p.crop_id
  having count(*) >= p_min_rows
$$;

create or replace function crop_price_medians(p_days int default 60, p_min_rows int default 30)
returns table (crop_id bigint, median numeric) language sql stable as $$
  select l.crop_id, l.median from live_crop_medians(p_days, p_min_rows) l
  union all
  select r.crop_id, r.median from crop_price_reference r
   where not exists (select 1 from live_crop_medians(p_days, p_min_rows) l where l.crop_id = r.crop_id)
$$;

create or replace function learned_per_kg_pairs(p_min_days int default 3, p_min_share numeric default 0.8)
returns table (mandi_id bigint, crop_id bigint) language sql stable as $$
  with obs as (
    select p.mandi_id, p.crop_id, p.price_date,
           p.modal_price * case when coalesce(p.quality_components->>'unit_reported_kg', '') = 'true' then 1 else 100 end as raw
      from mandi_daily_prices p
     where p.unit = 'kg' and p.price_date >= current_date - 180
    union all
    select (q.row_data->>'mandi_id')::bigint, (q.row_data->>'crop_id')::bigint,
           (q.row_data->>'price_date')::date, (q.row_data->>'modal_price')::numeric * 100
      from data_quality_issues q
     where q.error_code = 'DQ-PRICE-FLOOR' and q.review_status = 'PENDING' and q.row_data->>'unit' = 'kg'
  )
  select o.mandi_id, o.crop_id from obs o
   group by o.mandi_id, o.crop_id
  having count(distinct o.price_date) >= p_min_days and avg((o.raw >= 1 and o.raw < 100)::int) >= p_min_share
$$;

create or replace function per_kg_pairs(p_min_days int default 3, p_min_share numeric default 0.8)
returns table (mandi_id bigint, crop_id bigint) language sql stable as $$
  select l.mandi_id, l.crop_id from learned_per_kg_pairs(p_min_days, p_min_share) l
  union
  select r.mandi_id, r.crop_id from per_kg_pair_reference r
$$;

create or replace function refresh_price_reference() returns void language plpgsql as $$
begin
  insert into crop_price_reference (crop_id, median, n, updated_at)
  select p.crop_id, percentile_cont(0.5) within group (order by p.modal_price)::numeric, count(*), now()
    from mandi_daily_prices p
   where p.unit = 'kg' and p.modal_price >= 1
   group by p.crop_id having count(*) >= 30
  on conflict (crop_id) do update set median = excluded.median, n = excluded.n, updated_at = now();

  insert into per_kg_pair_reference (mandi_id, crop_id)
  select mandi_id, crop_id from learned_per_kg_pairs() on conflict do nothing;
end $$;

-- 3. Remove one archived window from the warehouse (prices, raw entries, quarantine rows). Never the
--    live window: the last 10 days stay so the app always has current prices.
create or replace function delete_price_window(p_start date, p_end date) returns jsonb language plpgsql as $$
declare n_prices bigint; n_raw bigint; n_dq bigint;
begin
  if p_end < p_start then raise exception 'delete_price_window: end % is before start %', p_end, p_start; end if;
  if p_end > current_date - 10 then
    raise exception 'delete_price_window: refusing to delete inside the live window (end % is within the last 10 days)', p_end;
  end if;
  perform set_config('app.allow_raw_purge', 'on', true);
  delete from mandi_daily_prices where price_date between p_start and p_end;
  get diagnostics n_prices = row_count;
  delete from raw_price_entries where price_date between p_start and p_end;
  get diagnostics n_raw = row_count;
  delete from data_quality_issues where row_data ? 'price_date' and (row_data->>'price_date')::date between p_start and p_end;
  get diagnostics n_dq = row_count;
  return jsonb_build_object('prices', n_prices, 'raw', n_raw, 'quarantine', n_dq);
end $$;

do $$ begin
  revoke execute on function refresh_price_reference() from public, anon, authenticated;
  grant execute on function refresh_price_reference() to service_role;
  revoke execute on function delete_price_window(date, date) from public, anon, authenticated;
  grant execute on function delete_price_window(date, date) to service_role;
  revoke execute on function live_crop_medians(int, int) from public, anon, authenticated;
  revoke execute on function learned_per_kg_pairs(int, numeric) from public, anon, authenticated;
  revoke execute on function crop_price_medians(int, int) from public, anon, authenticated;
  grant execute on function crop_price_medians(int, int) to service_role;
  revoke execute on function per_kg_pairs(int, numeric) from public, anon, authenticated;
  grant execute on function per_kg_pairs(int, numeric) to service_role;
end $$;

-- 4. Take the snapshot now (before any wipe).
select refresh_price_reference();
