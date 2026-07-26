-- price_cache had only denormalized variety/grade TEXT columns, matched against
-- varieties.name/grades.name by string equality — fragile (casing, whitespace,
-- silent mismatch). mandi_daily_prices already carries variety_id/grade_id from
-- an earlier backfill (100% populated, confirmed 1:1 with no ambiguous text
-- combos mapping to multiple ids). This migration gives price_cache the same
-- real foreign keys.

alter table public.price_cache
  add column if not exists variety_id bigint references public.varieties(id),
  add column if not exists grade_id bigint references public.grades(id);

-- Backfill from mandi_daily_prices (most recent row per combo).
update public.price_cache pc
set variety_id = src.variety_id,
    grade_id = src.grade_id
from (
    select distinct on (mandi_id, crop_id, coalesce(variety,''), coalesce(grade,''))
        mandi_id, crop_id, coalesce(variety,'') as variety, coalesce(grade,'') as grade,
        variety_id, grade_id
    from public.mandi_daily_prices
    order by mandi_id, crop_id, coalesce(variety,''), coalesce(grade,''), price_date desc
) src
where pc.mandi_id = src.mandi_id
  and pc.crop_id = src.crop_id
  and pc.variety = src.variety
  and pc.grade = src.grade;

-- Replace refresh_price_cache() to carry variety_id/grade_id forward on every future run,
-- not just this one-time backfill.
create or replace function public.refresh_price_cache()
returns void
language plpgsql
as $function$
begin
    insert into price_cache (mandi_id, crop_id, variety, grade, variety_id, grade_id,
                              latest_price_date, modal_price, min_price, max_price,
                              source_mandi_name, refreshed_at)
    select distinct on (mandi_id, crop_id, coalesce(variety,''), coalesce(grade,''))
        mandi_id, crop_id, coalesce(variety,''), coalesce(grade,''), variety_id, grade_id,
        price_date, modal_price, min_price, max_price, source_mandi_name, now()
    from mandi_daily_prices
    order by mandi_id, crop_id, coalesce(variety,''), coalesce(grade,''), price_date desc
    on conflict (mandi_id, crop_id, variety, grade) do update
        set variety_id = excluded.variety_id,
            grade_id = excluded.grade_id,
            latest_price_date = excluded.latest_price_date,
            modal_price = excluded.modal_price,
            min_price = excluded.min_price,
            max_price = excluded.max_price,
            source_mandi_name = excluded.source_mandi_name,
            refreshed_at = now();

    -- tolerance-window change computation (unchanged from prior version)
    update price_cache pc set
        change_1d = pc.modal_price - prior.modal_price,
        change_1d_pct = case when prior.modal_price > 0 then round(100.0*(pc.modal_price - prior.modal_price)/prior.modal_price, 2) end,
        last_recomputed_at = now()
    from (
        select distinct on (mandi_id, crop_id, coalesce(variety,''), coalesce(grade,''))
            mandi_id, crop_id, coalesce(variety,'') as variety, coalesce(grade,'') as grade, modal_price
        from mandi_daily_prices p
        where p.price_date between (select max(price_date) from mandi_daily_prices) - 3 and (select max(price_date) from mandi_daily_prices) - 1
        order by mandi_id, crop_id, coalesce(variety,''), coalesce(grade,''), price_date desc
    ) prior
    where pc.mandi_id = prior.mandi_id and pc.crop_id = prior.crop_id and pc.variety = prior.variety and pc.grade = prior.grade;

    update price_cache pc set
        change_7d = pc.modal_price - prior.modal_price,
        change_7d_pct = case when prior.modal_price > 0 then round(100.0*(pc.modal_price - prior.modal_price)/prior.modal_price, 2) end
    from (
        select distinct on (mandi_id, crop_id, coalesce(variety,''), coalesce(grade,''))
            mandi_id, crop_id, coalesce(variety,'') as variety, coalesce(grade,'') as grade, modal_price
        from mandi_daily_prices p
        where p.price_date between (select max(price_date) from mandi_daily_prices) - 10 and (select max(price_date) from mandi_daily_prices) - 4
        order by mandi_id, crop_id, coalesce(variety,''), coalesce(grade,''), price_date desc
    ) prior
    where pc.mandi_id = prior.mandi_id and pc.crop_id = prior.crop_id and pc.variety = prior.variety and pc.grade = prior.grade;

    update price_cache pc set
        change_1m = pc.modal_price - prior.modal_price,
        change_1m_pct = case when prior.modal_price > 0 then round(100.0*(pc.modal_price - prior.modal_price)/prior.modal_price, 2) end
    from (
        select distinct on (mandi_id, crop_id, coalesce(variety,''), coalesce(grade,''))
            mandi_id, crop_id, coalesce(variety,'') as variety, coalesce(grade,'') as grade, modal_price
        from mandi_daily_prices p
        where p.price_date between (select max(price_date) from mandi_daily_prices) - 35 and (select max(price_date) from mandi_daily_prices) - 25
        order by mandi_id, crop_id, coalesce(variety,''), coalesce(grade,''), price_date desc
    ) prior
    where pc.mandi_id = prior.mandi_id and pc.crop_id = prior.crop_id and pc.variety = prior.variety and pc.grade = prior.grade;
end;
$function$;
