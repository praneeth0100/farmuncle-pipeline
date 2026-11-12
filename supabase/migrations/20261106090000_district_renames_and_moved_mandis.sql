-- One market, two mandi rows: the feed renamed districts (Belgaum -> Belagavi) or moved a market into a newly
-- carved district (Hospet: Bellary -> Vijayanagara), and find_or_create_mandi only matches inside one district,
-- so each new label made a second mandi. 404 such same-name pairs on 2026-11-06.
--
-- 1. Renamed districts get a district_aliases row: normalize_district already applies it, so every later run
--    (live and backfill) files the old spelling under the current one.
-- 2. A merged mandi now remembers where it was: find_or_create_mandi follows a MERGED row with the same name,
--    state and district to its survivor. A split district (Bellary still exists) cannot be aliased whole, so this
--    per-mandi memory is what keeps Hospet-in-Bellary on the Vijayanagara mandi. It is state- and district-scoped,
--    unlike mandi_aliases, which match a name in any state.
-- 3. union_merge_mandi no longer writes a global alias when both rows already share the normalized name (step 2
--    covers it, and the old ON CONFLICT DO UPDATE could repoint another state's alias of the same name).

create or replace function public.find_or_create_mandi(p_name text, p_state text, p_district text, p_lat double precision, p_lng double precision, p_source text)
 returns bigint
 language plpgsql
as $function$
declare
    v_key text := _mandi_match_key(p_name);
    v_district text := normalize_district(p_state, p_district);
    v_id bigint;
    v_match_id bigint;
    v_sim numeric;
    v_status text;
    v_next bigint;
begin
    -- 1. exact business-key match on an active mandi
    select id into v_id from mandis
    where normalized_name = v_key and state = p_state and district = v_district
      and status = 'ACTIVE'
    limit 1;

    if v_id is not null then
        update mandis set last_seen_at = now() where id = v_id;
        return v_id;
    end if;

    -- 1b. this name in this district was merged into another mandi: follow the chain to the active survivor
    select merged_into_id into v_id from mandis
    where normalized_name = v_key and state = p_state and district = v_district and status = 'MERGED'
    limit 1;
    for i in 1..10 loop  -- bounded: a merge chain is one or two hops
        exit when v_id is null;
        select status, merged_into_id into v_status, v_next from mandis where id = v_id;
        exit when v_status = 'ACTIVE';
        v_id := v_next;
    end loop;

    if v_id is not null and v_status = 'ACTIVE' then
        update mandis set last_seen_at = now() where id = v_id;
        return v_id;
    end if;
    v_id := null;

    -- 2. approved alias match
    select mandi_id into v_id from mandi_aliases
    where normalized_alias = v_key and approved = true
    limit 1;

    if v_id is not null then
        update mandis set last_seen_at = now() where id = v_id;
        return v_id;
    end if;

    -- 3. fuzzy match within the same district -- log an UNAPPROVED alias,
    -- still resolve to the match so ingestion isn't blocked, but the pair
    -- is now visible for review (never silently guessed with no trail)
    select m.id, similarity(m.normalized_name, v_key) into v_match_id, v_sim
    from mandis m
    where m.state = p_state and m.district = v_district and m.status = 'ACTIVE'
    order by similarity(m.normalized_name, v_key) desc
    limit 1;

    if v_match_id is not null and v_sim > 0.5 then
        insert into mandi_aliases (mandi_id, alias_name, normalized_alias, match_method, match_confidence, approved, source)
        values (v_match_id, p_name, v_key, 'FUZZY', v_sim, false, p_source)
        on conflict (normalized_alias) do nothing;
        update mandis set last_seen_at = now() where id = v_match_id;
        return v_match_id;
    end if;

    -- 4. genuinely new mandi
    insert into mandis (slug, name, normalized_name, state, district, latitude, longitude, ingested_from)
    values (
        v_key || '-' || substr(md5(random()::text), 1, 6),
        p_name, v_key, p_state, v_district, p_lat, p_lng, p_source
    )
    returning id into v_id;

    return v_id;
end;
$function$;

create or replace function public.union_merge_mandi(p_survivor_id bigint, p_loser_id bigint, p_reason text default 'apmc_fv_union_merge'::text)
 returns table(deduped_identical bigint, kept_both_differing bigint, moved_no_collision bigint)
 language plpgsql
 set search_path to 'public'
as $function$
declare
  v_deduped bigint := 0;
  v_kept_both bigint := 0;
  v_moved bigint := 0;
  v_loser_name text;
  v_loser_norm text;
begin
  select name, normalized_name into v_loser_name, v_loser_norm from mandis where id = p_loser_id;

  update mandi_daily_prices set source_mandi_id = mandi_id, source_mandi_name = (select name from mandis where id = mandi_id)
  where mandi_id in (p_survivor_id, p_loser_id) and source_mandi_id is null;

  with dupes as (
    select l.id as loser_row_id
    from mandi_daily_prices l
    join mandi_daily_prices s
      on s.mandi_id = p_survivor_id and l.mandi_id = p_loser_id
     and s.crop_id = l.crop_id and s.variety = l.variety and s.grade = l.grade and s.price_date = l.price_date
     and s.modal_price is not distinct from l.modal_price
     and s.min_price is not distinct from l.min_price
     and s.max_price is not distinct from l.max_price
  )
  delete from mandi_daily_prices where id in (select loser_row_id from dupes);
  get diagnostics v_deduped = row_count;

  select count(*) into v_kept_both
  from mandi_daily_prices l
  join mandi_daily_prices s
    on s.mandi_id = p_survivor_id and l.mandi_id = p_loser_id
   and s.crop_id = l.crop_id and s.variety = l.variety and s.grade = l.grade and s.price_date = l.price_date;

  update mandi_daily_prices set mandi_id = p_survivor_id, updated_at = now() where mandi_id = p_loser_id;
  get diagnostics v_moved = row_count;

  update mandis set status = 'MERGED', merged_into_id = p_survivor_id, merge_reason = p_reason, merged_at = now()
  where id = p_loser_id;

  update mandi_aliases set mandi_id = p_survivor_id where mandi_id = p_loser_id;

  -- Same name in another district: find_or_create_mandi step 1b follows the MERGED row; no global alias needed.
  if v_loser_norm is distinct from (select normalized_name from mandis where id = p_survivor_id) then
    insert into mandi_aliases (mandi_id, alias_name, normalized_alias, match_method, match_confidence, approved, source)
    values (p_survivor_id, v_loser_name, v_loser_norm, 'MANUAL', 1.0, true, 'auto_created_on_merge')
    on conflict (normalized_alias) do update set mandi_id = p_survivor_id, approved = true;
  end if;

  return query select v_deduped, v_kept_both, v_moved;
end;
$function$;

-- Old spelling -> the spelling the feed uses now (latest price date on 2026-11-06; ties to the official name).
insert into district_aliases (state, alias_district, canonical_district, reason) values
  ('Andhra Pradesh', 'Anantapur', 'Ananthapuramu', 'rename'),
  ('Andhra Pradesh', 'Cuddapah', 'YSR Kadapa', 'rename'),
  ('Andhra Pradesh', 'Nellore', 'SPSR Nellore', 'rename'),
  ('Assam', 'MORIGAON', 'Morigaon', 'letter case'),
  ('Chattisgarh', 'Janjgir', 'Janjgeer-Champa', 'short form of Janjgir-Champa'),
  ('Chattisgarh', 'Koria', 'Koriya', 'spelling'),
  ('Jammu and Kashmir', 'Badgam', 'Budgam', 'spelling'),
  ('Karnataka', 'Belgaum', 'Belagavi', 'rename'),
  ('Karnataka', 'Bijapur', 'Vijayapura', 'rename'),
  ('Karnataka', 'Chamrajnagar', 'Chamarajanagar', 'spelling'),
  ('Karnataka', 'Chikmagalur', 'Chikkamagaluru', 'rename'),
  ('Karnataka', 'Mangalore(Dakshin Kannad)', 'Dakshina Kannada', 'old label'),
  ('Karnataka', 'Kalburgi', 'Kalaburagi', 'spelling'),
  ('Karnataka', 'Karwar(Uttar Kannad)', 'Uttara Kannada', 'old label'),
  ('Karnataka', 'Madikeri(Kodagu)', 'Kodagu', 'old label'),
  ('Karnataka', 'Mysore', 'Mysuru', 'rename'),
  ('Karnataka', 'Shimoga', 'Shivamogga', 'rename'),
  ('Karnataka', 'Tumkur', 'Tumakuru', 'rename'),
  ('Karnataka', 'Yadgiri', 'Yadgir', 'spelling'),
  ('Madhya Pradesh', 'Hoshangabad', 'Narmadapuram', 'rename'),
  ('Maharashtra', 'Ahmednagar', 'Ahilyanagar', 'rename'),
  ('Maharashtra', 'Dharashiv(Usmanabad)', 'Dharashiv', 'old label'),
  ('Maharashtra', 'Jalana', 'Jalna', 'spelling'),
  ('Maharashtra', 'Sholapur', 'Solapur', 'spelling'),
  ('Maharashtra', 'Vashim', 'Washim', 'spelling'),
  ('Punjab', 'kapurthala', 'Kapurthala', 'letter case'),
  ('Rajasthan', 'Jhunjunu', 'Jhunjhunu', 'spelling'),
  ('Rajasthan', 'Rajasamand', 'Rajsamand', 'spelling'),
  ('Tamil Nadu', 'Chengalpattu', 'Chengalpet', 'spelling'),
  ('Tamil Nadu', 'Kallakuruchi', 'Kallakurichi', 'spelling'),
  ('Tamil Nadu', 'Nagercoil (Kannyiakumari)', 'Kannyiakumari', 'old label'),
  ('Tamil Nadu', 'Sivaganga', 'Sivagangai', 'spelling'),
  ('Tamil Nadu', 'Tenkasi', 'Thenkasi', 'spelling'),
  ('Tamil Nadu', 'Thirunelveli', 'Tirunelveli', 'spelling'),
  ('Tamil Nadu', 'Thirupur', 'Thiruppur', 'spelling'),
  ('Tamil Nadu', 'Thiruvarur', 'Tiruvarur', 'spelling'),
  ('Tamil Nadu', 'Tuticorin', 'Thoothukudi', 'rename'),
  ('Telangana', 'Jayashankar Bhupalapally', 'Bhupalapally', 'long form'),
  ('Telangana', 'Hanmakonda', 'Hanumakonda', 'spelling'),
  ('Telangana', 'Jagityal', 'Jagtial', 'spelling'),
  ('Tripura', 'South District', 'South Tripura', 'old label'),
  ('Uttarakhand', 'UdhamSinghNagar', 'Udhamsinghnagar', 'letter case')
on conflict (state, alias_district) do nothing;
