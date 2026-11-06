-- One-time merge of mandis split by district relabels (data only; needs 20261106090000 first).
-- Survivor = the row the feed uses now (latest price date, then most price rows). union_merge_mandi moves the
-- warehouse rows; R2 history keeps old ids and publish/analytics follow merged_into_id, so no file is rewritten.

-- A. Renamed districts: same name in the current district -> merge; otherwise move the mandi to the current name.
do $$
declare r record; v_survivor bigint;
begin
  for r in
    select m.id, m.normalized_name, m.state, a.canonical_district
    from mandis m join district_aliases a on a.state = m.state and a.alias_district = m.district
    where m.status = 'ACTIVE'
  loop
    v_survivor := null;
    select id into v_survivor from mandis
    where status = 'ACTIVE' and state = r.state and district = r.canonical_district and normalized_name = r.normalized_name;
    if v_survivor is null then
      update mandis set district = r.canonical_district where id = r.id;
    else
      perform union_merge_mandi(v_survivor, r.id, 'district_renamed_2026-11-06');
    end if;
  end loop;
end $$;

-- B. Markets moved into a newly carved district: one name, one state, several districts. District pairs below share
-- a name by coincidence (two real towns, or a feed slip) and stay separate pending review.
do $$
declare g record; v_survivor bigint; v_loser bigint;
begin
  for g in
    with skip(state, d1, d2) as (values
      ('Uttar Pradesh','Ghazipur','Hapur'), ('Telangana','Khammam','Mahbubnagar'), ('Madhya Pradesh','Khandwa','Khargone'),
      ('Telangana','Ranga Reddy','Sangareddy'), ('Karnataka','Chikkaballapur','Chikkamagaluru'), ('Rajasthan','Ganganagar','Hanumangarh'),
      ('Rajasthan','Jhalawar','Kota'), ('Rajasthan','Chittorgarh','Udaipur'), ('Rajasthan','Bikaner','Jhunjhunu'),
      ('Telangana','Bhupalapally','Karimnagar'), ('Rajasthan','Alwar','Bharatpur'), ('Rajasthan','Ajmer','Bhilwara'),
      ('Rajasthan','Pratapgarh','Udaipur'), ('Jammu and Kashmir','Jammu','Ramban'), ('Telangana','Karimnagar','Ranga Reddy'),
      ('Assam','Goalpara','Sibsagar'), ('Andhra Pradesh','Chittor','Palnadu'), ('Keralam','Idukki','Kottayam'),
      ('Rajasthan','Jhunjhunu','Sikar')),
    grp as (
      select m.state, m.normalized_name, array_agg(m.id order by p.last desc nulls last, p.n desc, m.id desc) ids,
             array_agg(distinct m.district) dists
      from mandis m
      left join lateral (select max(price_date) last, count(*) n from mandi_daily_prices where mandi_id = m.id) p on true
      where m.status = 'ACTIVE'
      group by m.state, m.normalized_name
      having count(distinct m.district) > 1)
    select * from grp
    where not exists (select 1 from skip s where s.state = grp.state and s.d1 = any(grp.dists) and s.d2 = any(grp.dists))
  loop
    v_survivor := g.ids[1];
    foreach v_loser in array g.ids[2:] loop
      perform union_merge_mandi(v_survivor, v_loser, 'district_split_moved_2026-11-06');
    end loop;
  end loop;
end $$;

-- C. Renamed markets (the name changed too). Tamil Nadu relabelled every "X APMC" as "X RM" (regulated market);
-- none of the 31 pairs ever reported on the same day. Plus the Bengaluru/Ballari renames.
do $$
declare r record;
begin
  for r in
    select a.id loser, b.id survivor from mandis a join mandis b
      on a.state = 'Tamil Nadu' and b.state = a.state and a.id <> b.id and b.normalized_name = trim(a.normalized_name) || ' rm'
    where a.status = 'ACTIVE' and b.status = 'ACTIVE'
    union all
    select l, s from (values (3482::bigint, 541::bigint), (3484, 533), (3526, 529), (3491, 548), (3572, 2632)) v(l, s)
    where exists (select 1 from mandis where id = v.l and status = 'ACTIVE')
  loop
    perform union_merge_mandi(r.survivor, r.loser, 'market_renamed_2026-11-06');
  end loop;
end $$;
