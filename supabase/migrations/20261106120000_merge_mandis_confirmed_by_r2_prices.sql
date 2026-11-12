-- 49 more merges, each confirmed by R2 history: both mandis traded the same days at the same prices
-- (mostly 100% of shared crop prices). Survivor = the copy the feed used most recently. Kept apart despite
-- similar prices: Mullana/Jullana, Mannargudi I/II, Madhubani/Madhuban, Barad/Bairad (separate towns or thin evidence).
do $$ declare r record; begin
  for r in select s, l from (values (3879,3880), (886,900), (977,2771), (1445,4111), (1446,4124), (1465,4109), (1482,4110), (1484,4118), (1510,4112), (1533,3563), (1533,4121), (2600,3550), (2371,3694), (3035,3424), (3427,3432), (1876,4168), (3823,4164), (237,3324), (247,3334), (251,3335), (295,3331), (538,3481), (540,3471), (543,3470), (571,3490), (594,3535), (2633,3557), (2826,847), (972,899), (2837,934), (939,3295), (1018,2703), (2990,1139), (3142,1201), (2620,3654), (2923,1575), (2900,1690), (2944,1696), (3085,1736), (3184,2538), (2765,3222), (3091,3218), (3150,3213), (3185,3210), (3230,3231), (3282,3284), (3425,3440), (1850,3708), (2001,3926)) v(s, l)
    where exists (select 1 from mandis where id = v.l and status = 'ACTIVE') and exists (select 1 from mandis where id = v.s and status = 'ACTIVE')
  loop
    perform union_merge_mandi(r.s::bigint, r.l::bigint, 'same_market_by_r2_prices_2026-11-06');
  end loop; end $$;
