-- Per-kg entries are only real in a short list of markets (PER_KG_MARKETS in government_constants.py).
-- Rows restored from quarantine for any other market were quintal-market slips: put them back.
create temporary table per_kg_markets (state text, market text) on commit drop;
insert into per_kg_markets values
  ('Maharashtra','Pune APMC'),('Maharashtra','Pune(Pimpri) APMC'),('Maharashtra','Pune(Manjri) APMC'),
  ('Maharashtra','Pune(Khadiki) APMC'),('Maharashtra','Pune(Moshi) APMC'),('Maharashtra','Akluj APMC'),
  ('Maharashtra','Shrirampur APMC'),('Maharashtra','Rahata APMC'),('Maharashtra','Patan APMC'),
  ('Goa','Mapusa APMC'),('Goa','Margao APMC'),('Goa','Curchorem APMC'),('Goa','Canacona APMC'),
  ('Goa','Pernem APMC'),('Goa','Sanquelim APMC'),
  ('NCT of Delhi','Flower Market,Gazipur APMC'),
  ('Andaman and Nicobar','Diglipur Vegetable Market (Subhashgram)'),
  ('Haryana','Rania APMC'),('Haryana','Rania(Jiwan nagar) APMC'),
  ('Telangana','RYTHU BAZAR ERRAGADDA');

with doomed as (
  select p.id from mandi_daily_prices p join mandis m on m.id = p.mandi_id
   where p.quality_components->>'unit_reported_kg' = 'true'
     and not exists (select 1 from per_kg_markets k where k.state = m.state and k.market = m.name)
), gone as (
  delete from mandi_daily_prices where id in (select id from doomed) returning mandi_id, crop_id, variety, grade, price_date, source_mandi_id
)
update data_quality_issues q
   set review_status = 'PENDING', reviewed_at = null, review_note = 'not a per-kg market: slip, back in quarantine'
 where q.review_status = 'CORRECTED' and q.error_code = 'DQ-PRICE-FLOOR'
   and exists (select 1 from gone g
                where g.mandi_id = (q.row_data->>'mandi_id')::bigint and g.crop_id = (q.row_data->>'crop_id')::bigint
                  and g.price_date = (q.row_data->>'price_date')::date and g.variety = q.row_data->>'variety'
                  and coalesce(g.grade,'') = coalesce(q.row_data->>'grade','')
                  and g.source_mandi_id = (q.row_data->>'source_mandi_id')::bigint);
