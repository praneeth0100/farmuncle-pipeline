-- Merging mandis/crops/varieties/grades re-points prices (UPDATE mandi_id/crop_id/variety_id/grade_id).
-- With one row per canonical key that can collide. Resolve it by source precedence (manual >
-- resource_2 > resource_1); on a tie the row already there (the survivor's) wins. The loser is logged.
create or replace function price_rank(p_source text) returns int language sql immutable as $$
  select case p_source when 'manual' then 3 when 'resource_2' then 2 when 'resource_1' then 1 else 0 end
$$;

create or replace function resolve_price_key_collision() returns trigger language plpgsql as $$
declare other mandi_daily_prices%rowtype;
begin
  if (new.mandi_id, new.crop_id, new.variety_id, new.grade_id)
       is not distinct from (old.mandi_id, old.crop_id, old.variety_id, old.grade_id) then
    return new;
  end if;

  select * into other from mandi_daily_prices
   where mandi_id = new.mandi_id and crop_id = new.crop_id and variety_id = new.variety_id
     and grade_id = new.grade_id and price_date = new.price_date and id <> old.id;
  if not found then
    return new;
  end if;

  if price_rank(new.source) > price_rank(other.source) then
    insert into data_quality_issues (batch_id, resource, row_data, error_code, error_message)
    values (other.batch_id, other.source, to_jsonb(other), 'DQ-MERGE-COLLISION', 'replaced by a higher-precedence row during an entity merge');
    delete from mandi_daily_prices where id = other.id;
    return new;
  end if;

  insert into data_quality_issues (batch_id, resource, row_data, error_code, error_message)
  values (old.batch_id, old.source, to_jsonb(old), 'DQ-MERGE-COLLISION', 'dropped during an entity merge: the surviving entity already has this price');
  delete from mandi_daily_prices where id = old.id;
  return null;
end $$;

create trigger trg_price_key_collision
  before update of mandi_id, crop_id, variety_id, grade_id on mandi_daily_prices
  for each row execute function resolve_price_key_collision();
