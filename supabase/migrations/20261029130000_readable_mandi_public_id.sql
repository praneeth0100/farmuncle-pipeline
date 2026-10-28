-- Readable, frozen public ids for mandis: state.district.market (spec section 3), e.g. ka.dharwad.hubli.
-- Internal numeric ids are unchanged; public_id is what published files use. Set once, never changed.

create table if not exists state_codes (
    state text primary key,
    code text not null unique
);

insert into state_codes (state, code) values
    ('Andhra Pradesh', 'ap'), ('Arunachal Pradesh', 'ar'), ('Assam', 'as'), ('Bihar', 'br'),
    ('Chattisgarh', 'cg'), ('Goa', 'ga'), ('Gujarat', 'gj'), ('Haryana', 'hr'),
    ('Himachal Pradesh', 'hp'), ('Jharkhand', 'jh'), ('Karnataka', 'ka'), ('Keralam', 'kl'),
    ('Madhya Pradesh', 'mp'), ('Maharashtra', 'mh'), ('Manipur', 'mn'), ('Meghalaya', 'ml'),
    ('Mizoram', 'mz'), ('Nagaland', 'nl'), ('Odisha', 'od'), ('Punjab', 'pb'), ('Rajasthan', 'rj'),
    ('Sikkim', 'sk'), ('Tamil Nadu', 'tn'), ('Telangana', 'tg'), ('Tripura', 'tr'),
    ('Uttar Pradesh', 'up'), ('Uttarakhand', 'uk'), ('West Bengal', 'wb'), ('NCT of Delhi', 'dl'),
    ('Jammu and Kashmir', 'jk'), ('Puducherry', 'py'), ('Chandigarh', 'ch'), ('Andaman and Nicobar', 'an')
on conflict (state) do nothing;

create or replace function slugify_public_id(p text) returns text
language sql immutable as $$
    select trim(both '_' from regexp_replace(lower(coalesce(p, '')), '[^a-z0-9]+', '_', 'g'));
$$;

create or replace function mandi_public_id(p_state text, p_district text, p_normalized_name text, p_id bigint) returns text
language sql stable as $$
    select coalesce((select code from state_codes where state = p_state), slugify_public_id(p_state))
        || '.' || coalesce(nullif(slugify_public_id(p_district), ''), 'unknown')
        || '.' || coalesce(nullif(slugify_public_id(p_normalized_name), ''), 'mandi_' || p_id);
$$;

alter table mandis add column if not exists public_id text;

update mandis set public_id = mandi_public_id(state, district, normalized_name, id) where public_id is null;

-- Refuse to finish if two mandis would share a public id (a readable id must identify one mandi).
do $$
begin
    if exists (select 1 from mandis group by public_id having count(*) > 1) then
        raise exception 'public_id collision: readable mandi ids are not unique';
    end if;
end $$;

alter table mandis alter column public_id set not null;
create unique index if not exists uq_mandis_public_id on mandis (public_id);

-- New mandis (find_or_create_mandi) get their readable id on insert. A collision adds the numeric id.
create or replace function mandis_set_public_id() returns trigger
language plpgsql as $$
begin
    if new.public_id is null then
        new.public_id := mandi_public_id(new.state, new.district, new.normalized_name, new.id);
    end if;
    if exists (select 1 from mandis where public_id = new.public_id) then
        new.public_id := new.public_id || '_' || new.id;
    end if;
    return new;
end $$;

drop trigger if exists trg_mandis_set_public_id on mandis;
create trigger trg_mandis_set_public_id before insert on mandis
    for each row execute function mandis_set_public_id();
