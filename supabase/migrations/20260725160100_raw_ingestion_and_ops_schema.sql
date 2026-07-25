-- Raw ingestion + ops layer. Column names here were cross-checked directly against
-- every real .table()/.insert()/.update() call site in farmuncle_pipeline/core/*.py
-- and farmuncle_pipeline/ingestion/*.py on 2026-07-25 -- this is what caught the
-- completed_at/error_summary vs finished_at/notes mismatch (and several others)
-- before any real pipeline run could hit them.

create table raw_api_batches (
    id text primary key,  -- ULID
    job_name text not null,
    resource text not null,
    date_range_start date,
    date_range_end date,
    started_at timestamptz not null default now(),
    completed_at timestamptz,
    status text not null default 'RUNNING' check (status in ('RUNNING','SUCCESS','PARTIAL','FAILED')),
    total_pages int default 0,
    total_records int default 0,
    error_summary text,
    schema_version int not null default 1
);

create table ingestion_batches (
    id text primary key,  -- ULID
    job_name text not null,
    resource text,
    date_range_start date,
    date_range_end date,
    started_at timestamptz not null default now(),
    completed_at timestamptz,
    status text not null default 'RUNNING' check (status in ('RUNNING','SUCCESS','PARTIAL','FAILED')),
    rows_processed int default 0,
    rows_failed int default 0,
    error_summary text,
    schema_version int not null default 1
);
create index idx_ingestion_batches_status on ingestion_batches(status);
create index idx_ingestion_batches_job_name on ingestion_batches(job_name);
create index idx_ingestion_batches_started_at on ingestion_batches(started_at);
-- the real §12 concurrency mechanism -- one RUNNING row per job_name at a time
create unique index uq_ingestion_batches_running_job on ingestion_batches(job_name) where status = 'RUNNING';

create table raw_price_entries (
    id bigint generated always as identity primary key,
    resource text not null,
    market text not null,
    state text not null,
    district text not null,
    commodity text not null,
    raw_variety text,
    raw_grade text,
    price_date date not null,
    modal_price numeric,
    min_price numeric,
    max_price numeric,
    content_hash text not null,
    payload jsonb,
    parser_version int not null default 1,
    -- batch_id references raw_api_batches, NOT ingestion_batches -- confirmed
    -- against raw_dedup.py's docstring ("batch_id: the current run's
    -- raw_api_batches.id"), a real drift risk the same class as the ULID/bigint
    -- incident that took the old project down before.
    first_seen_batch_id text not null references raw_api_batches(id),
    last_seen_batch_id text not null references raw_api_batches(id),
    first_seen_at timestamptz not null default now(),
    last_seen_at timestamptz not null default now()
);
create unique index uq_raw_price_entries_identity on raw_price_entries (
    resource, market, state, coalesce(district,''), commodity,
    coalesce(raw_variety,''), coalesce(raw_grade,''), price_date, content_hash
);
create index idx_raw_price_entries_batch on raw_price_entries(first_seen_batch_id);

create or replace function trg_raw_price_entries_no_delete_fn() returns trigger as $$
begin
    raise exception 'raw_price_entries rows are immutable -- delete is not permitted';
end;
$$ language plpgsql;
create trigger trg_raw_price_entries_no_delete
    before delete on raw_price_entries
    for each row execute function trg_raw_price_entries_no_delete_fn();

create or replace function trg_raw_price_entries_no_edit_fn() returns trigger as $$
begin
    if new.payload is distinct from old.payload
       or new.content_hash is distinct from old.content_hash
       or new.first_seen_batch_id is distinct from old.first_seen_batch_id
       or new.first_seen_at is distinct from old.first_seen_at then
        raise exception 'raw_price_entries: only last_seen_batch_id/last_seen_at may be updated';
    end if;
    return new;
end;
$$ language plpgsql;
create trigger trg_raw_price_entries_no_edit
    before update on raw_price_entries
    for each row execute function trg_raw_price_entries_no_edit_fn();

create table api_call_logs (
    id bigint generated always as identity primary key,
    batch_id text references ingestion_batches(id),
    job_name text,
    resource text,
    state text,
    page int,
    duration_ms int,
    status text,
    rows int,
    error_code text,
    error_message text,
    called_at timestamptz not null default now()
);
create index idx_api_call_logs_batch_id on api_call_logs(batch_id);
create index idx_api_call_logs_status on api_call_logs(status);
create index idx_api_call_logs_called_at on api_call_logs(called_at);
create index idx_api_call_logs_error_code on api_call_logs(error_code);

create table failed_pages (
    id bigint generated always as identity primary key,
    batch_id text not null references ingestion_batches(id),
    resource text not null,
    state text,
    page int,
    error_code text,
    error_message text,
    status text not null default 'PENDING' check (status in ('PENDING','RESOLVED')),
    created_at timestamptz not null default now(),
    resolved_at timestamptz
);
create index idx_failed_pages_status on failed_pages(status);
create index idx_failed_pages_batch_id on failed_pages(batch_id);

create table price_cache (
    mandi_id bigint not null references mandis(id),
    crop_id bigint not null references crops(id),
    variety text not null default '',
    grade text not null default '',
    latest_price_date date,
    modal_price numeric,
    min_price numeric,
    max_price numeric,
    source_mandi_name text,
    change_1d numeric,
    change_1d_pct numeric,
    change_7d numeric,
    change_7d_pct numeric,
    change_1m numeric,
    change_1m_pct numeric,
    last_recomputed_at timestamptz,
    refreshed_at timestamptz not null default now(),
    primary key (mandi_id, crop_id, variety, grade)
);

create table quality_alerts (
    id bigint generated always as identity primary key,
    batch_id text references ingestion_batches(id),
    coverage_report_id bigint,
    severity text check (severity in ('LOW','MEDIUM','HIGH','CRITICAL')),
    status text not null default 'OPEN' check (status in ('OPEN','ACKNOWLEDGED','RESOLVED')),
    error_code text,
    message text,
    created_at timestamptz not null default now()
);
create index idx_quality_alerts_severity on quality_alerts(severity);
create index idx_quality_alerts_created_at on quality_alerts(created_at);
create index idx_quality_alerts_status on quality_alerts(status);
create index idx_quality_alerts_batch_id on quality_alerts(batch_id);

create table coverage_reports (
    id bigint generated always as identity primary key,
    batch_id text references ingestion_batches(id),
    report_date date,
    expected_count int,
    actual_count int,
    created_at timestamptz not null default now()
);
create index idx_coverage_reports_report_date on coverage_reports(report_date);
create index idx_coverage_reports_batch_id on coverage_reports(batch_id);

alter table quality_alerts add constraint fk_quality_alerts_coverage_report
    foreign key (coverage_report_id) references coverage_reports(id);

create table compression_runs (
    id bigint generated always as identity primary key,
    week_start_date date,
    status text check (status in ('PENDING','WRITTEN','VERIFIED','DELETED','FAILED')),
    verification_hash text,
    created_at timestamptz not null default now()
);
create index idx_compression_runs_status on compression_runs(status);
create index idx_compression_runs_week_start_date on compression_runs(week_start_date);

create table historical_jobs (
    id bigint generated always as identity primary key,
    compression_run_id bigint references compression_runs(id),
    status text check (status in ('PENDING','RUNNING','SUCCESS','FAILED')),
    created_at timestamptz not null default now()
);
create index idx_historical_jobs_status on historical_jobs(status);
create index idx_historical_jobs_compression_run_id on historical_jobs(compression_run_id);

create table data_quality_issues (
    id bigint generated always as identity primary key,
    batch_id text references ingestion_batches(id),
    resource text,
    row_data jsonb,
    error_code text,
    error_message text,
    created_at timestamptz not null default now()
);

create table audit_events (
    id bigint generated always as identity primary key,
    batch_id text references ingestion_batches(id),
    entity_type text,
    entity_id bigint,
    event_type text,
    details jsonb,
    created_at timestamptz not null default now()
);
create index idx_audit_events_batch_id on audit_events(batch_id);
create index idx_audit_events_entity on audit_events(entity_type, entity_id);

create table entity_history (
    id bigint generated always as identity primary key,
    batch_id text references ingestion_batches(id),
    entity_type text,
    entity_id bigint,
    field_name text,
    old_value text,
    new_value text,
    rpc_version int not null default 1,
    created_at timestamptz not null default now()
);
create index idx_entity_history_batch_id on entity_history(batch_id);
create index idx_entity_history_entity on entity_history(entity_type, entity_id);

create table system_config (
    key text primary key,
    value text not null,
    updated_at timestamptz not null default now()
);
