# baseline/

`live_schema_2026-10-28.sql` is a catalog dump of the live warehouse schema, for reference. It is not a migration and
has not been run against an empty database. From now on every schema change must be a new file in
`supabase/migrations/`, applied with the migration tool and not in the SQL editor, so the repo and the database stop
drifting apart.
