# archive/

Two migrations written for an older Supabase project (`wqccgjmvslevkglfkmtc`) that were never applied to the live
warehouse (`flxjrcbhmcuaynctokpv`). They reference tables that only exist after later migration files, so running
`supabase/migrations/` in order on a fresh project would fail on them. Kept for history only.
