# storefront_migrations/

This folder is history/record-keeping for the **storefront** Supabase project
(`xpkrjhcqqsoqsmcsaiar`, separate account from the warehouse this repo's
`supabase/migrations/` folder targets).

Nothing here auto-applies. Same as `supabase/migrations/` in this repo (no
linked `supabase` CLI project, no CI step runs `supabase db push`) — these are
applied manually, in order, via the Supabase SQL editor on the storefront
project or through an MCP/tool connection to it.

Run in order: 001 → 002 → 003 → 004.
