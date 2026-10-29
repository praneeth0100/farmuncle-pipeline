# Scheduler

One hourly Cloudflare cron -> GitHub `workflow_dispatch`: `live_tick` every hour; `archive_history` every 3 h (backstop); `daily_rewrite`, `archive_copy`, `nightly_quality_audit`, `archive_aged`, `purge_raw` and `retry_failed_pages` at the UTC hours listed in `worker.js`.

One-time setup (the token never goes in chat or in the repo):
1. GitHub > Settings > Developer settings > Fine-grained token, only repo `farmuncle-pipeline`, permission Actions: read and write.
2. `cd cloudflare/scheduler && npx wrangler secret put GITHUB_TOKEN` (paste the token), then `npx wrangler deploy`.

Check: Cloudflare dashboard > Workers > farmuncle-scheduler > Logs; a run of `live_tick` shows as `workflow_dispatch` in GitHub Actions.
