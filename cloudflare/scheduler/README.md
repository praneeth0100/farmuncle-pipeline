# Scheduler

Cloudflare cron -> GitHub `workflow_dispatch` for `live_tick` (hourly) and `archive_history` (backstop, every 3 h).

One-time setup (the token never goes in chat or in the repo):
1. GitHub > Settings > Developer settings > Fine-grained token, only repo `farmuncle-pipeline`, permission Actions: read and write.
2. `cd cloudflare/scheduler && npx wrangler secret put GITHUB_TOKEN` (paste the token), then `npx wrangler deploy`.

Check: Cloudflare dashboard > Workers > farmuncle-scheduler > Logs; a run of `live_tick` shows as `workflow_dispatch` in GitHub Actions.
