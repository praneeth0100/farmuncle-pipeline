#!/usr/bin/env bash
# The nightly warehouse jobs and archive_history must not write the warehouse at the same time.
# They used to share one concurrency group, but GitHub keeps only ONE waiting run per group and cancels the older
# waiting one: every archive_history window that queued behind a waiting daily_rewrite, nightly or purge_raw cancelled
# it (daily_rewrite was cancelled on 5 of 7 nights, 5-11 Nov). archive_history now has its own group and these
# two checks keep them apart instead.
#   warehouse_busy.sh jobs     -> prints "busy" when a nightly warehouse job is queued or running (archive_history yields)
#   warehouse_busy.sh history  -> waits up to 90 min for a running archive_history window to finish (nightly jobs go next)
set -euo pipefail
case "${1:-}" in
  jobs)
    for w in daily_rewrite nightly purge_raw archive_aged; do
      if [ "$(gh run list --workflow "$w.yml" -L 5 --json status --jq '[.[] | select(.status != "completed")] | length')" != "0" ]; then echo busy; exit 0; fi
    done ;;
  history)
    for minute in $(seq 1 90); do
      [ "$(gh run list --workflow archive_history.yml -L 5 --json status --jq '[.[] | select(.status == "in_progress")] | length')" = "0" ] && exit 0
      echo "archive_history is finishing a window; waiting ($minute min)"; sleep 60
    done
    echo "archive_history still running after 90 min; going ahead" ;;
  *) echo "usage: $0 jobs|history" >&2; exit 2 ;;
esac
