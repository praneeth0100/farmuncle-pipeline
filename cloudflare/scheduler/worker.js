// Starts the pipeline workflows on time. GitHub's own cron skips or delays runs for this repo (live_tick last
// fired at 04:42 UTC with an hourly schedule; daily_rewrite has not fired by schedule since 4 Aug), so one
// hourly Cloudflare cron calls workflow_dispatch instead. The GitHub crons stay as a fallback.
const REPO = "praneeth0100/farmuncle-pipeline";

// UTC hours (0-23) each workflow is started in, at minute 05. A workflow that is still running when it is
// started again waits in its concurrency group, so a double start is harmless.
const EVERY_HOUR = ["live_tick.yml"];
const AT_HOURS = {
  "archive_history.yml": [0, 3, 6, 9, 12, 15, 18, 21], // backstop; the run also starts its own successor
  "retry_failed_pages.yml": [6, 16],
  "daily_rewrite.yml": [15], // IST evening, after the day is final
  "archive_copy.yml": [17],
  "nightly_quality_audit.yml": [19],
  "archive_aged.yml": [21],
  "purge_raw.yml": [22],
};

export function due(hour) {
  return [...EVERY_HOUR, ...Object.entries(AT_HOURS).filter(([, hours]) => hours.includes(hour)).map(([name]) => name)];
}

export default {
  async scheduled(event, env) {
    const hour = new Date(event.scheduledTime).getUTCHours();
    const failures = [];
    for (const workflow of due(hour)) {
      const res = await fetch(`https://api.github.com/repos/${REPO}/actions/workflows/${workflow}/dispatches`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${env.GITHUB_TOKEN}`,
          accept: "application/vnd.github+json",
          "user-agent": "farmuncle-scheduler",
        },
        body: JSON.stringify({ ref: "main" }),
      });
      if (res.status !== 204) failures.push(`${workflow}: GitHub answered ${res.status} ${await res.text()}`);
    }
    if (failures.length) throw new Error(failures.join("; "));
  },
};
