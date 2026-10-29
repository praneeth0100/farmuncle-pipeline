// Starts the pipeline workflows on time. GitHub's own cron skips runs for this repo (live_tick last fired
// 04:42 UTC with an hourly schedule), so Cloudflare's cron calls workflow_dispatch instead.
const REPO = "praneeth0100/farmuncle-pipeline";

const JOBS = {
  "5 * * * *": "live_tick.yml",
  "20 */3 * * *": "archive_history.yml", // backstop; the run also starts its own successor
};

export default {
  async scheduled(event, env) {
    const workflow = JOBS[event.cron];
    if (!workflow) throw new Error(`no workflow for cron ${event.cron}`);
    const res = await fetch(`https://api.github.com/repos/${REPO}/actions/workflows/${workflow}/dispatches`, {
      method: "POST",
      headers: {
        authorization: `Bearer ${env.GITHUB_TOKEN}`,
        accept: "application/vnd.github+json",
        "user-agent": "farmuncle-scheduler",
      },
      body: JSON.stringify({ ref: "main" }),
    });
    if (res.status !== 204) throw new Error(`${workflow}: GitHub answered ${res.status} ${await res.text()}`);
  },
};
