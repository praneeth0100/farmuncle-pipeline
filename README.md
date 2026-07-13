# FarmUncle v2 — ingestion pipeline

Phase C of the Master Build Spec: `live_tick`, `daily_rewrite`, `historical_backfill`, `retry_failed_pages`.

## Structure

```
farmuncle_pipeline/
├── config.py                # secrets, versions, vocabulary enums
├── government_constants.py  # STATES (Resource 2's per-state pagination)
├── ingest_common.py         # the facade — every script imports FROM HERE only
│
├── core/                    # shared, cross-cutting — not imported directly by scripts
│   ├── config_validator.py  # §17 live-schema half of startup validation
│   ├── startup_validation.py# §17 composition + entry point (validate_startup)
│   ├── logging_utils.py     # §18 logging standard (api_call_logs)
│   ├── batch_lifecycle.py   # ingestion_batches / raw_api_batches / failed_pages CRUD, §12 guard
│   ├── identity_client.py   # memoized wrapper over find_or_create_mandi/crop, normalize_*
│   ├── quality_scoring.py   # mandi_daily_prices.quality_score / quality_components
│   ├── resource_client.py   # fetch_page (§9 retry/ok-flag) + parse_agmarknet_record
│   ├── price_writer.py      # upsert_price_rows + §8 precedence enforcement
│   ├── record_processor.py  # parse → identity → quality → row dict, shared by every script
│   └── resource2_pipeline.py# the full per-date Resource 2 pipeline (daily_rewrite + historical_backfill)
│
└── ingestion/                # the four runnable scripts
    ├── live_tick.py
    ├── daily_rewrite.py
    ├── historical_backfill.py
    └── retry_failed_pages.py
```

**Import rule going forward:** scripts in `ingestion/` import from `farmuncle_pipeline.ingest_common` (the facade) and directly from `farmuncle_pipeline.core.<module>` for anything `ingest_common` doesn't re-export. Never duplicate a function that already exists in `core/` — every extraction in this codebase's history happened because a second script needed identical logic to a first (see each module's docstring for which). If a third script needs something two scripts already do inline, that's the signal to extract it into `core/`, not copy it again.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env             # then fill in real values, local dev only
```

## Running a script

Always run as a **module**, from the repo root (the folder containing `farmuncle_pipeline/`) — not by pointing `python` directly at the `.py` file, since that skips package resolution and the imports will fail:

```bash
python -m farmuncle_pipeline.ingestion.live_tick
python -m farmuncle_pipeline.ingestion.daily_rewrite
python -m farmuncle_pipeline.ingestion.historical_backfill --start-date 2026-06-01 --end-date 2026-06-07
python -m farmuncle_pipeline.ingestion.retry_failed_pages
```

## VS Code

Open the folder containing `farmuncle_pipeline/` (this folder) as the workspace root — `.vscode/settings.json` and `.vscode/launch.json` are already set up:
- **Run/Debug (F5):** pick a configuration from the Run panel — each script has one, already using module-style launch so imports resolve under the debugger too.
- **Integrated terminal:** `PYTHONPATH` is pre-set to the workspace root, so the `python -m ...` commands above work directly.
- **IntelliSense:** `python.analysis.extraPaths` is set so Pylance resolves `farmuncle_pipeline.*` imports without red squiggles.

## GitHub Actions

Each workflow step should run the same `python -m farmuncle_pipeline.ingestion.<script>` form, invoked from the repo root (the default `working-directory` when checkout puts the repo at `$GITHUB_WORKSPACE`) — no extra `PYTHONPATH` setup needed as long as it's run via `-m` from that root.
