# Price forecast (Screen 3 dotted line)

**What it is:** for every crop, mandi and variety, the price 1–12 weeks ahead: a middle line (median) and a light band (10–90%: 8 times in 10 the price should land inside).
It is shown only where a backtest says it beats "the price stays the same".

## Where the numbers come from

`farmuncle_pipeline/ingestion/publish_forecast.py`, run daily by `.github/workflows/forecast.yml` at 07:00 IST, after the nightly charts are rebuilt.

1. **Prices**: the published chart files (`chart/{crop}/{mandi}.json`), the same numbers the app draws.
   - Mandi aliases and merges are already applied in those files.
   - Grades are merged as the app does it: on each day, the highest price wins within a variety.
   - Days are then averaged into Monday weeks.
   - The job never reads or writes `facts/daily`.
2. **Weather**: `weather/history/{cell}.json` (NASA POWER), keyed by the mandi's grid cell from `catalog/weather_cells.json`.
3. **Festivals**: `farmuncle_pipeline/forecast/festivals.json`, a copy of the app's 2016–2030 calendar.

## The model

There are three LightGBM models: the 10th, 50th and 90th percentile. They are pooled over every crop, mandi and horizon, with the horizon as one input.
- **Target:** ln(price h weeks later ÷ price this week).
- **Inputs** at week *t*. Each one uses only weeks ≤ *t*; a test rewrites the future and checks that no input moves:
  - **momentum:** log change over 1, 2, 4, 8, 13, 26 and 52 weeks;
  - **level:** price against its own 52-week median; against the median of the same weeks over the last 3 years; 13-week volatility;
  - **season:** the move from week *t* to *t+h* in each earlier year (up to 8), for this line, averaged over the crop in the state, and over the crop nationally; week of year, month;
  - **neighbours:** the crop's state median momentum over 1, 2, 4 and 13 weeks; the national median momentum over 1 and 4 weeks; every crop in the state over 4 weeks (a market-wide move); price against the state median and the district median; how many mandis in the state reported;
  - **identity:** crop and state (categories);
  - **weather:** rain over the last 4 and 13 weeks, and the hottest days over the last 4 weeks, each against the same weeks in earlier years;
  - **festivals:** weeks to the next festival, and whether one falls inside the horizon;
  - **reporting:** weeks with a price in the last 8 and 12; weeks since the report before; report days this week.
- **Down-weighting:** March–June 2020 (lockdown) counts at 0.3 in training.
- **Band calibration:** quantile models give bands that are too narrow or too wide. Each band is stretched or shrunk around the median, by the factor that made it hold 80% on earlier out-of-sample forecasts (conformal, last 52 weeks).
  - The factor is set per crop × horizon bucket (1–2, 3–6, 7–12 weeks).
  - A crop with fewer than 300 points uses the all-crop factor for that bucket.

**Not used, and why:**
- **The MET Norway forecast.** No past forecasts are stored, so the model can't learn how much to trust them.
- **Arrival quantities.** We don't have them and won't get them; the feeds give only an arrival *date*. Reporting density (weeks and days reported, weeks since the last report) stands in for supply.
- **Mandi identity as a category.** About 6,000 values overfit. The neighbour and level inputs carry what matters.

## The backtest and the gate

This is a rolling-origin backtest:
- 14 origins, 8 weeks apart, covering the last two years.
- At each origin, the model is trained only on outcomes known before it. It then forecasts every line's anchors in the next 8 weeks.
- The first origin only warms up the band calibration, so it is left out of every number.

**Ratio** = mean |ln(actual/forecast)| ÷ the same error for "the price stays the same".

A crop is published for a horizon bucket (1–2, 3–6, 7–12 weeks) when all of these hold:
- ratio ≤ 0.9;
- the 90% upper bound of that ratio is under 1.0 (bootstrap over anchor weeks, because one week's forecasts move together);
- band coverage is 75–85%;
- at least 300 forecasts from 5+ lines.

The app draws the line from today's price, so a crop shows only the passing buckets that run without a gap up to week 12. A crop with none is not published.
A line also needs 6+ weekly prices in its last 12 weeks, and its newest price no more than 2 weeks old.

Report: `meta/forecast/backtest.md`, rewritten at every retrain. The first run is in [forecast-backtest.md](forecast-backtest.md).

## Files

| Key | What |
|---|---|
| `forecast/crops.json` | crops that passed, with their horizons, ratio and coverage per horizon. **The app reads this first**; a crop not listed has no forecast, even if old files remain. |
| `forecast/{crop}/{mandi}.json` | `{v, crop, mandi_id, updated_at, trained_at, lines: [{variety, anchor_date, anchor_price, horizon_weeks: [{h, week, mid, lo, hi}]}], quality: {horizons, ratio_vs_naive, coverage}}`. Prices are ₹ per the chart's unit; `week` is a Monday. |
| `meta/forecast/q10.txt`, `q50.txt`, `q90.txt`, `model.json` | the models, band factors and gate |
| `meta/forecast/backtest.md`, `backtest.json` | the latest backtest |
| `meta/forecast_hashes.json` | a file is rewritten only when its content changed |

- **Retraining:** at most once a week, when `model.json` is 7+ days old, or by hand with the `train` input.
- **Other days:** the stored model is applied to the newest week of prices.
- **Skipped lines:** a line whose newest price is more than 2 weeks old, or with fewer than 3 prices in 8 weeks, gets no forecast.
- **Deletion:** nothing is ever deleted.

## Results (first local run, 15 Nov 2026)

[forecast-backtest.md](forecast-backtest.md) has 4 scored origins, 13 weeks apart, 3.0M forecasts and 179 crops.

| Horizon | Ratio vs "stays the same" | Band holds |
|---|---|---|
| 1–2 weeks | 99% | 80% |
| 3–6 weeks | 88% | 80% |
| 7–12 weeks | 79% | 80% |

46 crops passed.

At 7–12 weeks, against the app's seasonal method:

| Crop | App seasonal | This model |
|---|---|---|
| Onion | 87% | 68% |
| Tomato | 92% | 70% |
| Cabbage | 90% | 70% |
| Green chilli | 90% | 78% |
| Potato | 88% | 81% |
| Brinjal | 93% | 83% |
| Pomegranate | 95% | 86% |
| Banana | 98% | 93% (not shown) |

The model was first built without Fable's extra inputs (v1), then with them (v2: 52-week median, 3-year same-week level, 1–2 week neighbour momentum, the all-crop state index, report gaps, 2020 weight).
- v2 improved every horizon by about half a point to a point: 1–2 weeks 99.6% → 98.5%, 3–6 weeks 89.1% → 88.4%, 7–12 weeks 79.0% → 78.5%.
- 1–2 weeks stays at about the baseline for every crop. A week-ahead price is mostly the current price, so nothing is shown that close unless a crop passes (mustard does).

### Where this departs from Fable's plan (15 Nov 2026), and why

- **Anchor:** the weekly mean of the anchor week, not the median of the last 7 reported days. The chart files carry pre-averaged weeks before the last year, so a median isn't available there.
  - The baseline uses the same anchor, so the ratio is fair.
  - The ratio against the raw last daily price isn't reported.
- **Origins:** every 8 weeks, not every 4, so a weekly retrain stays near an hour on the free runner.
- **Gate coverage:** 75–85% (the owner's rule), not 77–83%.
- **Not built yet:**
  - unit-error, Hampel and stale-run cleaning, and grade-break resets;
  - nearest-5-mandi medians and hand-mapped substitute crops;
  - weather over the crop's growing districts (a mandi-to-growing-area map doesn't exist; the mandi's own cell is used);
  - per-crop ridge fallback; "district forecast when a line is thin".
  - Each of these is a candidate follow-up, and none is needed to pass the gate.
- **No arrivals:** the data has no arrival quantities.
- **Model storage:** R2 (`meta/forecast/`), not a GitHub release asset. The job already holds R2 keys, and nothing expires there.

## Cost

Everything runs on GitHub's free runners (the repo is public).
- Building the price panel from about 29,000 chart files: about 3 minutes.
- A retrain (14 backtest folds plus the final model): about 1.5 hours estimated. A 3-fold local run took 41 minutes, and the workflow allows 3 hours.
- A predict-only day: a few minutes.
