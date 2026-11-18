# Arrival volumes (how much came into each mandi)

**What it is:** how many tonnes of a crop arrived at a mandi, for every block of each month since January 2018. It's the supply side to go with the prices.

## Where it comes from

Agmarknet's public price-trend report, "Market-wise Wholesale Arrivals Weekly", on the government site's own backend:

`GET https://api.agmarknet.gov.in/v1/price-trend/wholesale-arrivals-weekly?report_mode=Marketwise&commodity=&year=&month=&week=1..4&state=&district=`

- Plain requests work: no login, no CAPTCHA.
- A district is required; without one the answer has no rows.
- Each answer gives every market in the district for one block, plus the block before it. It also carries the same block last month and last year.
- Weeks are **blocks of the month**: days 1–8, 9–15, 16–23 and 24–end. They are not Monday weeks.
- The current block fills in day by day.
- Agmarknet's daily per-date reports need a CAPTCHA. They are not used.

## Files

`arrivals/{crop}/{district}.json`, one per crop and district that appears in today's live files:

```
{v, crop, district, start: "2018-01", unit: "tonnes", blocks: "1-8,9-15,16-23,24-end",
 markets: {"mh.nashik.chandwad": [t or null, ...], "?Baphana Agro Market Inc, Niphad": [...]},
 done: {"2026-11-3": "2026-11-18", ...}, updated_at}
```

- **Value index:** (year − 2018) × 48 + (month − 1) × 4 + block − 1.
- **One number per crop and mandi.** Agmarknet reports a commodity per market, so all varieties at a mandi share it.
- **Unmatched markets:** a market that matches none of our mandis is kept under `?` plus its Agmarknet name. Nothing fetched is lost, and the name can be mapped later.
- **`done`:** which report calls have been made, and on what day.

## How names are matched

Our catalogue was built from Agmarknet's own names, so matching is by name.

- **Crops:** 329 of 329 match.
- **Districts:** 603 of 608 match exactly, and the rest by a small alias list. That's 99.8% of our price lines.
- **Mandis:** matched within the district on the same key the identity resolver uses. Words like "APMC", "mandi" and "market" and brackets are dropped, and merged mandis point at the mandi they merged into.

## How a run works

A daily workflow, `.github/workflows/arrivals.yml` at 03:30 IST, spends up to 5 hours calling Agmarknet. It paces calls one second apart.

1. **Refresh:** every pair's newest block is asked again, the pairs not fetched for the longest first. In the first 8 days of a month, last month's final block is asked again too.
2. **Backfill:** older months, newest first, across every pair. One month at a time, so the whole country's recent history fills in before the old years. Weeks 2 and 4 cover a month, because each answer includes the block before.
3. **Quiet pairs:** a pair with no market at all after 6 calls isn't traded there. It's checked once a week and not backfilled.
4. **Saving:** files are written every 10 minutes and at the end.
5. **Rate limits:** on a 429, the run waits 1, 3 and then 10 minutes. If it's still refused, it stops and saves what it has.

**Rules kept:**
- Values are only ever added. A later empty answer never erases a stored number, and no market is dropped.
- The job reads `catalog/` and `live/` files only, and never touches `facts/daily` or the warehouse.

## Scale

| | Calls | At 1 a second |
|---|---|---|
| Full backfill, 2018 → today (12,887 crop × district pairs × 212) | about 2.7M | about 31 days of 5-hour runs if every pair trades |
| Daily refresh | about 13,000 | about 3.6 hours |

Many pairs go quiet after 6 calls, so the real total is lower.

**Rate limit:** on 18 Nov 2026, four parallel workers got 429 within a few hundred calls, and the block was still on more than an hour later. The safe pace isn't known yet.
- `ARRIVALS_GAP_S` (default 1.0) sets the spacing between calls.
- `--limit N` makes a small trial run.
- Start with a trial run of a few hundred pairs and read the log before letting it run in full.

## Not done yet

- **Forecast:** arrival inputs are not in the forecast yet. Planned: tonnage against the same block in earlier years, and the 4-block change. They go in only if the backtest gate improves.
- **App:** the app doesn't show arrivals yet.
- **Monday weeks:** turning month blocks into Monday weeks happens where the data is used, not here.
