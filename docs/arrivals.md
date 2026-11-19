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

## Coverage (measured 18 Nov 2026)

100 random crop × district pairs, monthly report, one call every 3 s, no 429:
- **95 of 100 pairs** had arrivals.
- **182 of our 196 crop × mandi pairs (93%)** matched an arrivals row.
- Most misses were names like "Vellore RM", "Sub yard Kawai Salpura" and "Mumbai- Fruit Market". The matcher now handles those, plus a single unambiguous prefix match ("Shahpura Niwas" → Shahpura).

Weekly trial, 200 random pairs, the job's own code, one call every 3 s, written only to a local folder:
- 430 calls, no 429.
- 198 of 200 pairs had arrivals.
- 491 market series matched our mandis and 2 didn't: "Fruit and Vegetable Market Udhampur" and "Mandawari APMC".

## Scale and limits

- **Rate limit:** four parallel callers got 429 on the weekly report, and it stayed blocked for more than 6 hours. One call every 3 s ran clean on the monthly report, so the default gap is now 3 s (`ARRIVALS_GAP_S`).
- **Client name:** the server returns 403 to Python's default `python-requests` client name. The job sends `farmuncle-pipeline (github.com/praneeth0100/farmuncle-pipeline)`.
- **One crop per call:** the report needs one commodity per call, so calls scale with crop × district (12,887 pairs).

| | Calls | At 1 every 3 s |
|---|---|---|
| Full weekly backfill, 2018 → today (12,887 × 212) | about 2.7M | about 94 days nonstop (about 450 five-hour runs); less as never-traded pairs go quiet |
| Refresh every pair once | about 13,000 | about 11 hours |

A 5-hour run makes about 6,000 calls. Every pair's newest block is therefore refreshed about every 2 days, and backfill only gets the time that's left. Recent months fill first because backfill goes newest first.

## Not done yet

- **Forecast:** arrival inputs are not in the forecast yet. Planned: tonnage against the same block in earlier years, and the 4-block change. They go in only if the backtest gate improves.
- **App:** the app doesn't show arrivals yet.
- **Monday weeks:** turning month blocks into Monday weeks happens where the data is used, not here.
