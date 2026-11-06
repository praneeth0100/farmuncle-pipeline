# Weather sources (Screen 3 "Weather and price")

Checked 6 Nov 2026 against the official pages.

## Past weather: NASA POWER (power.larc.nasa.gov)
- Daily point API, `community=AG`, parameters `PRECTOTCORR` (precipitation, mm/day, observation-corrected MERRA-2 over
  land) and `T2M_MAX` (daily max 2 m temperature, °C), `time-standard=UTC`. Grid ½° lat × ⅝° lon (MERRA-2): one request
  per cell; repeating the same location can get blocked. Latency 2–3 days; recent values can be revised.
- Licence: POWER pages state none; NASA Earth Science data policy: "Unless the content is marked with a use restriction
  or license, data provided from a NASA-led mission are licensed as Creative Commons Zero (CC0)." NASA must be
  acknowledged and must not be presented as endorsing the app.
- Credit used in the app: "Weather history: NASA Langley Research Center POWER project". POWER asks to be told if data
  is passed on to other researchers (not applicable to app users).

## Forecast: MET Norway Locationforecast 2.0 (api.met.no)
- Licence: NLOD 2.0 and CC BY 4.0 — attribution required, changes must be indicated.
- Credit used in the app: "Forecast based on data from MET Norway" (derived daily totals).
- Rules: identifying User-Agent with app name and contact (env `WEATHER_CONTACT`); under 20 requests/s; never re-request
  before `Expires`, use `If-Modified-Since` with the last `Last-Modified`; coordinates truncated to 4 decimals; add random
  minutes to the schedule; HTTPS + gzip. Apps should use a backend cache, never call the API from phones (we do).

Not used: Open-Meteo (free tier non-commercial), IMD (no free district API).
