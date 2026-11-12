"""Weather maths for Screen 3: grid cells, NASA POWER and MET Norway parsing, Monday weeks. Pure functions, no I/O."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

LAT_STEP, LNG_STEP = 0.5, 0.625  # POWER's MERRA-2 meteorology grid (½° × ⅝°): one request per cell
FILL = -999.0  # POWER's value for a missing day
MIN_DAYS_PER_WEEK = 4


def cell_of(lat: float, lng: float) -> str:
    return f"{int((lat + 90) // LAT_STEP)}_{int((lng + 180) // LNG_STEP)}"


def cell_centre(cell: str) -> tuple[float, float]:
    i, j = (int(x) for x in cell.split("_"))
    return round(-90 + LAT_STEP * (i + 0.5), 4), round(-180 + LNG_STEP * (j + 0.5), 4)


def parse_power(payload: dict) -> dict[date, tuple[float | None, float | None]]:
    """Daily (rain mm, max °C) from a POWER daily point response; the fill value becomes None."""
    p = payload["properties"]["parameter"]
    rain, tmax = p.get("PRECTOTCORR", {}), p.get("T2M_MAX", {})
    clean = lambda v: None if v is None or v <= FILL + 1 else float(v)  # noqa: E731
    return {datetime.strptime(k, "%Y%m%d").date(): (clean(rain.get(k)), clean(tmax.get(k))) for k in sorted({*rain, *tmax})}


def weekly(days: dict[date, tuple[float | None, float | None]], start: date) -> list[list[float] | None]:
    """Monday weeks from `start`: [rain total mm, max of daily max °C], each to 1 dp; fewer than 4 days of rain → None."""
    if not days:
        return []
    last = max(days)
    out: list[list[float] | None] = []
    week = start
    while week <= last:
        span = [days.get(week + timedelta(d)) for d in range(7)]
        rain = [r for r, _ in (s for s in span if s) if r is not None]
        temps = [t for _, t in (s for s in span if s) if t is not None]
        out.append([round(sum(rain), 1), round(max(temps), 1)] if len(rain) >= MIN_DAYS_PER_WEEK and temps else None)
        week += timedelta(7)
    return out


def parse_metno(payload: dict, tz_offset_hours: float = 5.5) -> list[dict]:
    """Locationforecast 2.0 compact → per local (IST) day: rain total and max temperature, at most 9 days."""
    tz = timezone(timedelta(hours=tz_offset_hours))
    rain: dict[str, float] = defaultdict(float)
    tmax: dict[str, float] = {}
    for step in payload["properties"]["timeseries"]:
        day = datetime.fromisoformat(step["time"].replace("Z", "+00:00")).astimezone(tz).date().isoformat()
        data = step["data"]
        # The day's peak: instant readings, plus the 6-hour maximum (from day 3 the steps are 6-hourly and miss the afternoon).
        temps = [data["instant"]["details"].get("air_temperature"), (data.get("next_6_hours") or {}).get("details", {}).get("air_temperature_max")]
        for temp in (t for t in temps if t is not None):
            tmax[day] = max(tmax.get(day, temp), temp)
        hourly = data.get("next_1_hours")
        if hourly:
            rain[day] += hourly["details"].get("precipitation_amount", 0.0)
        elif data.get("next_6_hours"):  # beyond ~2 days the forecast steps are 6-hourly
            rain[day] += data["next_6_hours"]["details"].get("precipitation_amount", 0.0)
    days = sorted({*rain, *tmax})[:9]
    return [{"date": d, "rain_mm": round(rain.get(d, 0.0), 1), "tmax_c": round(tmax[d], 1) if d in tmax else None} for d in days]
