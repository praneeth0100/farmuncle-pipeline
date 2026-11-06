from datetime import date, timedelta

from farmuncle_pipeline.weather import core


def test_cell_of_groups_nearby_points_and_centres():
    a = core.cell_of(12.97, 77.59)
    assert a == core.cell_of(12.99, 77.61)
    lat, lng = core.cell_centre(a)
    assert abs(lat - 12.97) <= 0.25 and abs(lng - 77.59) <= 0.3125
    assert core.cell_of(12.97, 77.59) != core.cell_of(13.6, 77.59)


def test_parse_power_drops_fill_values():
    payload = {"properties": {"parameter": {
        "PRECTOTCORR": {"20240101": 2.5, "20240102": -999.0},
        "T2M_MAX": {"20240101": 31.2, "20240102": 30.0}}}}
    days = core.parse_power(payload)
    assert days[date(2024, 1, 1)] == (2.5, 31.2)
    assert days[date(2024, 1, 2)] == (None, 30.0)


def test_weekly_sums_rain_takes_max_temp_and_needs_four_days():
    start = date(2024, 1, 1)  # a Monday
    days = {start + timedelta(d): (2.0, 30.0 + d % 6) for d in range(7)}
    days.update({start + timedelta(7 + d): (1.0, 25.0) for d in range(3)})  # 3 days only
    weeks = core.weekly(days, start)
    assert weeks[0] == [14.0, 35.0]
    assert weeks[1] is None


def test_parse_metno_makes_ist_days():
    def entry(t, temp, rain):
        return {"time": t, "data": {"instant": {"details": {"air_temperature": temp}},
                                    "next_1_hours": {"details": {"precipitation_amount": rain}}}}
    payload = {"properties": {"timeseries": [
        entry("2026-11-06T04:00:00Z", 28.0, 1.0),
        entry("2026-11-06T08:00:00Z", 32.5, 2.5),
        entry("2026-11-06T20:00:00Z", 22.0, 4.0),  # 01:30 IST next day
    ]}}
    days = core.parse_metno(payload)
    assert days[0] == {"date": "2026-11-06", "rain_mm": 3.5, "tmax_c": 32.5}
    assert days[1] == {"date": "2026-11-07", "rain_mm": 4.0, "tmax_c": 22.0}
