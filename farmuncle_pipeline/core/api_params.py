"""
FarmUncle v2 — api_params.py

Purpose (module-level):
    The single place that builds a data.gov.in query for either
    resource. The two resources use different filter field names
    (Resource 1: `arrival_date`, `state.keyword`; Resource 2:
    `Arrival_Date`, `State`), and a wrong name is silently ignored by
    the API, which returns an unfiltered page. `live_tick`,
    `resource2_pipeline` and `retry_failed_pages` all call this, so a
    retry can never use different names from the original fetch.
"""

from __future__ import annotations

from farmuncle_pipeline.config import Resource

_FILTER_NAMES = {
    Resource.RESOURCE_1: {"date": "filters[arrival_date]", "state": "filters[state.keyword]"},
    Resource.RESOURCE_2: {"date": "filters[Arrival_Date]", "state": "filters[State]"},
}


def build_params(
    *,
    resource: Resource,
    api_key: str,
    page_size: int,
    offset: int,
    date_str: str,
    state: str | None = None,
) -> dict:
    """
    Inputs:
        date_str: `DD/MM/YYYY`, the format both resources expect.
        state: omit for a nationwide query (never used by the
            pipeline jobs, which always page per state).
    Outputs:
        The `params` dict for `resource_client.fetch_page`.
    """
    names = _FILTER_NAMES[resource]
    params = {
        "api-key": api_key,
        "format": "json",
        "limit": page_size,
        "offset": offset,
        names["date"]: date_str,
    }
    if state is not None:
        params[names["state"]] = state
    return params
