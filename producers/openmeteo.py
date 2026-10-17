"""
Open-Meteo client: current weather at each station's coordinates.

One request carries every station: Open-Meteo accepts comma-separated
latitude and longitude lists and answers with a list of one object per point,
in the same order. A single point answers with a bare object, which is
normalised to a one-element list so callers never branch on it.

The `current` block is a 15-minute model value; its `time` is the ISO local
time in the requested timezone. We request UTC so event_time needs no
conversion.
"""

from datetime import datetime, timezone

import requests

from producers.config import OPEN_METEO_CURRENT_FIELDS, OPEN_METEO_URL
from producers.http import request_with_retry


class OpenMeteoFormatError(ValueError):
    pass


def fetch_current(
    session: requests.Session,
    points: list[tuple[float, float]],
    url: str = OPEN_METEO_URL,
    fields: list[str] = OPEN_METEO_CURRENT_FIELDS,
    **retry,
) -> list[dict]:
    if not points:
        return []
    response = request_with_retry(
        session,
        "GET",
        url,
        params={
            "latitude": ",".join(str(lat) for lat, _ in points),
            "longitude": ",".join(str(lon) for _, lon in points),
            "current": ",".join(fields),
            "timezone": "UTC",
        },
        **retry,
    )
    try:
        payload = response.json()
    except ValueError as exc:
        raise OpenMeteoFormatError(f"not JSON: {response.text[:200]!r}") from exc
    results = payload if isinstance(payload, list) else [payload]
    if len(results) != len(points):
        raise OpenMeteoFormatError(f"asked for {len(points)} points, got {len(results)} results")
    for result in results:
        if "current" not in result:
            raise OpenMeteoFormatError(f"result without 'current': {str(result)[:200]}")
    return results


def current_event_time(result: dict) -> datetime | None:
    """'2026-10-16T22:30' (UTC, as requested) -> aware datetime."""
    stamp = (result.get("current") or {}).get("time")
    if not stamp:
        return None
    return datetime.fromisoformat(stamp).replace(tzinfo=timezone.utc)
