"""
REMMAQ client: the latest hourly values of every station, from the
Secretaria de Ambiente's Envista portal.

The endpoint is an ASP.NET AJAX page method (see config.REMMAQ_SITES_INFO_URL).
It expects a POST with a JSON content type and an empty JSON object, and
answers {"d": [site, ...]}. The response is returned as parsed JSON, untouched;
the helpers below only read it.

Timestamps come in the .NET JSON format "/Date(1792188000000)/": milliseconds
since the Unix epoch, UTC. The feed also carries AqiTimeString, a local-time
string without offset, which is ignored because the epoch form is unambiguous.
"""

import re
from datetime import datetime, timezone

import requests

from producers.config import REMMAQ_SITES_INFO_URL
from producers.http import request_with_retry

_DOTNET_DATE = re.compile(r"^/Date\((-?\d+)(?:[+-]\d{4})?\)/$")


class RemmaqFormatError(ValueError):
    """The feed answered, but not in the shape the producer was written against."""


def fetch_sites_info(
    session: requests.Session, url: str = REMMAQ_SITES_INFO_URL, **retry
) -> list[dict]:
    response = request_with_retry(
        session,
        "POST",
        url,
        headers={
            "Content-Type": "application/json; charset=utf-8",
            "X-Requested-With": "XMLHttpRequest",
        },
        json={},
        **retry,
    )
    try:
        payload = response.json()
    except ValueError as exc:
        raise RemmaqFormatError(f"not JSON: {response.text[:200]!r}") from exc
    sites = payload.get("d") if isinstance(payload, dict) else None
    if not isinstance(sites, list):
        raise RemmaqFormatError(
            f"expected {{'d': [...]}}, got keys {list(payload)[:5] if isinstance(payload, dict) else type(payload).__name__}"
        )
    for site in sites:
        for field in ("SourceSiteID", "SiteName", "LayerInfos"):
            if field not in site:
                raise RemmaqFormatError(f"site record without {field}: {str(site)[:200]}")
    return sites


def parse_dotnet_date(value: str | None) -> datetime | None:
    """'/Date(1792188000000)/' -> 2026-10-16T22:00:00Z. None stays None."""
    if value is None:
        return None
    match = _DOTNET_DATE.match(value)
    if not match:
        raise RemmaqFormatError(f"unrecognised date {value!r}")
    return datetime.fromtimestamp(int(match.group(1)) / 1000, tz=timezone.utc)


def site_event_time(site: dict) -> datetime | None:
    """The hour a site's values are for.

    AqiTime is the site-level stamp; when the site is offline it can be absent,
    in which case the newest layer DataTime is used, and if there is none the
    record has no event time and Silver will route it to quarantine.
    """
    stamp = parse_dotnet_date(site.get("AqiTime"))
    if stamp is not None:
        return stamp
    layer_times = [parse_dotnet_date(layer.get("DataTime")) for layer in site.get("LayerInfos", [])]
    layer_times = [t for t in layer_times if t is not None]
    return max(layer_times) if layer_times else None


def valid_layers(site: dict) -> list[dict]:
    return [layer for layer in site.get("LayerInfos", []) if layer.get("IsValid")]
