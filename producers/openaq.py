"""
OpenAQ client
Thin wrapper over the OpenAQ v3 endpoints the pipeline needs. The producer
(Phase 1) and scripts/discover_stations.py both build on it.

Responses are returned as parsed JSON, untouched. Bronze stores the payload
exactly as the source sent it, so nothing here casts, renames or drops fields;
flatten_sensors() exists only for the human-readable station listing.
"""

import os

import requests
from dotenv import load_dotenv

from producers.config import (
    HTTP_TIMEOUT,
    OPENAQ_API_KEY_ENV,
    OPENAQ_BASE_URL,
    QUITO_LAT,
    QUITO_LON,
    QUITO_RADIUS_M,
)

load_dotenv()


def get_api_key() -> str:
    try:
        return os.environ[OPENAQ_API_KEY_ENV]
    except KeyError:
        raise RuntimeError(
            f"No OpenAQ key: set {OPENAQ_API_KEY_ENV} in .env (see .env.example)."
        ) from None


def _get(path: str, api_key: str, params: dict | None = None) -> dict:
    response = requests.get(
        f"{OPENAQ_BASE_URL}/{path}",
        params=params,
        headers={"X-API-Key": api_key},
        timeout=HTTP_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()


def fetch_quito_locations(api_key: str) -> list[dict]:
    """Every OpenAQ location within QUITO_RADIUS_M of the city centre."""
    payload = _get(
        "locations",
        api_key,
        params={
            # OpenAQ expects "latitude,longitude", the reverse of GeoJSON order.
            "coordinates": f"{QUITO_LAT},{QUITO_LON}",
            "radius": QUITO_RADIUS_M,
            "limit": 1000,
        },
    )
    return payload["results"]


def fetch_location_latest(location_id: int, api_key: str) -> list[dict]:
    """Latest value of every sensor at one location."""
    return _get(f"locations/{location_id}/latest", api_key)["results"]


def flatten_sensors(locations: list[dict]) -> list[dict]:
    """One row per sensor, for listing which station measures what.

    Optional fields are read with .get(): OpenAQ leaves locality, provider and
    datetimeLast null on some locations, and a station that has never reported
    is still worth seeing in the listing.
    """
    rows = []
    for location in locations:
        coordinates = location.get("coordinates") or {}
        last_seen = location.get("datetimeLast") or {}
        provider = location.get("provider") or {}
        for sensor in location["sensors"]:
            parameter = sensor["parameter"]
            rows.append(
                {
                    "location_id": location["id"],
                    "location_name": location["name"],
                    "locality": location.get("locality"),
                    "provider": provider.get("name"),
                    "latitude": coordinates.get("latitude"),
                    "longitude": coordinates.get("longitude"),
                    "sensor_id": sensor["id"],
                    "parameter": parameter["name"],
                    "units": parameter.get("units"),
                    "last_seen_utc": last_seen.get("utc"),
                }
            )
    return rows
