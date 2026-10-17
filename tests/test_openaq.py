"""OpenAQ client, kept for discovery. Stale for Quito since 2021, so only the
mechanics are tested: key handling, request shape, null-tolerant flattening."""

import pytest
import responses

from producers import config
from producers.openaq import (
    fetch_location_latest,
    fetch_quito_locations,
    flatten_sensors,
    get_api_key,
)


def test_missing_key_is_a_clear_error(monkeypatch):
    monkeypatch.delenv(config.OPENAQ_API_KEY_ENV, raising=False)
    with pytest.raises(RuntimeError, match="No OpenAQ key"):
        get_api_key()


def test_key_from_environment(monkeypatch):
    monkeypatch.setenv(config.OPENAQ_API_KEY_ENV, "k")
    assert get_api_key() == "k"


def test_locations_request_shape(mocked):
    mocked.add(responses.GET, f"{config.OPENAQ_BASE_URL}/locations", json={"results": [{"id": 1}]})
    assert fetch_quito_locations("key") == [{"id": 1}]
    request = mocked.calls[0].request
    assert request.headers["X-API-Key"] == "key"
    assert "coordinates=-0.1807%2C-78.4678" in request.url and "radius=25000" in request.url
    assert "key" not in request.url  # never in the query string


def test_latest_request(mocked):
    mocked.add(
        responses.GET, f"{config.OPENAQ_BASE_URL}/locations/42641/latest", json={"results": []}
    )
    assert fetch_location_latest(42641, "key") == []


def test_flatten_sensors_tolerates_nulls():
    locations = [
        {
            "id": 42641,
            "name": "Carapungo",
            "locality": None,
            "provider": None,
            "coordinates": None,
            "datetimeLast": None,
            "sensors": [{"id": 1, "parameter": {"name": "pm25", "units": "µg/m³"}}],
        }
    ]
    rows = flatten_sensors(locations)
    assert rows == [
        {
            "location_id": 42641,
            "location_name": "Carapungo",
            "locality": None,
            "provider": None,
            "latitude": None,
            "longitude": None,
            "sensor_id": 1,
            "parameter": "pm25",
            "units": "µg/m³",
            "last_seen_utc": None,
        }
    ]
