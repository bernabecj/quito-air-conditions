from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse

import pytest
import responses

from producers import config
from producers.http import make_session
from producers.openmeteo import OpenMeteoFormatError, current_event_time, fetch_current
from producers.stations import STATIONS

POINTS = [(station.latitude, station.longitude) for station in STATIONS]


def _query(mocked):
    return parse_qs(urlparse(mocked.calls[0].request.url).query)


def test_one_request_for_all_points(mocked, openmeteo_nine):
    mocked.add(responses.GET, config.OPEN_METEO_URL, json=openmeteo_nine)
    results = fetch_current(make_session(), POINTS)
    assert len(results) == 9 and len(mocked.calls) == 1
    query = _query(mocked)
    assert query["latitude"][0].split(",") == [str(lat) for lat, _ in POINTS]
    assert query["longitude"][0].split(",") == [str(lon) for _, lon in POINTS]
    assert query["timezone"] == ["UTC"]
    assert query["current"][0].split(",") == config.OPEN_METEO_CURRENT_FIELDS


def test_single_point_is_normalised_to_list(mocked, openmeteo_one):
    mocked.add(responses.GET, config.OPEN_METEO_URL, json=openmeteo_one)
    assert fetch_current(make_session(), POINTS[:1]) == [openmeteo_one]


def test_no_points_no_request(mocked):
    assert fetch_current(make_session(), []) == []
    assert len(mocked.calls) == 0


def test_count_mismatch_is_a_format_error(mocked, openmeteo_nine):
    mocked.add(responses.GET, config.OPEN_METEO_URL, json=openmeteo_nine[:8])
    with pytest.raises(OpenMeteoFormatError, match="asked for 9 points, got 8"):
        fetch_current(make_session(), POINTS)


def test_missing_current_block(mocked):
    mocked.add(responses.GET, config.OPEN_METEO_URL, json={"latitude": 0, "hourly": {}})
    with pytest.raises(OpenMeteoFormatError, match="without 'current'"):
        fetch_current(make_session(), POINTS[:1])


def test_not_json(mocked):
    mocked.add(responses.GET, config.OPEN_METEO_URL, body="Service Unavailable")
    with pytest.raises(OpenMeteoFormatError, match="not JSON"):
        fetch_current(make_session(), POINTS[:1])


def test_event_time_is_utc(openmeteo_one):
    assert current_event_time(openmeteo_one) == datetime(2026, 10, 16, 22, 30, tzinfo=timezone.utc)


def test_event_time_missing():
    assert current_event_time({"current": {}}) is None
    assert current_event_time({}) is None
