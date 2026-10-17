"""
Shared fixtures. Every HTTP call in the tests is mocked at the `requests` layer
with the `responses` library, so the suite runs without network or API keys.

The payload fixtures are real captures from 2026-10-16 (see docs/ANALYSIS.md),
not hand-written samples: the tests exercise the producers against the exact
shape the sources return.
"""

import json
import re
from datetime import datetime, timezone
from pathlib import Path

import pytest
import responses as responses_lib

from producers import config
from producers.settings import Settings
from producers.stations import STATIONS

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def remmaq_payload() -> dict:
    """The live feed document: {"d": [9 sites]}."""
    return json.loads((FIXTURES / "remmaq_get_sites_info.json").read_text(encoding="utf-8"))


@pytest.fixture
def remmaq_sites(remmaq_payload) -> list[dict]:
    return remmaq_payload["d"]


@pytest.fixture
def openmeteo_one() -> dict:
    """Open-Meteo answer for a single point (a bare object, not a list)."""
    return json.loads((FIXTURES / "openmeteo_current.json").read_text(encoding="utf-8"))


@pytest.fixture
def openmeteo_nine(openmeteo_one) -> list[dict]:
    """What Open-Meteo returns for all nine station coordinates in one call."""
    return [
        dict(openmeteo_one, latitude=station.latitude, longitude=station.longitude)
        for station in STATIONS
    ]


@pytest.fixture
def mocked():
    """A `responses` mock that fails the test on any unregistered request."""
    with responses_lib.RequestsMock(assert_all_requests_are_fired=False) as mock:
        yield mock


@pytest.fixture
def mock_sources(mocked, remmaq_payload, openmeteo_nine):
    """Both sources answering normally."""
    mocked.add(
        responses_lib.POST,
        config.REMMAQ_SITES_INFO_URL,
        json=remmaq_payload,
        content_type="application/json",
    )
    mocked.add(
        responses_lib.GET, re.compile(re.escape(config.OPEN_METEO_URL) + ".*"), json=openmeteo_nine
    )
    return mocked


@pytest.fixture
def file_settings(tmp_path) -> Settings:
    return Settings.from_env({"SINK": "file", "FILE_SINK_DIR": str(tmp_path / "sink")})


@pytest.fixture
def kafka_settings() -> Settings:
    return Settings.from_env(
        {
            "SINK": "kafka",
            "KAFKA_BOOTSTRAP_SERVERS": "broker.example:9092",
            "KAFKA_SASL_USERNAME": "user",
            "KAFKA_SASL_PASSWORD": "secret",
        }
    )


@pytest.fixture
def now() -> datetime:
    return datetime(2026, 10, 16, 23, 41, 46, tzinfo=timezone.utc)


@pytest.fixture
def no_sleep(monkeypatch):
    """Record backoff delays instead of sleeping."""
    delays: list[float] = []
    monkeypatch.setattr("producers.http.time.sleep", delays.append)
    return delays


class FakeMessage:
    def __init__(self, topic, key):
        self._topic, self._key = topic, key

    def topic(self):
        return self._topic

    def key(self):
        return self._key


class FakeKafkaProducer:
    """Stands in for confluent_kafka.Producer: records produce() calls and
    invokes the delivery callback synchronously."""

    def __init__(self, fail_keys=(), leave_queued=0):
        self.sent: list[tuple[str, bytes, bytes]] = []
        self.fail_keys = set(fail_keys)
        self.leave_queued = leave_queued
        self.polls = 0
        self.flushed_with: float | None = None

    def produce(self, topic, key, value, on_delivery):
        self.sent.append((topic, key, value))
        error = "Local: Message timed out" if key in self.fail_keys else None
        on_delivery(error, FakeMessage(topic, key))

    def poll(self, timeout):
        self.polls += 1

    def flush(self, timeout):
        self.flushed_with = timeout
        return self.leave_queued


@pytest.fixture
def fake_producer_class():
    return FakeKafkaProducer
