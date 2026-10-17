import json
from datetime import datetime, timedelta, timezone

import pytest

from producers.config import SCHEMA_VERSION
from producers.envelope import build_envelope, iso_utc, serialize, utc_now


def test_iso_utc_uses_z_suffix():
    assert iso_utc(datetime(2026, 10, 16, 22, 0, tzinfo=timezone.utc)) == "2026-10-16T22:00:00Z"


def test_iso_utc_converts_other_offsets():
    quito = timezone(timedelta(hours=-5))
    assert iso_utc(datetime(2026, 10, 16, 17, 0, tzinfo=quito)) == "2026-10-16T22:00:00Z"


def test_iso_utc_rejects_naive():
    with pytest.raises(ValueError, match="timezone-aware"):
        iso_utc(datetime(2026, 10, 16, 22, 0))


def test_utc_now_is_aware():
    assert utc_now().tzinfo is timezone.utc


def test_envelope_fields(now):
    payload = {"SiteName": "Belisario", "nested": {"x": [1, 2]}}
    envelope = build_envelope(
        source="remmaq",
        key="abc",
        payload=payload,
        event_time=datetime(2026, 10, 16, 22, 0, tzinfo=timezone.utc),
        fetched_at=now,
    )
    assert envelope == {
        "source": "remmaq",
        "schema_version": SCHEMA_VERSION,
        "key": "abc",
        "event_time": "2026-10-16T22:00:00Z",
        "fetched_at": "2026-10-16T23:41:46Z",
        "payload": payload,
    }
    assert envelope["payload"] is payload  # the same object, not a copy or a reshaping


def test_event_time_may_be_unknown(now):
    envelope = build_envelope(source="remmaq", key="k", payload={}, event_time=None, fetched_at=now)
    assert envelope["event_time"] is None


def test_serialize_is_compact_utf8_and_round_trips(now):
    envelope = build_envelope(
        source="remmaq", key="k", payload={"name": "Guamaní"}, event_time=None, fetched_at=now
    )
    raw = serialize(envelope)
    assert isinstance(raw, bytes)
    assert b", " not in raw and b": " not in raw  # compact separators
    assert "Guamaní".encode("utf-8") in raw  # not escaped to \\u00ed
    assert json.loads(raw) == envelope
