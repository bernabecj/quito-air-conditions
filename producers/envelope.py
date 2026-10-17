"""
The message envelope every producer writes.

Bronze stores the source payload exactly as received, so the envelope wraps it
rather than reshaping it. The fields around the payload are metadata the
pipeline needs regardless of source:

- `source`          which producer wrote it ("remmaq", "openmeteo").
- `schema_version`  version of this envelope, so consumers can branch on it.
- `key`             the Kafka message key, repeated inside the value so a
                    record read back from a file sink is self-describing.
- `event_time`      when the observation is for, UTC ISO 8601. This is the
                    source's clock: the hour the average covers.
- `fetched_at`      when the producer fetched it, UTC ISO 8601. This is our
                    clock. The difference is the source's publication delay,
                    which gold.pipeline_health will report.
- `payload`         the source record, untouched.

Event time versus ingestion time matters for two things downstream: Silver
deduplicates on (station, parameter, event_time), never on fetched_at, and the
"is this stale" check on the site compares event_time to now.
"""

import json
from datetime import datetime, timezone

from producers.config import SCHEMA_VERSION


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(moment: datetime) -> str:
    """ISO 8601 with an explicit Z; naive datetimes are rejected."""
    if moment.tzinfo is None:
        raise ValueError("event and fetch times must be timezone-aware")
    return moment.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def build_envelope(
    *,
    source: str,
    key: str,
    payload: dict,
    event_time: datetime | None,
    fetched_at: datetime,
) -> dict:
    return {
        "source": source,
        "schema_version": SCHEMA_VERSION,
        "key": key,
        "event_time": iso_utc(event_time) if event_time is not None else None,
        "fetched_at": iso_utc(fetched_at),
        "payload": payload,
    }


def serialize(envelope: dict) -> bytes:
    """Compact UTF-8 JSON. Non-ASCII stays as is (station names carry accents)."""
    return json.dumps(envelope, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
