"""
Where producers write. Two implementations behind one small interface:

- FileSink   JSON Lines on disk, one file per topic per run. Used in the
             sandbox and in tests, and as a replay fixture for the Bronze job.
- KafkaSink  confluent-kafka producer. Used in production.

The producers never know which one they hold. `make_sink(settings)` picks.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol

from producers.settings import SINK_KAFKA, Settings

log = logging.getLogger(__name__)


class Sink(Protocol):
    def send(self, topic: str, key: str, value: bytes) -> None: ...

    def flush(self) -> int:
        """Block until every sent record is durable. Returns the count written."""
        ...


class SinkError(RuntimeError):
    """Raised by flush() when at least one record was not written."""


class FileSink:
    """Appends `{"topic", "key", "value"}` lines to <dir>/<topic>/<run>.jsonl.

    `value` is stored as parsed JSON, not as a string, so the file is readable
    by humans and by spark.read.json without a second decoding step.
    """

    def __init__(self, base_dir: Path, run_id: str | None = None):
        self.base_dir = Path(base_dir)
        self.run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self._buffer: list[tuple[str, str, bytes]] = []
        self.paths: dict[str, Path] = {}

    def send(self, topic: str, key: str, value: bytes) -> None:
        self._buffer.append((topic, key, value))

    def flush(self) -> int:
        written = 0
        by_topic: dict[str, list[tuple[str, bytes]]] = {}
        for topic, key, value in self._buffer:
            by_topic.setdefault(topic, []).append((key, value))
        for topic, records in by_topic.items():
            path = self.base_dir / topic / f"{self.run_id}.jsonl"
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as handle:
                for key, value in records:
                    line = {"topic": topic, "key": key, "value": json.loads(value)}
                    handle.write(json.dumps(line, ensure_ascii=False) + "\n")
                    written += 1
            self.paths[topic] = path
            log.info("file sink: %d records -> %s", len(records), path)
        self._buffer.clear()
        return written


class KafkaSink:
    """Thin wrapper over confluent_kafka.Producer.

    produce() is asynchronous: records sit in librdkafka's queue until the
    broker acknowledges them, and the delivery callback records the outcome.
    flush() waits for the queue to drain and raises if anything failed, so a
    cron run exits non-zero when data did not reach the broker.
    """

    def __init__(self, settings: Settings, producer=None):
        if producer is None:
            from confluent_kafka import Producer  # imported lazily: optional at test time

            producer = Producer(settings.kafka_config())
        self._producer = producer
        self.delivered = 0
        self.failed: list[str] = []

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            self.failed.append(f"{msg.topic()}/{msg.key()}: {err}")
        else:
            self.delivered += 1

    def send(self, topic: str, key: str, value: bytes) -> None:
        self._producer.produce(
            topic, key=key.encode("utf-8"), value=value, on_delivery=self._on_delivery
        )
        # Serve delivery callbacks without blocking; keeps the queue from filling.
        self._producer.poll(0)

    def flush(self, timeout_s: float = 30.0) -> int:
        remaining = self._producer.flush(timeout_s)
        if remaining:
            self.failed.append(f"{remaining} records still queued after {timeout_s}s")
        if self.failed:
            raise SinkError("; ".join(self.failed[:5]))
        log.info("kafka sink: %d records acknowledged", self.delivered)
        return self.delivered


def make_sink(settings: Settings, run_id: str | None = None) -> Sink:
    if settings.sink == SINK_KAFKA:
        return KafkaSink(settings)
    return FileSink(settings.file_sink_dir, run_id=run_id)
