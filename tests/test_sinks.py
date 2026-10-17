import json

import pytest

from producers.settings import Settings
from producers.sinks import FileSink, KafkaSink, SinkError, make_sink


class TestFileSink:
    def test_one_file_per_topic_with_run_id(self, tmp_path):
        sink = FileSink(tmp_path, run_id="run1")
        sink.send("topic.a", "k1", b'{"v": 1}')
        sink.send("topic.a", "k2", b'{"v": 2}')
        sink.send("topic.b", "k3", b'{"v": 3}')
        assert sink.flush() == 3
        assert sorted(p.relative_to(tmp_path).as_posix() for p in tmp_path.rglob("*.jsonl")) == [
            "topic.a/run1.jsonl",
            "topic.b/run1.jsonl",
        ]
        lines = [
            json.loads(l) for l in (tmp_path / "topic.a" / "run1.jsonl").read_text().splitlines()
        ]
        assert lines == [
            {"topic": "topic.a", "key": "k1", "value": {"v": 1}},
            {"topic": "topic.a", "key": "k2", "value": {"v": 2}},
        ]
        assert sink.paths["topic.b"] == tmp_path / "topic.b" / "run1.jsonl"

    def test_value_stored_as_json_not_string(self, tmp_path):
        # spark.read.json must see the envelope as a struct, not as an escaped string.
        sink = FileSink(tmp_path, run_id="r")
        sink.send("t", "k", b'{"payload": {"a": [1, 2]}}')
        sink.flush()
        line = json.loads((tmp_path / "t" / "r.jsonl").read_text())
        assert line["value"]["payload"]["a"] == [1, 2]

    def test_flush_clears_buffer_and_appends(self, tmp_path):
        sink = FileSink(tmp_path, run_id="r")
        sink.send("t", "k", b"{}")
        assert sink.flush() == 1
        assert sink.flush() == 0
        sink.send("t", "k", b"{}")
        sink.flush()
        assert len((tmp_path / "t" / "r.jsonl").read_text().splitlines()) == 2

    def test_default_run_id_is_utc_stamp(self, tmp_path):
        sink = FileSink(tmp_path)
        assert len(sink.run_id) == 16 and sink.run_id.endswith("Z")

    def test_unicode_preserved(self, tmp_path):
        sink = FileSink(tmp_path, run_id="r")
        sink.send("t", "k", '{"n": "Guamaní"}'.encode("utf-8"))
        sink.flush()
        assert "Guamaní" in (tmp_path / "t" / "r.jsonl").read_text(encoding="utf-8")


class TestKafkaSink:
    def test_produce_with_encoded_key_and_delivery_count(self, kafka_settings, fake_producer_class):
        producer = fake_producer_class()
        sink = KafkaSink(kafka_settings, producer=producer)
        sink.send("aq.measurements.raw", "abc", b"{}")
        sink.send("aq.measurements.raw", "def", b"{}")
        assert producer.sent == [
            ("aq.measurements.raw", b"abc", b"{}"),
            ("aq.measurements.raw", b"def", b"{}"),
        ]
        assert producer.polls == 2  # callbacks served after every produce
        assert sink.flush() == 2
        assert producer.flushed_with == 30.0

    def test_failed_delivery_raises_on_flush(self, kafka_settings, fake_producer_class):
        sink = KafkaSink(kafka_settings, producer=fake_producer_class(fail_keys={b"bad"}))
        sink.send("t", "good", b"{}")
        sink.send("t", "bad", b"{}")
        with pytest.raises(SinkError, match="t/b'bad': Local: Message timed out"):
            sink.flush()
        assert sink.delivered == 1

    def test_records_left_in_queue_raise(self, kafka_settings, fake_producer_class):
        sink = KafkaSink(kafka_settings, producer=fake_producer_class(leave_queued=3))
        sink.send("t", "k", b"{}")
        with pytest.raises(SinkError, match="3 records still queued"):
            sink.flush(timeout_s=5)

    def test_real_producer_constructed_from_settings(self, kafka_settings):
        # No broker needed to construct; librdkafka connects lazily.
        sink = KafkaSink(kafka_settings)
        assert type(sink._producer).__name__ == "Producer"


def test_make_sink_picks_by_settings(
    file_settings, kafka_settings, monkeypatch, fake_producer_class
):
    assert isinstance(make_sink(file_settings, run_id="r"), FileSink)
    monkeypatch.setattr("confluent_kafka.Producer", fake_producer_class)
    assert isinstance(make_sink(kafka_settings), KafkaSink)
