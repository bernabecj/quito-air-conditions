"""Both producers end to end: mocked sources, real envelope, real file sink."""

import json
from pathlib import Path

from producers import config, remmaq_producer, weather_producer
from producers.http import make_session
from producers.runner import run_once
from producers.sinks import FileSink
from producers.stations import STATIONS, STATIONS_BY_ID


def _lines(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


class TestRemmaqProducer:
    def test_one_record_per_station_payload_untouched(
        self, mock_sources, file_settings, now, remmaq_sites
    ):
        sink = FileSink(file_settings.file_sink_dir, run_id="r")
        summary = remmaq_producer.poll(make_session(), sink, now)
        sink.flush()
        records = _lines(sink.paths[config.TOPIC_MEASUREMENTS_RAW])

        assert summary.records == summary.stations == len(records) == 9
        assert [r["key"] for r in records] == [site["SourceSiteID"] for site in remmaq_sites]
        assert [r["value"]["payload"] for r in records] == remmaq_sites  # byte-for-byte the source
        assert all(r["key"] in STATIONS_BY_ID for r in records)

    def test_envelope_metadata(self, mock_sources, file_settings, now):
        sink = FileSink(file_settings.file_sink_dir, run_id="r")
        remmaq_producer.poll(make_session(), sink, now)
        sink.flush()
        value = _lines(sink.paths[config.TOPIC_MEASUREMENTS_RAW])[0]["value"]
        assert value["source"] == "remmaq"
        assert value["schema_version"] == config.SCHEMA_VERSION
        assert value["event_time"] == "2026-10-16T22:00:00Z"
        assert value["fetched_at"] == "2026-10-16T23:41:46Z"
        assert value["key"] == value["payload"]["SourceSiteID"]

    def test_summary_reports_offline_station(self, mock_sources, file_settings, now):
        summary = remmaq_producer.poll(make_session(), FileSink(file_settings.file_sink_dir), now)
        assert summary.detail["offline_stations"] == ["El Camal"]
        assert summary.detail["newest_event_time"] == "2026-10-16T22:00:00+00:00"
        assert summary.detail["valid_layers"] == 73 and summary.detail["total_layers"] == 89
        assert summary.warnings == ["stations with no valid layer: El Camal"]

    def test_unknown_station_is_warned_not_dropped(
        self, mocked, remmaq_payload, file_settings, now
    ):
        new_site = dict(
            remmaq_payload["d"][0],
            SourceSiteID="00000000-0000-0000-0000-000000000000",
            SiteName="Jipijapa",
        )
        remmaq_payload["d"].append(new_site)
        mocked.add("POST", config.REMMAQ_SITES_INFO_URL, json=remmaq_payload)
        sink = FileSink(file_settings.file_sink_dir, run_id="r")
        summary = remmaq_producer.poll(make_session(), sink, now)
        sink.flush()
        assert summary.records == 10  # still published: Bronze keeps everything
        assert any("Jipijapa" in w and "stations.py" in w for w in summary.warnings)

    def test_empty_feed_is_warned(self, mocked, file_settings, now):
        mocked.add("POST", config.REMMAQ_SITES_INFO_URL, json={"d": []})
        summary = remmaq_producer.poll(make_session(), FileSink(file_settings.file_sink_dir), now)
        assert summary.records == 0 and "zero stations" in summary.warnings[0]


class TestWeatherProducer:
    def test_one_record_per_station_keyed_by_station_id(
        self, mock_sources, file_settings, now, openmeteo_nine
    ):
        sink = FileSink(file_settings.file_sink_dir, run_id="r")
        summary = weather_producer.poll(make_session(), sink, now)
        sink.flush()
        records = _lines(sink.paths[config.TOPIC_WEATHER_RAW])
        assert summary.records == 9
        assert [r["key"] for r in records] == [s.station_id for s in STATIONS]
        assert [r["value"]["payload"] for r in records] == openmeteo_nine

    def test_pairs_results_with_stations_in_order(self, mock_sources, file_settings, now):
        sink = FileSink(file_settings.file_sink_dir, run_id="r")
        weather_producer.poll(make_session(), sink, now)
        sink.flush()
        for record, station in zip(_lines(sink.paths[config.TOPIC_WEATHER_RAW]), STATIONS):
            assert record["value"]["payload"]["latitude"] == station.latitude
            assert record["value"]["event_time"] == "2026-10-16T22:30:00Z"
            assert record["value"]["source"] == "openmeteo"


def test_run_once_flushes_and_matches_counts(mock_sources, file_settings):
    sink = FileSink(file_settings.file_sink_dir, run_id="r")
    summary = run_once(remmaq_producer.poll, file_settings, sink=sink)
    assert summary.records == 9 and summary.warnings == ["stations with no valid layer: El Camal"]
    assert sink.paths[config.TOPIC_MEASUREMENTS_RAW].exists()


def test_run_once_warns_when_sink_wrote_fewer(mock_sources, file_settings):
    class LossySink(FileSink):
        def flush(self):
            super().flush()
            return 7

    summary = run_once(
        remmaq_producer.poll, file_settings, sink=LossySink(file_settings.file_sink_dir)
    )
    assert "sink wrote 7 of 9 records" in summary.warnings
