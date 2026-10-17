"""The command line: exit codes and the summary line a scheduler sees."""

import json

import pytest
import requests

from producers import config, remmaq_producer
from producers.runner import main


def _last_json_line(capsys) -> dict:
    lines = [l for l in capsys.readouterr().out.splitlines() if l.startswith("{")]
    return json.loads(lines[-1])


def test_file_sink_run_exits_zero_and_prints_summary(mock_sources, tmp_path, capsys):
    code = main(remmaq_producer.poll, "test", ["--sink", "file", "--out-dir", str(tmp_path)])
    assert code == 0
    summary = _last_json_line(capsys)
    assert summary["source"] == "remmaq" and summary["records"] == 9
    assert list((tmp_path / config.TOPIC_MEASUREMENTS_RAW).glob("*.jsonl"))


def test_source_down_exits_one(mocked, no_sleep, tmp_path, capsys):
    for _ in range(3):
        mocked.add("POST", config.REMMAQ_SITES_INFO_URL, body=requests.ConnectionError("down"))
    code = main(remmaq_producer.poll, "test", ["--sink", "file", "--out-dir", str(tmp_path)])
    assert code == 1
    assert "failed after 3 attempts" in _last_json_line(capsys)["error"]
    assert not (tmp_path / config.TOPIC_MEASUREMENTS_RAW).exists()  # nothing half-written


def test_source_changed_shape_exits_one(mocked, tmp_path, capsys):
    mocked.add("POST", config.REMMAQ_SITES_INFO_URL, json={"results": []})
    code = main(remmaq_producer.poll, "test", ["--sink", "file", "--out-dir", str(tmp_path)])
    assert code == 1
    assert "expected" in _last_json_line(capsys)["error"]


def test_sink_from_environment_when_no_flag(mock_sources, tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("SINK", "file")
    monkeypatch.setenv("FILE_SINK_DIR", str(tmp_path))
    assert main(remmaq_producer.poll, "test", []) == 0
    assert list(tmp_path.rglob("*.jsonl"))


def test_loop_runs_until_interrupted(mock_sources, tmp_path, monkeypatch, capsys):
    ticks = iter([None, None, KeyboardInterrupt()])

    def fake_sleep(seconds):
        assert seconds <= 600
        nxt = next(ticks)
        if nxt:
            raise nxt

    monkeypatch.setattr("producers.runner.time.sleep", fake_sleep)
    with pytest.raises(KeyboardInterrupt):
        main(
            remmaq_producer.poll,
            "test",
            ["--sink", "file", "--out-dir", str(tmp_path), "--loop", "600"],
        )
    out = capsys.readouterr().out
    assert out.count('"source": "remmaq"') == 3  # three polls before the interrupt


def test_invalid_sink_flag_rejected(tmp_path):
    with pytest.raises(SystemExit):
        main(remmaq_producer.poll, "test", ["--sink", "s3"])
