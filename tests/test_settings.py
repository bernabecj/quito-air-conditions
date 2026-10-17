from pathlib import Path

import pytest

from producers.settings import SINK_FILE, SINK_KAFKA, Settings


def test_defaults_are_file_sink():
    settings = Settings.from_env({})
    assert settings.sink == SINK_FILE
    assert settings.file_sink_dir == Path("data/local/sink")


def test_env_overrides(tmp_path):
    settings = Settings.from_env({"SINK": "FILE", "FILE_SINK_DIR": str(tmp_path)})
    assert settings.sink == SINK_FILE  # case-insensitive
    assert settings.file_sink_dir == tmp_path


def test_unknown_sink_rejected():
    with pytest.raises(ValueError, match="SINK must be one of"):
        Settings.from_env({"SINK": "s3"})


def test_kafka_requires_bootstrap():
    with pytest.raises(ValueError, match="KAFKA_BOOTSTRAP_SERVERS"):
        Settings.from_env({"SINK": "kafka"})


def test_kafka_config_is_at_least_once(kafka_settings):
    assert kafka_settings.sink == SINK_KAFKA
    cfg = kafka_settings.kafka_config()
    assert cfg["bootstrap.servers"] == "broker.example:9092"
    assert cfg["acks"] == "all"
    assert cfg["enable.idempotence"] is True
    assert cfg["security.protocol"] == "SASL_SSL"
    assert cfg["sasl.mechanisms"] == "PLAIN"
    assert cfg["sasl.username"] == "user"
    assert cfg["sasl.password"] == "secret"


def test_plaintext_omits_sasl():
    settings = Settings.from_env(
        {
            "SINK": "kafka",
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
            "KAFKA_SECURITY_PROTOCOL": "PLAINTEXT",
        }
    )
    cfg = settings.kafka_config()
    assert "sasl.username" not in cfg and "security.protocol" not in cfg


def test_password_not_in_repr(kafka_settings):
    assert "secret" not in repr(kafka_settings)
