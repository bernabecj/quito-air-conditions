"""
Runtime settings, read from the environment (and .env through python-dotenv).

Everything that differs between the sandbox, a laptop, GitHub Actions and a
future Lambda lives here: which sink to use, where the Kafka cluster is, where
file output goes. Constants that never change per environment are in config.py.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

SINK_FILE = "file"
SINK_KAFKA = "kafka"
SINKS = (SINK_FILE, SINK_KAFKA)


@dataclass(frozen=True)
class Settings:
    # "file" writes JSON Lines under file_sink_dir; "kafka" needs the kafka_* fields.
    sink: str = SINK_FILE
    file_sink_dir: Path = Path("data/local/sink")

    kafka_bootstrap_servers: str = ""
    kafka_security_protocol: str = "SASL_SSL"
    kafka_sasl_mechanism: str = "PLAIN"
    kafka_sasl_username: str = ""
    kafka_sasl_password: str = field(default="", repr=False)
    kafka_client_id: str = "airquito-producer"

    @classmethod
    def from_env(cls, env: dict | None = None) -> "Settings":
        env = os.environ if env is None else env
        sink = env.get("SINK", SINK_FILE).lower()
        if sink not in SINKS:
            raise ValueError(f"SINK must be one of {SINKS}, got {sink!r}")
        settings = cls(
            sink=sink,
            file_sink_dir=Path(env.get("FILE_SINK_DIR", "data/local/sink")),
            kafka_bootstrap_servers=env.get("KAFKA_BOOTSTRAP_SERVERS", ""),
            kafka_security_protocol=env.get("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
            kafka_sasl_mechanism=env.get("KAFKA_SASL_MECHANISM", "PLAIN"),
            kafka_sasl_username=env.get("KAFKA_SASL_USERNAME", ""),
            kafka_sasl_password=env.get("KAFKA_SASL_PASSWORD", ""),
            kafka_client_id=env.get("KAFKA_CLIENT_ID", "airquito-producer"),
        )
        if settings.sink == SINK_KAFKA and not settings.kafka_bootstrap_servers:
            raise ValueError("SINK=kafka requires KAFKA_BOOTSTRAP_SERVERS")
        return settings

    def kafka_config(self) -> dict:
        """librdkafka configuration for the producer.

        acks=all and idempotence: a record is acknowledged only when every
        in-sync replica has it, and broker-side retries cannot duplicate it.
        This is the at-least-once side of the contract; deduplication by
        natural key happens in Silver.
        """
        config = {
            "bootstrap.servers": self.kafka_bootstrap_servers,
            "client.id": self.kafka_client_id,
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 50,
            "compression.type": "gzip",
        }
        if self.kafka_security_protocol != "PLAINTEXT":
            config.update(
                {
                    "security.protocol": self.kafka_security_protocol,
                    "sasl.mechanisms": self.kafka_sasl_mechanism,
                    "sasl.username": self.kafka_sasl_username,
                    "sasl.password": self.kafka_sasl_password,
                }
            )
        return config
