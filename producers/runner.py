"""
Shared command line and run loop for the producers.

A producer is a function `poll(session, sink, now) -> RunSummary`. This module
turns it into a process: settings from the environment, a sink, one poll (the
cron case) or a loop with a fixed interval (the always-on machine case), a
summary line on stdout, and a non-zero exit code when the source or the sink
failed, so the scheduler can see it.
"""

import argparse
import json
import logging
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable

import requests

from producers.envelope import iso_utc, utc_now
from producers.http import SourceUnavailable, make_session
from producers.settings import SINKS, Settings
from producers.sinks import Sink, SinkError, make_sink

log = logging.getLogger(__name__)


@dataclass
class RunSummary:
    source: str
    fetched_at: str
    records: int = 0
    stations: int = 0
    # Per-source detail the pipeline_health table will want later.
    detail: dict = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)


Poll = Callable[[requests.Session, Sink, datetime], RunSummary]


def run_once(
    poll: Poll,
    settings: Settings,
    session: requests.Session | None = None,
    sink: Sink | None = None,
) -> RunSummary:
    session = session or make_session()
    sink = sink or make_sink(settings)
    now = utc_now()
    summary = poll(session, sink, now)
    written = sink.flush()
    if written != summary.records:
        summary.warnings.append(f"sink wrote {written} of {summary.records} records")
    return summary


def main(poll: Poll, description: str, argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--sink", choices=SINKS, help="override SINK from the environment")
    parser.add_argument("--out-dir", type=Path, help="override FILE_SINK_DIR for the file sink")
    parser.add_argument(
        "--loop",
        type=int,
        metavar="SECONDS",
        help="keep polling every SECONDS instead of exiting after one poll",
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=args.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    overrides = {}
    if args.sink:
        overrides["SINK"] = args.sink
    if args.out_dir:
        overrides["FILE_SINK_DIR"] = str(args.out_dir)
    settings = Settings.from_env({**_environ(), **overrides})

    exit_code = 0
    while True:
        started = time.monotonic()
        try:
            summary = run_once(poll, settings)
            print(summary.to_json())
            for warning in summary.warnings:
                log.warning(warning)
        except (SourceUnavailable, SinkError, ValueError) as exc:
            # ValueError covers the *FormatError classes: the source changed shape.
            log.error("%s", exc)
            print(json.dumps({"error": str(exc), "at": iso_utc(utc_now())}))
            exit_code = 1
            if args.loop is None:
                return exit_code
        if args.loop is None:
            return exit_code
        time.sleep(max(0.0, args.loop - (time.monotonic() - started)))


def _environ() -> dict:
    import os

    return dict(os.environ)


def exit_main(poll: Poll, description: str) -> None:
    sys.exit(main(poll, description))
