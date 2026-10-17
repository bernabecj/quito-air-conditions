"""
Producer two: Open-Meteo current weather at each station -> weather.current.raw.

One request for all nine station coordinates, one message per station keyed
by station id, payload untouched. The station list is producers/stations.py,
so weather is always joinable to measurements by key.

    python -m producers.weather_producer
    python -m producers.weather_producer --sink file --loop 900
"""

from datetime import datetime

import requests

from producers.config import TOPIC_WEATHER_RAW
from producers.envelope import build_envelope, serialize
from producers.openmeteo import current_event_time, fetch_current
from producers.runner import RunSummary, exit_main
from producers.sinks import Sink
from producers.stations import STATIONS

SOURCE = "openmeteo"


def poll(session: requests.Session, sink: Sink, now: datetime) -> RunSummary:
    points = [(station.latitude, station.longitude) for station in STATIONS]
    results = fetch_current(session, points)
    summary = RunSummary(source=SOURCE, fetched_at=now.isoformat())
    event_times = []

    for station, result in zip(STATIONS, results):
        event_time = current_event_time(result)
        envelope = build_envelope(
            source=SOURCE,
            key=station.station_id,
            payload=result,
            event_time=event_time,
            fetched_at=now,
        )
        sink.send(TOPIC_WEATHER_RAW, station.station_id, serialize(envelope))
        summary.records += 1
        if event_time is not None:
            event_times.append(event_time)

    summary.stations = len(results)
    summary.detail = {"newest_event_time": max(event_times).isoformat() if event_times else None}
    return summary


if __name__ == "__main__":
    exit_main(poll, "Fetch current weather at every REMMAQ station and publish it.")
