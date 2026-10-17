"""
Producer one: REMMAQ latest values -> aq.measurements.raw.

One poll fetches every station in a single request and emits one message per
station, keyed by station id, whose payload is the site record exactly as the
feed returned it (all layers, valid or not, plus the IQCA block). Exploding
layers into one message per parameter is Silver's job; doing it here would
mean Bronze no longer holds what the source said.

The producer is stateless. It does not remember what it sent last time, so a
reading that has not changed since the previous poll is sent again. With nine
stations and a ten minute schedule that is a few hundred small records per
hour, and it keeps the producer trivially restartable. Silver deduplicates on
(station, parameter, event_time).

    python -m producers.remmaq_producer                 # SINK from .env, default file
    python -m producers.remmaq_producer --sink file --out-dir data/local/sink
    python -m producers.remmaq_producer --loop 600      # every 10 minutes, always-on
"""

from datetime import datetime

import requests

from producers.config import TOPIC_MEASUREMENTS_RAW
from producers.envelope import build_envelope, serialize
from producers.remmaq import fetch_sites_info, site_event_time, valid_layers
from producers.runner import RunSummary, exit_main
from producers.sinks import Sink
from producers.stations import STATIONS_BY_ID

SOURCE = "remmaq"


def poll(session: requests.Session, sink: Sink, now: datetime) -> RunSummary:
    sites = fetch_sites_info(session)
    summary = RunSummary(source=SOURCE, fetched_at=now.isoformat())
    offline, unknown, event_times = [], [], []

    for site in sites:
        station_id = site["SourceSiteID"]
        event_time = site_event_time(site)
        envelope = build_envelope(
            source=SOURCE, key=station_id, payload=site, event_time=event_time, fetched_at=now
        )
        sink.send(TOPIC_MEASUREMENTS_RAW, station_id, serialize(envelope))
        summary.records += 1

        if event_time is not None:
            event_times.append(event_time)
        if not valid_layers(site):
            offline.append(site["SiteName"])
        if station_id not in STATIONS_BY_ID:
            unknown.append(f"{site['SiteName']} ({station_id})")

    summary.stations = len(sites)
    summary.detail = {
        "offline_stations": offline,
        "newest_event_time": max(event_times).isoformat() if event_times else None,
        "valid_layers": sum(len(valid_layers(site)) for site in sites),
        "total_layers": sum(len(site.get("LayerInfos", [])) for site in sites),
    }
    if offline:
        summary.warnings.append(f"stations with no valid layer: {', '.join(offline)}")
    if unknown:
        summary.warnings.append(f"stations not in producers/stations.py: {', '.join(unknown)}")
    if not sites:
        summary.warnings.append("feed returned zero stations")
    return summary


if __name__ == "__main__":
    exit_main(poll, "Poll REMMAQ once and publish every station's latest values.")
