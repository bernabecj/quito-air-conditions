"""Constants and the station reference table: cheap to test, expensive to get wrong."""

import math

from producers import config
from producers.stations import LAYERS, POLLUTANT_LAYERS, STATIONS, STATIONS_BY_ID


def test_topics_are_distinct_and_namespaced():
    topics = [
        config.TOPIC_MEASUREMENTS_RAW,
        config.TOPIC_WEATHER_RAW,
        config.TOPIC_SUBSCRIBERS_CDC,
        config.TOPIC_ALERTS_OUTBOUND,
        config.TOPIC_ALERTS_DELIVERY,
    ]
    assert len(set(topics)) == len(topics)
    assert all("." in topic for topic in topics)


def test_endpoints_use_https():
    for url in (config.REMMAQ_SITES_INFO_URL, config.OPEN_METEO_URL, config.OPENAQ_BASE_URL):
        assert url.startswith("https://")


def test_schema_version_is_positive_int():
    assert isinstance(config.SCHEMA_VERSION, int) and config.SCHEMA_VERSION >= 1


def test_nine_stations_with_unique_identifiers():
    assert len(STATIONS) == 9
    for field in ("station_id", "name", "history_column"):
        values = [getattr(station, field) for station in STATIONS]
        assert len(set(values)) == 9, f"duplicate {field}"
    assert set(STATIONS_BY_ID) == {station.station_id for station in STATIONS}


def test_station_ids_are_guids():
    for station in STATIONS:
        parts = station.station_id.split("-")
        assert [len(p) for p in parts] == [8, 4, 4, 4, 12]


def test_stations_are_within_quito_radius():
    def haversine_km(lat1, lon1, lat2, lon2):
        p1, p2 = math.radians(lat1), math.radians(lat2)
        dp, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
        a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
        return 2 * 6371 * math.asin(math.sqrt(a))

    for station in STATIONS:
        distance = haversine_km(
            config.QUITO_LAT, config.QUITO_LON, station.latitude, station.longitude
        )
        assert distance <= config.QUITO_RADIUS_M / 1000, f"{station.name} is {distance:.0f} km away"


def test_history_columns_are_upper_case_headers():
    # The xlsx headers are upper case with spaces; the loader normalises with strip().upper().
    for station in STATIONS:
        assert station.history_column == station.history_column.strip().upper()


def test_pollutant_layers_have_units():
    assert set(POLLUTANT_LAYERS) <= set(LAYERS)
    assert LAYERS["CO_mg"] == "mg/m3"
    assert all(LAYERS[layer] == "ug/m3" for layer in POLLUTANT_LAYERS if layer != "CO_mg")
