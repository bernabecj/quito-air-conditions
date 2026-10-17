"""The probe script's pure helpers. The network part was verified by running it."""

from scripts.probe_sources import LinkHarvester, freshest_timestamp, haversine_km


def test_freshest_iso_timestamp_wins():
    text = "updated 2026-10-16T17:00:00-05:00 and earlier 2026-10-15T09:30:00Z"
    assert freshest_timestamp(text) == "2026-10-16T17:00:00-05:00"


def test_dmy_dates_are_reported_not_sorted():
    assert "dd/mm/yyyy" in freshest_timestamp("Actualizado 16/10/2026 17:00")


def test_no_timestamp():
    assert freshest_timestamp("nothing here") is None


def test_haversine_quito_north_south():
    # Carapungo to Guamaní, roughly 29 km along the valley.
    assert 27 < haversine_km(-0.095472, -78.449809, -0.333949, -78.553416) < 31


def test_link_harvester_resolves_relative_links():
    harvester = LinkHarvester("https://ambiente.quito.gob.ec/red/")
    harvester.feed('<a href="/iqca">Índice <b>IQCA</b></a><a href="https://x.test/">x</a>')
    assert harvester.links == [
        ("https://ambiente.quito.gob.ec/iqca", "Índice IQCA"),
        ("https://x.test/", "x"),
    ]
