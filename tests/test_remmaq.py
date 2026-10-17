"""REMMAQ client against the real captured payload."""

from datetime import datetime, timezone

import pytest
import responses

from producers import config
from producers.http import make_session
from producers.remmaq import (
    RemmaqFormatError,
    fetch_sites_info,
    parse_dotnet_date,
    site_event_time,
    valid_layers,
)


class TestParseDotnetDate:
    def test_epoch_millis_to_utc(self):
        assert parse_dotnet_date("/Date(1792188000000)/") == datetime(
            2026, 10, 16, 22, 0, tzinfo=timezone.utc
        )

    def test_offset_suffix_is_ignored(self):
        # .NET may append the server's offset; the epoch is already UTC.
        assert parse_dotnet_date("/Date(1792188000000-0500)/") == datetime(
            2026, 10, 16, 22, 0, tzinfo=timezone.utc
        )

    def test_none_passes_through(self):
        assert parse_dotnet_date(None) is None

    @pytest.mark.parametrize("bad", ["2026-10-16T17:00:00", "/Date()/", "Date(1)", ""])
    def test_garbage_raises(self, bad):
        with pytest.raises(RemmaqFormatError):
            parse_dotnet_date(bad)


class TestFetchSitesInfo:
    def test_posts_empty_json_with_ajax_headers(self, mocked, remmaq_payload):
        mocked.add(responses.POST, config.REMMAQ_SITES_INFO_URL, json=remmaq_payload)
        sites = fetch_sites_info(make_session())
        request = mocked.calls[0].request
        assert request.body == b"{}"
        assert request.headers["Content-Type"].startswith("application/json")
        assert request.headers["X-Requested-With"] == "XMLHttpRequest"
        assert request.headers["User-Agent"] == config.USER_AGENT
        assert len(sites) == 9

    def test_returns_sites_untouched(self, mocked, remmaq_payload):
        mocked.add(responses.POST, config.REMMAQ_SITES_INFO_URL, json=remmaq_payload)
        assert fetch_sites_info(make_session()) == remmaq_payload["d"]

    def test_not_json(self, mocked):
        mocked.add(responses.POST, config.REMMAQ_SITES_INFO_URL, body="<html>login</html>")
        with pytest.raises(RemmaqFormatError, match="not JSON"):
            fetch_sites_info(make_session())

    def test_missing_d_wrapper(self, mocked):
        mocked.add(responses.POST, config.REMMAQ_SITES_INFO_URL, json={"sites": []})
        with pytest.raises(RemmaqFormatError, match="expected"):
            fetch_sites_info(make_session())

    def test_site_without_required_field(self, mocked, remmaq_payload):
        del remmaq_payload["d"][0]["LayerInfos"]
        mocked.add(responses.POST, config.REMMAQ_SITES_INFO_URL, json=remmaq_payload)
        with pytest.raises(RemmaqFormatError, match="without LayerInfos"):
            fetch_sites_info(make_session())

    def test_server_error_is_retried(self, mocked, no_sleep, remmaq_payload):
        mocked.add(responses.POST, config.REMMAQ_SITES_INFO_URL, status=503)
        mocked.add(responses.POST, config.REMMAQ_SITES_INFO_URL, json=remmaq_payload)
        assert len(fetch_sites_info(make_session())) == 9
        assert len(no_sleep) == 1


class TestSiteHelpers:
    def test_event_time_from_aqi_time(self, remmaq_sites):
        assert site_event_time(remmaq_sites[0]) == datetime(
            2026, 10, 16, 22, 0, tzinfo=timezone.utc
        )

    def test_event_time_falls_back_to_newest_layer(self):
        site = {
            "LayerInfos": [
                {"DataTime": "/Date(1792184400000)/"},
                {"DataTime": "/Date(1792188000000)/"},
                {"DataTime": None},
            ]
        }
        assert site_event_time(site) == datetime(2026, 10, 16, 22, 0, tzinfo=timezone.utc)

    def test_event_time_none_when_nothing(self):
        assert site_event_time({"LayerInfos": []}) is None

    def test_valid_layers_real_payload(self, remmaq_sites):
        by_name = {site["SiteName"]: site for site in remmaq_sites}
        assert valid_layers(by_name["El Camal"]) == []  # offline station
        assert len(valid_layers(by_name["Belisario"])) == 10
        assert len(valid_layers(by_name["Guamani"])) == 7
