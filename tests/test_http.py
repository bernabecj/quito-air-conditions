"""Retry policy: transient failures are retried with growing delays, client errors are not."""

import pytest
import requests
import responses

from producers.config import USER_AGENT
from producers.http import SourceUnavailable, make_session, request_with_retry

URL = "https://example.test/data"


def test_session_identifies_the_project():
    assert make_session().headers["User-Agent"] == USER_AGENT
    assert "AirQuito" in USER_AGENT and "github.com" in USER_AGENT


def test_success_first_try(mocked, no_sleep):
    mocked.add(responses.GET, URL, json={"ok": 1})
    response = request_with_retry(make_session(), "GET", URL)
    assert response.json() == {"ok": 1}
    assert no_sleep == []


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_transient_status_is_retried_then_succeeds(mocked, no_sleep, status):
    mocked.add(responses.GET, URL, status=status)
    mocked.add(responses.GET, URL, json={"ok": 1})
    response = request_with_retry(make_session(), "GET", URL, backoff_s=1.0)
    assert response.status_code == 200
    assert len(mocked.calls) == 2
    assert len(no_sleep) == 1 and 1.0 <= no_sleep[0] <= 1.5  # backoff plus jitter


def test_backoff_grows_exponentially(mocked, no_sleep):
    for _ in range(3):
        mocked.add(responses.GET, URL, status=503)
    with pytest.raises(SourceUnavailable, match="after 3 attempts: HTTP 503"):
        request_with_retry(make_session(), "GET", URL, backoff_s=2.0)
    assert len(mocked.calls) == 3
    assert len(no_sleep) == 2  # no sleep after the last attempt
    assert 2.0 <= no_sleep[0] <= 3.0 and 4.0 <= no_sleep[1] <= 5.0


@pytest.mark.parametrize("status", [400, 401, 403, 404])
def test_client_errors_are_not_retried(mocked, no_sleep, status):
    mocked.add(responses.GET, URL, status=status)
    with pytest.raises(requests.HTTPError):
        request_with_retry(make_session(), "GET", URL)
    assert len(mocked.calls) == 1
    assert no_sleep == []


def test_connection_error_is_retried(mocked, no_sleep):
    mocked.add(responses.GET, URL, body=requests.ConnectionError("refused"))
    mocked.add(responses.GET, URL, json={"ok": 1})
    assert request_with_retry(make_session(), "GET", URL).json() == {"ok": 1}
    assert len(no_sleep) == 1


def test_connection_error_exhausts_budget(mocked, no_sleep):
    for _ in range(3):
        mocked.add(responses.GET, URL, body=requests.ConnectionError("refused"))
    with pytest.raises(SourceUnavailable, match="ConnectionError"):
        request_with_retry(make_session(), "GET", URL)


def test_max_attempts_is_configurable(mocked, no_sleep):
    mocked.add(responses.GET, URL, status=500)
    with pytest.raises(SourceUnavailable, match="after 1 attempts"):
        request_with_retry(make_session(), "GET", URL, max_attempts=1)
    assert no_sleep == []


def test_timeout_default_applied(mocked, no_sleep):
    mocked.add(responses.GET, URL, json={})
    request_with_retry(make_session(), "GET", URL)
    assert mocked.calls[0].request.req_kwargs.get("timeout") is not None
