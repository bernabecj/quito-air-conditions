"""
One HTTP session for every source, with a descriptive User-Agent and a small
retry policy for transient failures.

Retries are deliberately conservative: three attempts with exponential backoff
and jitter, only on errors that can plausibly succeed on a second try
(connection errors, timeouts, 429 Too Many Requests, 5xx). A 4xx other than
429 means the request itself is wrong and is raised immediately. An IP of this
project has been blocked before, so hammering a source is never acceptable.
"""

import logging
import random
import time

import requests

from producers.config import HTTP_BACKOFF_S, HTTP_MAX_ATTEMPTS, HTTP_TIMEOUT, USER_AGENT

log = logging.getLogger(__name__)

RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class SourceUnavailable(RuntimeError):
    """Raised when a source still fails after the retry budget is spent."""


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    return session


def request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    *,
    max_attempts: int = HTTP_MAX_ATTEMPTS,
    backoff_s: float = HTTP_BACKOFF_S,
    sleep=None,
    **kwargs,
) -> requests.Response:
    """Perform the request, retrying transient failures. Returns a 2xx response.

    `sleep` is injectable so tests can run the backoff without waiting.
    """
    sleep = sleep or time.sleep
    kwargs.setdefault("timeout", HTTP_TIMEOUT)
    last_error: str = ""
    for attempt in range(1, max_attempts + 1):
        try:
            response = session.request(method, url, **kwargs)
        except requests.RequestException as exc:
            last_error = f"{type(exc).__name__}: {exc}"
        else:
            if response.status_code < 400:
                return response
            if response.status_code not in RETRYABLE_STATUS:
                response.raise_for_status()
            last_error = f"HTTP {response.status_code}"

        if attempt < max_attempts:
            delay = backoff_s * 2 ** (attempt - 1) + random.uniform(0, backoff_s / 2)
            log.warning(
                "%s %s failed (%s), retry %d/%d in %.1fs",
                method,
                url,
                last_error,
                attempt,
                max_attempts - 1,
                delay,
            )
            sleep(delay)

    raise SourceUnavailable(f"{method} {url} failed after {max_attempts} attempts: {last_error}")
