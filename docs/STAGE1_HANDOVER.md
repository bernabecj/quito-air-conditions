# Stage 1 handover: producers, notebook, tests, docs

Everything built in this session, how to verify it yourself, how the tests
work and what data they use, and how to break them on purpose (by hand or with
a script) to see that they are testing something real. Read this
after [ANALYSIS.md](ANALYSIS.md) (why these sources, what the data looks like)
and [data-contracts.md](data-contracts.md) (what each topic promises).

---

## 1. Summary of changes

### New, `producers/`
| File | What it does |
|---|---|
| `http.py` | One `requests.Session` per run, descriptive User-Agent, `request_with_retry`: 3 attempts, exponential backoff with jitter, only on connection errors/timeouts/429/5xx |
| `settings.py` | `Settings.from_env()`: sink choice, file sink directory, Kafka bootstrap/SASL; `kafka_config()` sets `acks=all` + idempotence |
| `stations.py` | The 9 REMMAQ stations: GUID, feed name, xlsx column header, coordinates; the three identifiers used to join feed, history and producers |
| `envelope.py` | `{source, schema_version, key, event_time, fetched_at, payload}`; `payload` is never modified |
| `sinks.py` | `FileSink` (JSON Lines, Spark-readable) and `KafkaSink` (delivery callbacks, raises on failure) behind one `Sink` protocol |
| `remmaq.py` | Client for the live feed: POST the page method, validate the `{"d": [...]}` shape, parse `/Date(ms)/`, `site_event_time`, `valid_layers` |
| `openmeteo.py` | One request for all 9 coordinates, normalises single vs list responses |
| `runner.py` | Shared CLI: `--sink`, `--out-dir`, `--loop`, JSON summary line, exit codes |
| `remmaq_producer.py`, `weather_producer.py` | The two producers: one message per station, keyed by station GUID, payload untouched |

### Changed
- `producers/config.py`: REMMAQ and Open-Meteo endpoints, topic names, `SCHEMA_VERSION`; OpenAQ constants kept, demoted.
- `producers/openaq.py`, `scripts/discover_stations.py`: docstrings updated to say why they still exist (the discovery record) and that no producer reads them.
- `pyproject.toml` / `poetry.lock`: added `confluent-kafka` (runtime), `responses` (dev), an optional `notebook` group (`pyspark==3.5.8`, `setuptools`, `pyarrow`, `pandas`, `openpyxl`, `ipykernel`, `nbconvert`).
- `requirements.txt` (new): exported from the lock, all groups; CI fails if it drifts.
- `.github/workflows/ci.yml`: added the `requirements.txt` sync check; removed the "no tests yet" escape hatch now that tests exist.
- `.github/workflows/producers.yml` (new): cron every 10 minutes, `workflow_dispatch` with a `sink=file` dry run uploaded as an artifact.
- `.gitignore`, `.gitattributes` (new): `data/local/`, caches, `.env`; line-ending normalisation (the repo had a CRLF/LF mix).
- `.env.example`: `SINK`, `FILE_SINK_DIR`, `KAFKA_*`, plus the probe's optional keys.
- `README.md`, `docs/PROJECT_PLAN.md`: status, sources (REMMAQ primary, OpenAQ demoted), architecture, topic table, Phase 1 split into Stage 1 (done) / Stage 2 (next), repository layout.

### New, everything else
- `scripts/probe_sources.py`: polite source-freshness probe (used to find the REMMAQ feed).
- `scripts/break_tests.py`: see section 4.
- `notebooks/01_exploring.ipynb`: 47 cells, executed with outputs. See [ANALYSIS.md](ANALYSIS.md) section 3 for the findings.
- `tests/` (new): `conftest.py` + 10 test files, 98 tests, fixtures in `tests/fixtures/` are real captures (not hand-written samples).
- `docs/ANALYSIS.md`, `docs/data-contracts.md`, this file.

---

## 2. Verify from this sandbox (no network)

```bash
python -m venv .venv && source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt                           # or --find-links <wheels dir> if pyspark
                                                            #   has no prebuilt wheel for your platform
black --check . && isort --check-only .
pytest                                                     # 98 tests, < 1 s, no network or keys needed
```

Run a producer against a mocked source the way the tests do, or for real:
```bash
python -m producers.remmaq_producer --sink file --out-dir /tmp/out   # needs network; real REMMAQ call
python -m producers.weather_producer --sink file --out-dir /tmp/out  # needs network; real Open-Meteo call
cat /tmp/out/aq.measurements.raw/*.jsonl | head -1 | python -m json.tool
```

Execute the notebook headlessly:
```bash
cd notebooks
AIRQUITO_DATA_ROOT=$(pwd)/../data/local \
  jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.timeout=-1 01_exploring.ipynb
```
Needs `remmaq/PM2.5.xlsx`, `NO2.xlsx`, `O3.xlsx` and a `probe/<run>/secretaria_get_sites_info.json`
under `AIRQUITO_DATA_ROOT` (gitignored; see `docs/ANALYSIS.md` for where they came from).
First run converts the xlsx files to a parquet cache under `remmaq/parquet/`
(a few minutes); later runs reuse it. Delete the cache folder to force a rebuild.

**Known sandbox quirk, may not apply to you:** a `pyspark` wheel built on
Windows can lose the executable bit on its launch scripts under Linux/macOS. If
`SparkSession.builder.getOrCreate()` fails with `Permission denied:
.../pyspark/bin/spark-submit`, run once: `chmod +x .venv/lib/python3.*/site-packages/pyspark/bin/*`.

---

## 3. Verify on Databricks (Free Edition)

1. **Upload the data to a Unity Catalog volume**, e.g. `workspace.default.airquito`,
   preserving this layout:
   ```
   /Volumes/workspace/default/airquito/
     remmaq/PM2.5.xlsx  NO2.xlsx  O3.xlsx  Marco_QAQC_REMMAQ.pdf
     probe/<run>/secretaria_get_sites_info.json  report.json
     sink/aq.measurements.raw/*.jsonl            (optional, notebook section 7)
   ```
   Don't upload `remmaq/parquet/`; the notebook creates it in the volume.
2. **Add the repo as a Git folder** (Workspace → Repos → Add repo) so
   `producers/stations.py` is importable; without it the notebook falls back to
   an inline copy of the same table and prints which path it used.
3. **Import `notebooks/01_exploring.ipynb`** (or open it directly from the Git
   folder), attach **serverless compute**, **Run all**. `spark` and `display()`
   are injected by Databricks; `pandas` and `openpyxl` are in the runtime.
4. **Running a producer from Databricks**: not how it's deployed (that's GitHub
   Actions), but useful to prove the code runs there too. In a notebook cell:
   ```python
   %pip install confluent-kafka python-dotenv
   import os, sys
   sys.path.insert(0, "/Workspace/Repos/<you>/quito-air-conditions")
   os.environ["SINK"] = "file"
   os.environ["FILE_SINK_DIR"] = "/Volumes/workspace/default/airquito/sink"
   from producers import remmaq_producer
   from producers.runner import run_once
   from producers.settings import Settings
   print(run_once(remmaq_producer.poll, Settings.from_env()).to_json())
   ```
   This needs serverless egress to `aireambiente.quito.gob.ec`, which Databricks
   Free Edition allows for outbound HTTPS.
5. **Running the test suite on Databricks** is not the intended workflow (tests
   run in CI and locally); `%pip install -r requirements.txt && pytest` in a
   notebook works if you want to see it anyway.

---

## 4. How the tests work

### Principles

- **No network, no keys, no broker.** Every HTTP call is intercepted at the
  `requests` layer by the [`responses`](https://github.com/getsentry/responses)
  library, so the client code runs for real (it builds the URL, the headers,
  the body, parses the answer) but the bytes come from a fixture instead of the
  internet. Kafka is replaced by a small fake that records `produce()` calls
  and fires the delivery callback synchronously. The whole suite runs in under
  a second and would pass on a plane.
- **Fixtures are real captures, not invented samples.** The two JSON files in
  `tests/fixtures/` are byte-for-byte what the sources returned on 2026-10-16.
  When a test says "El Camal has zero valid layers" or "the event time is
  22:00 UTC", that is what the Secretaría's server actually said that evening.
  If the source changes shape, the right move is to capture a new payload with
  `scripts/probe_sources.py --only secretaria`, replace the fixture, and watch
  which tests break: that diff is the schema drift.
- **Real code paths, fake boundaries.** The `FileSink` writes to a real
  temporary directory (`tmp_path`) and the tests read the files back. The
  `KafkaSink` is tested with the fake producer *and* constructed once with the
  real `confluent_kafka.Producer` to prove librdkafka accepts the generated
  configuration (no connection is attempted at construction time).
- **Time is pinned.** The `now` fixture is `2026-10-16 23:41:46 UTC`, the
  moment the payload was captured, so `fetched_at` and the publication-delay
  arithmetic are deterministic. Backoff sleeps are replaced by a list that
  records the requested delays, so the retry tests assert on the *values* of
  the delays (2 s, then 4 s, plus jitter) without waiting.

### Data the tests use

| File | Origin | Used by |
|---|---|---|
| `tests/fixtures/remmaq_get_sites_info.json` | One POST to `GetSitesInfo`, 2026-10-16 23:41 UTC (`data/local/probe/20261016T234146Z/`). 9 stations, 89 layers, 73 valid; El Camal fully offline; San Antonio without a PM2.5 layer; hour 17:00 local | `test_remmaq.py`, `test_producers.py`, `test_runner.py` |
| `tests/fixtures/openmeteo_current.json` | One GET for the city centre, 2026-10-16 22:35 UTC (`data/local/probe/20261016T223521Z/`). 13.1 °C, 92 % RH, wind 7.4 km/h from 68° | `test_openmeteo.py`, `test_producers.py`; replicated 9× with each station's coordinates by the `openmeteo_nine` fixture, which is what the real API returns for a 9-point request |
| `producers/stations.py` | Not a fixture, but the tests treat it as reference data: 9 GUIDs, names and coordinates taken from the same capture | `test_config_and_stations.py`, `test_producers.py` |
| Synthetic inputs, built inline | Edge cases the real capture does not contain: a site with no `AqiTime`, a feed with an unknown tenth station, an empty feed, a malformed `/Date()/`, a response that is HTML instead of JSON, a 503 followed by a 200 | the specific edge-case tests, each clearly named |

### Where each fixture lives (`tests/conftest.py`)

| Fixture | What it gives a test |
|---|---|
| `remmaq_payload`, `remmaq_sites` | The feed document, parsed; and its list of 9 sites |
| `openmeteo_one`, `openmeteo_nine` | The single-point answer; and the 9-point answer |
| `mocked` | A `responses` mock; any request to an unregistered URL fails the test |
| `mock_sources` | `mocked` with both sources registered and answering normally |
| `file_settings`, `kafka_settings` | `Settings` for a file sink in `tmp_path`; for a SASL Kafka cluster |
| `now` | The frozen capture time |
| `no_sleep` | Replaces `time.sleep` in the retry loop with a list that records delays |
| `fake_producer_class` | The fake `confluent_kafka.Producer`, configurable to fail some keys or leave records queued |

### Running subsets

```bash
pytest                                  # everything, with coverage report
pytest tests/test_remmaq.py -q          # one file
pytest -k "retry" -q                    # by name
pytest -q --no-cov                      # faster, no coverage
pytest -x --pdb                         # stop at the first failure, open the debugger
```

Coverage is reported for `producers/` only (configured in `pyproject.toml`);
the three uncovered lines are the `if __name__ == "__main__":` guards.

---

## 5. Break the tests by hand

Each row is one edit you make in an editor, the command to run, the failure
you will see (copied from actually doing it), and why that failure matters.
Revert with `git checkout -- <file>` or by undoing the edit; run `pytest -q`
afterwards to confirm 98 passed.

| # | Edit | Run | You will see | Why it matters |
|---|---|---|---|---|
| 1 | `producers/envelope.py`: in `build_envelope`, rename the key `"fetched_at"` to `"fetched"` | `pytest tests/test_envelope.py tests/test_producers.py -q` | `Left contains 1 more item: {'fetched': …}` / `Right contains 1 more item: {'fetched_at': …}` | The envelope is a contract with Bronze; renaming a field without bumping `SCHEMA_VERSION` breaks every consumer silently |
| 2 | `tests/fixtures/remmaq_get_sites_info.json`: find `"SiteName": "El Camal"`, set its first layer's `"IsValid": true` and `"Concentration": 0.5` | `pytest tests/test_remmaq.py tests/test_producers.py -q` | `assert [{'ClassName'…}] == []` and `assert [] == ['El Camal']` | Shows the tests are anchored to the real capture: changing what the source said changes what the producer must report. This is what a schema or data drift looks like in the suite |
| 3 | `producers/config.py`: `TOPIC_MEASUREMENTS_RAW = "aq_measurements_raw"` (underscores) | `pytest tests/test_config_and_stations.py -q` | `FAILED …test_topics_are_distinct_and_namespaced` | Topic names are namespaced with dots by convention; a rename here would create a new empty topic in the cluster and leave consumers reading the old one |
| 4 | `producers/stations.py`: change Tumbaco's latitude from `-0.214933` to `-2.214933` | `pytest tests/test_config_and_stations.py -q` | `AssertionError: Tumbaco is 226 km away` | A typo in a coordinate would put a station on the map in the wrong province and fetch weather for the wrong place |
| 5 | `producers/openaq.py`: in `_get`, replace `params=params,` with `params={**(params or {}), "api_key": api_key},` | `pytest tests/test_openaq.py -q` | `assert 'key' not in 'https://api…&api_key=key'` | Secrets must never be in URLs, which end up in logs and proxies. The test asserts the key travels only in the header |
| 6 | `producers/config.py`: `USER_AGENT = "python-requests"` | `pytest tests/test_http.py -q` | `assert ('AirQuito' in 'python-requests')` | An anonymous client is the kind that gets an IP blocked; the test pins the polite, identifiable agent |
| 7 | `producers/remmaq.py`: delete the line `"X-Requested-With": "XMLHttpRequest",` | `pytest tests/test_remmaq.py -q` | `KeyError: 'x-requested-with'` | ASP.NET page methods can reject calls that do not look like the page's own AJAX; the test pins the exact request shape that worked |
| 8 | `tests/test_producers.py`: in `test_envelope_metadata`, change the expected `event_time` to `"2026-10-16T17:00:00Z"` | `pytest tests/test_producers.py -q` | `assert '2026-10-16T22:00:00Z' == '2026-10-16T17:00:00Z'` | A test can be wrong too. 17:00 is the *local* time the feed shows in `AqiTimeString`; the producer correctly publishes UTC. If this ever "fails", check the timezone reasoning before touching the code |

Things that will **not** break a test, which is also informative:
- Editing a docstring or a comment.
- Reordering the keys in the envelope dict (JSON object equality ignores order).
- Changing the jitter range in `http.py` within `[0, backoff_s / 2]` (tests allow the jitter band).
- Changing `FILE_SINK_DIR`'s default (tests always pass an explicit `tmp_path`).

---

## 6. Break the tests with the script

`scripts/break_tests.py` applies one surgical, reversible edit at a time to a
real contract in `producers/`, tells you which test to run and why it should
fail, and restores on request. State is tracked in `.break_tests_state.json`
(gitignored), so `restore --all` is a safety net even in a fresh shell.

```bash
python -m scripts.break_tests list              # every available break
python -m scripts.break_tests break http-no-retry
python -m scripts.break_tests status             # what's currently broken
pytest tests/test_http.py -q                      # watch it fail
python -m scripts.break_tests restore http-no-retry
python -m scripts.break_tests restore --all       # if you lose track
```

| Break | Breaks | Test that catches it | What not catching it would mean |
|---|---|---|---|
| `http-no-retry` | Retry policy: no status is retryable | `test_http.py`, `test_remmaq.py::test_server_error_is_retried` | The producer dies on the first 503 instead of riding out a blip |
| `envelope-allows-naive-datetime` | `iso_utc()` stops rejecting naive datetimes | `test_envelope.py::test_iso_utc_rejects_naive` | A future local-time bug would silently mis-tag as UTC instead of crashing loudly |
| `remmaq-key-is-name-not-guid` | Kafka key becomes the station name, not the GUID | `test_producers.py` (4 tests) | A station rename would split one station's history across two Kafka keys |
| `kafka-sink-swallows-failures` | `KafkaSink.flush()` stops raising on a failed delivery | `test_sinks.py` (2 tests) | A broker rejecting every record would look like a green, successful run |
| `kafka-acks-weakened` | `acks` becomes `"1"` instead of `"all"` | `test_settings.py::test_kafka_config_is_at_least_once` | At-least-once quietly degrades to best-effort |
| `dotnet-date-off-by-1000` | `/Date(ms)/` parsed as seconds | `test_remmaq.py::TestParseDotnetDate` (4 tests) | Every event time would land in the year 58766 |
| `runner-hides-source-failure` | CLI exits 0 even when the source failed | `test_runner.py` (2 tests) | A scheduler only sees a dead producer through its exit code; this hides every outage |

All seven were applied, observed to fail exactly the listed tests, and
restored as part of writing this document; `git diff` is clean and the full
suite passes. Try one yourself; nothing here needs network access.

---

## 7. What's next (Stage 2)

Tracked in `docs/PROJECT_PLAN.md` section 9: create the Kafka cluster and send
for real, Bronze (including the xlsx backfill), Silver (the rules from
`ANALYSIS.md` section 3), Gold, the Postgres push, and the first deployed page.
