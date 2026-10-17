# Analysis: sources, discoveries and objectives

What was learned about the data before and during Stage 1 (producers), and what
the project can therefore promise. The evidence is in the repository: probe
reports under `data/local/probe/` (not committed, reproducible with
`scripts/probe_sources.py`), the executed notebook
[`notebooks/01_exploring.ipynb`](../notebooks/01_exploring.ipynb), and the test
fixtures in `tests/fixtures/`, which are real captures.

**Session:** 2026-10-16 / 17. **Scope:** Stage 1, producers.

---

## 1. Objectives of the project

The plan's pitch, restated as claims the data has to support:

| # | Claim | Status after this session |
|---|---|---|
| 1 | A live map of Quito's monitoring stations with current air quality | **Supported.** Nine REMMAQ stations, hourly values published about 70 minutes after the hour, reachable through one unauthenticated call. |
| 2 | "Is it a good time to exercise outside" per neighbourhood | **Supported.** Hour-of-day patterns are strong (PM2.5 and NO₂ peak at 07:00, O₃ at noon), and the station IQCA with its health advisory is in the feed. |
| 3 | 24 h and 7 d trends per station | **Supported** once Bronze accumulates; backfillable to **2004** from the Secretaría's validated exports. |
| 4 | Alerts when a threshold is crossed, and when it clears | **Supported** on concentration thresholds today; index-based thresholds need the full IQCA breakpoint table (open item). |
| 5 | An honest pipeline health view | **Supported.** Publication delay, offline stations and quarantine rate are all observable from the producer summary and the Silver rules. |
| 6 | Real data, with real problems | **Over-delivered.** Schema drift between files of the same publisher, a drifting export clock, an undocumented endpoint, stations that go dark for months, a validated history whose upper tail was cut. |

What the project does **not** claim: sub-second streaming, more than nine
stations, or independence from a municipal web portal that could change.

---

## 2. Source evaluation

### 2.1 OpenAQ is stale for Quito

The plan's primary source was OpenAQ v3. `scripts/discover_stations.py` listed
10 locations and 51 sensors within 25 km of the centre; `/locations/{id}/latest`
confirmed the newest reading per station:

| Provider | Locations | Newest reading |
|---|---|---|
| Quito REMMAQ (government) | 8 | **2021-11-24** |
| AirGradient (two low-cost sensors) | 2 | 2025-02-19 and 2025-04-04 |

REMMAQ stopped syncing to OpenAQ almost five years before this session.
Belisario and Jipijapa never appeared. OpenAQ is therefore **demoted** to the
evidence trail: the client and the discovery script stay in the repository, no
producer reads it.

### 2.2 Other candidates, probed on 2026-10-16

One polite request each (`scripts/probe_sources.py`, 3 s apart, descriptive
User-Agent, no retries):

| Source | Result | Verdict |
|---|---|---|
| AirGradient open data | 2 906 live sensors worldwide, **0 within 25 km** | No coverage |
| Sensor.Community | Empty | No coverage |
| PurpleAir | Not probed (needs a key) | Open |
| Open-Meteo weather | Live, 15 min cadence | **Kept** as secondary source |
| Open-Meteo air quality (Copernicus CAMS) | Live, hourly, 0.1° grid; two distinct PM2.5 cells across the city | **Kept for later**: city-level baseline and 5-day forecast, not per station |
| WAQI / aqicn (cross-check only) | Centro station live at 16:00 local, attribution `ambiente.quito.gob.ec` | Proved REMMAQ was still publishing; never a source (terms forbid storage) |
| `quitoambiente.gob.ec`, `datosabiertos.quito.gob.ec` | Domains no longer resolve | Dead |
| `ambiente.quito.gob.ec` | WordPress brochure with image-banner links to two applications | Led to the real sources |

### 2.3 The REMMAQ portal, found by reading its JavaScript

`aireambiente.quito.gob.ec` is an Envista ARM web portal (ASP.NET WebForms).
Its map loads station markers through an ASP.NET AJAX **page method**:

```
POST https://aireambiente.quito.gob.ec/Modules/Maps/GoogleMap.aspx/GetSitesInfo
Content-Type: application/json        body: {}
→ {"d": [site, ...]}
```

No authentication. One call returns every station with: GUID, name,
coordinates, station IQCA (value, category, responsible pollutant, health
advisory text in Spanish), a per-pollutant IQCA list, and **eleven layers**
(CO, NO₂, O₃, SO₂, PM2.5, PM10, temperature, solar radiation, rain, wind speed
and direction), each with a concentration, unit, `IsValid` flag and timestamp.
Timestamps are `.NET` `/Date(ms)/`, epoch milliseconds in UTC.

`datosambiente.quito.gob.ec` serves the validated history: one `.rar` per
parameter, each an xlsx with a `LIMPIO` sheet, hourly since **2004**, plus
`Marco_QAQC_REMMAQ.pdf`, the network's own validation protocol (summarised in
[remmaq-qaqc.md](remmaq-qaqc.md)).

**Risk, recorded:** this is an internal endpoint of a vendor product, not a
published API. It can change without notice. Mitigations: the producer
validates the response shape and exits non-zero on drift (`RemmaqFormatError`,
tested), the data contract documents the dependency, and Bronze stores the raw
payload so a change never loses data already fetched.

---

## 3. Discoveries about the data

From the notebook; section numbers refer to it.

| # | Discovery | Consequence |
|---|---|---|
| §1 | Schema drift between exports of the same publisher: `Fecha` vs `FECHA`, NO₂ workbook has no San Antonio column | Normalise headers; never assume a fixed station list per file |
| §2 | pandas `NaN` is not a Spark null | Convert on load, or every coverage number is a false 100 % |
| §3 | The export clock drifts: −5 ms before the hour from 2025-12, +7.5 s by 2026-08 | **Round** to the nearest hour; truncating files seven months under the previous hour |
| §3 | NO₂ and O₃ exports lack 191 and 187 rows (whole hours absent) | A missing row and a null value both mean "no observation" |
| §4 | History is **local time** (O₃ peaks 12:00, NO₂/PM2.5 07:00); feed is UTC | `to_utc_timestamp(ts, 'America/Guayaquil')` for history; Silver key in UTC |
| §5 | Three station generations (2004, 2013–2017); working coverage 93–99 %/year | Per-parameter availability windows in `silver.stations`, not a boolean |
| §5 | Current outages: El Camal since 2026-06-15, Guamaní Sep 2025 → Aug 2026, San Antonio PM2.5 since 2026-04-17, Los Chillos PM2.5 and Guamaní NO₂ invalid in feed | The map needs `stale` / `down` / `not_measured` states |
| §6 | All 9 GUIDs ↔ names ↔ xlsx headers map one-to-one | `producers/stations.py` is complete |
| §6 | Hourly value appears about **70 min** after the hour | 10-minute polling, 3-hour staleness alarm |
| §6 | The feed is the **unvalidated** twin of `LIMPIO` | Quarantine table, not a filter |
| §7 | Producer output reads directly with `spark.read.json`; `explode(LayerInfos)` is the Silver transform | 9 Bronze records → ~90 Silver rows per poll |
| §8 | No negatives in 22 years; PM2.5 never above **104.7 µg/m³** | The validation cut the upper tail; live limits must come from instrument range, not history maxima |
| §9 | The Secretaría's QA/QC rules reproduce `LIMPIO` within 0.05–0.44 %, but global MAD/tail rules flag real multi-hour episodes | **Quarantine** on physical limits and spikes; **flag** on MAD and tails; compute MAD per station, parameter and month |
| §10 | IQCA first-segment slopes: PM2.5 ×2, PM10 ×1, O₃ ×1, NO₂ ×0.5, SO₂ ×0.83, CO ×10; index concentrations use rolling windows (24 h PM2.5, 8 h O₃/CO, 1 h NO₂) | Feed IQCA usable for "now"; full breakpoint table needed for history and alerts |
| §11 | PM2.5 yearly means fell from ~20 to ~13 µg/m³; 2020 lockdown dip at every station; WHO 24 h guideline exceeded on 30 % of station-days in 2025, national limit on none | Show both scales; the story is ordinary days, not disasters |

---

## 4. Design decisions taken in Stage 1

| Decision | Alternative considered | Why |
|---|---|---|
| One Kafka message per **station** (full site document), keyed by station GUID | One message per sensor reading, as the original plan said (OpenAQ era) | The station document is the source's unit; the IQCA block is station-level; splitting before Bronze is a transformation |
| **Stateless** producer, at-least-once | Remember the last event time and skip unchanged readings | ~54 records/hour is nothing; restartability and simplicity win; Silver deduplicates on the natural key |
| GitHub Actions cron every 10 min as the deployment | Lambda + EventBridge; Databricks job; always-on machine | Zero infrastructure, already in the repo, adequate for an hourly source; Lambda kept as documented alternative |
| `FileSink` alongside `KafkaSink` | Kafka only | Runs in the sandbox and in tests; output doubles as Bronze replay fixture; same producer code |
| Payload as raw JSON string in Bronze | Typed struct | A new source field must never fail the Bronze write |
| Polite HTTP: descriptive User-Agent, 3 attempts, exponential backoff, no retry on 4xx | Aggressive retries | An IP of this project has been blocked before |
| Poetry stays the source of truth; one `requirements.txt` exported from the lock, checked in CI | Two requirements files; or drop Poetry | Owner's preference; the sync check removes the drift risk |

---

## 5. Open items

1. **IQCA breakpoint table** (Ordenanza Metropolitana / Secretaría definition) for
   all six pollutants and categories. Needed before index-based alerts and
   historical index charts.
2. **Remaining history exports**: `CO.rar`, `SO2.rar`, `PM10.rar` from
   `datosambiente.quito.gob.ec`. The notebook's `FILES` dict is the only place
   to add them.
3. **Instrument range limits** per analyser, to replace the provisional physical
   limits in the notebook (PM2.5 1000, NO₂ 1000, O₃ 600 µg/m³).
4. **Hour-label convention** (hour ending vs starting). Irrelevant at hourly grain,
   worth one email to the Secretaría.
5. **First real Kafka send** from a machine with network access, against Confluent
   Cloud or Redpanda Serverless; the sandbox could only verify construction and
   delivery-callback handling with a fake producer.
6. **Gap 2026-09-01 → first Bronze record**: in neither source until the next export.
