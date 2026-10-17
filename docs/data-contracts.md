# Data contracts

What each producer puts on its topic, what consumers may rely on, and what they
must not. A contract here is a promise made by code in `producers/` and checked
by a test in `tests/`; the test is named next to each statement.

Contracts for the CDC and alert topics arrive with Phase 3.

---

## 1. The envelope (all topics)

Every message value is one UTF-8 JSON object with exactly these top-level fields
(`tests/test_envelope.py`):

| Field | Type | Meaning |
|---|---|---|
| `source` | string | Producer name: `remmaq`, `openmeteo` |
| `schema_version` | int | Version of this envelope. Currently **1**. Bumped on any change to the fields above or below; consumers branch on it |
| `key` | string | The Kafka message key, repeated so a record read from a file is self-describing |
| `event_time` | string or null | When the observation is **for**, ISO 8601 UTC with `Z`. The source's clock. Null only when the source gave no usable time (the record then goes to quarantine in Silver) |
| `fetched_at` | string | When the producer fetched it, ISO 8601 UTC with `Z`. Our clock. `fetched_at − event_time` is the publication delay |
| `payload` | object | The source record, **unchanged**. No field is renamed, cast, dropped or added |

Serialisation: compact JSON (`,` and `:` separators, no spaces), non-ASCII kept
as UTF-8, not `\u`-escaped.

**Guarantees**
- Delivery is **at-least-once**. The same observation is normally sent several
  times (the producer polls every 10 minutes, the source changes hourly).
  Consumers deduplicate on the natural key, never on `fetched_at`.
- Records for one key are in order within a partition (keyed by station).
- A record is acknowledged only when all in-sync replicas have it (`acks=all`,
  idempotent producer; `tests/test_settings.py`).

**Non-guarantees**
- No uniqueness. No exactly-once.
- No schema for `payload` beyond what the source happens to send. Sections 2
  and 3 describe the shape observed on 2026-10-16, which is also what the
  fixtures in `tests/fixtures/` contain; they are documentation, not promises.

---

## 2. `aq.measurements.raw`

| | |
|---|---|
| Producer | `producers/remmaq_producer.py` |
| Source | `POST https://aireambiente.quito.gob.ec/Modules/Maps/GoogleMap.aspx/GetSitesInfo`, body `{}` |
| Key | REMMAQ station GUID (`payload.SourceSiteID`), e.g. `f1f5d7e6-09f3-4966-ad15-f6ad47b89973` |
| Granularity | **One message per station per poll**, all layers included |
| `event_time` | `payload.AqiTime` (`/Date(ms)/` → UTC); falls back to the newest `LayerInfos[].DataTime`; null if neither |
| Cadence | Poll every 10 min; the source updates hourly, about 70 min after the hour |
| Expected volume | 9 messages per poll, ~54 per hour, ~50 KB per poll |
| Update frequency of the source | Hourly |

### Payload shape observed (`tests/fixtures/remmaq_get_sites_info.json`)

```
SourceSiteID            string  GUID, stable identifier
SiteName                string  "Belisario", "LosChillos" (no space), "El Camal" (space)
Latitude, Longitude     double
AqiTime                 string  "/Date(1792188000000)/"  epoch ms, UTC
AqiTimeString           string  "2026-10-16T17:00:00"     local time, no offset; ignored
AqiValue                int|null  station IQCA, null when no valid layer
AqiCategory             string  Deseable | Aceptable | Precaución | Alerta | Alarma | Emergencia | unknown
AqiMessage              string  Spanish text naming the responsible pollutant
AqiResponsibleParameterName / Concentration / UnitName
HealthAdvisory, HealthEffects, SensitiveGroups   Spanish text
LayerInfos[]            array of 6–11 objects:
  ParameterName         string  CO_mg | NO2_ug | OZONO_ug | SO2_ug | PM2.5_ug | PM10_ug |
                                TEMP_AMB | RAD_SOLAR | LLUVIA | RAP_VEC | DIR_VEC
  UnitName              string  mg/m3 | ug/m3 | °C | W/m2 | mm | m/s | DEG
  Concentration         number|null   the hourly value
  IsValid               bool
  Message               string|null   "N/A" when invalid
  DataTime              string  "/Date(ms)/"
SiteAqiResults[]        per-pollutant IQCA: AqiValue, AqiResponsibleParameterName,
                        AqiResponsibleParameterConcentration (rolling-window value,
                        not the hourly one), category and advisory text
```

Things a consumer **must** handle, all seen in the capture:
- A station with **every layer invalid** (El Camal): `AqiValue` null, `AqiCategory`
  `"unknown"`, `Concentration` null everywhere. Still published; it is a station.
- A station **missing a layer entirely** (San Antonio has no PM2.5 layer) versus a
  station whose layer is present but `IsValid: false` (Los Chillos PM2.5). Different
  meanings: "not measured here" versus "down".
- **CO in mg/m³**, everything else in µg/m³. Carry `UnitName` into Silver.
- `Concentration` may be an integer (`0`, `39`) or a float. Cast.
- A **station not in `producers/stations.py`** is still published, with a warning
  in the run summary (`tests/test_producers.py::test_unknown_station_is_warned_not_dropped`).

What makes the producer **fail** (exit 1, nothing written;
`tests/test_remmaq.py::TestFetchSitesInfo`, `tests/test_runner.py`):
- Response is not JSON, or not `{"d": [...]}`.
- A site without `SourceSiteID`, `SiteName` or `LayerInfos`.
- HTTP 4xx other than 429; or 429/5xx/connection failure three times in a row.

### Known fragility

The endpoint is an internal page method of the Envista ARM portal, not a
documented API. It may change shape or disappear without notice. If it does,
the producer fails loudly on the next poll and the GitHub Actions run turns
red; nothing already in Kafka or Bronze is affected.

---

## 3. `weather.current.raw`

| | |
|---|---|
| Producer | `producers/weather_producer.py` |
| Source | `GET https://api.open-meteo.com/v1/forecast`, one request for all nine station coordinates, `timezone=UTC` |
| Key | REMMAQ station GUID (same as topic 2, so weather joins to measurements by key) |
| Granularity | One message per station per poll |
| `event_time` | `payload.current.time` (ISO, UTC as requested) |
| Cadence | Same 10-minute poll; the source updates every 15 min |
| Expected volume | 9 messages per poll, ~500 B each |

### Payload shape observed (`tests/fixtures/openmeteo_current.json`)

```
latitude, longitude     double  the grid point Open-Meteo snapped to, not the station's
elevation               double
timezone                "GMT"   utc_offset_seconds 0
current_units           {time: "iso8601", temperature_2m: "°C", relative_humidity_2m: "%",
                         wind_speed_10m: "km/h", wind_direction_10m: "°"}
current                 {time: "2026-10-16T22:30", interval: 900,
                         temperature_2m, relative_humidity_2m, wind_speed_10m, wind_direction_10m}
```

Note that REMMAQ's own layers also include temperature, solar radiation, rain
and wind at the station. Open-Meteo is the gridded model complement and, in a
later phase, the forecast; when both exist, the station measurement wins.

What makes the producer fail: response not JSON, number of results different
from the number of stations requested, a result without `current`
(`tests/test_openmeteo.py`).

---

## 4. History exports (not a topic)

The validated history does not pass through Kafka. It is loaded once by a
backfill job into `bronze.measurements_history` from files in a volume:

| | |
|---|---|
| Source | `https://datosambiente.quito.gob.ec/datos/<PARAM>.rar` → `<PARAM>.xlsx`, sheet `LIMPIO` |
| Shape | Wide: timestamp column (`FECHA` or `Fecha`) then one column per station (upper case, with spaces) |
| Time | **Local** (`America/Guayaquil`), no offset; stamps drift by up to ±7.5 s after 2025-12; **round** to the hour |
| Values | Validated by the Secretaría's QA/QC; no negatives; upper tail cut (PM2.5 max 104.7 in 22 years) |
| Station mapping | `producers/stations.py` `history_column` ↔ `station_id` |

Details and evidence: `notebooks/01_exploring.ipynb`, sections 1–5 and 8.

---

## 5. Politeness policy (all HTTP sources)

Enforced by `producers/http.py`, tested in `tests/test_http.py`:

- `User-Agent: AirQuito/0.1 (+https://github.com/bernabecj/quito-air-conditions)`
- One request per source per poll.
- Retries only on connection errors, timeouts, HTTP 429 and 5xx: at most 3
  attempts, delays of 2 s and 4 s plus jitter. Any other 4xx fails immediately.
- Poll interval is never below 5 minutes in production.
