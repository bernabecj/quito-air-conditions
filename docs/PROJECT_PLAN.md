# Project Plan

This is the planning doc for AirQuito: the pitch, the data sources, the target
architecture and the phased build plan. For setup instructions and what exists
today, see the [README](../README.md).

**Status:** Phase 0 (setup) done. Phase 1 in progress: producers, tests,
exploration notebook and docs are done (Stage 1); the Delta tables, the Postgres
push and the site are next. The primary source changed from OpenAQ to the
Secretaría de Ambiente's own portal, see section 3 and
[docs/ANALYSIS.md](ANALYSIS.md).
**Created:** 2026-10-15. **Updated:** 2026-10-17.

**The idea:** a continuously running data pipeline that reads Quito's public air
quality measurements, stores them in a lakehouse, publishes a free public
website, and sends alerts to people who subscribe to their neighborhood.

---

## 1. Why this project

Three goals, in order of importance:

1. **Real data.** No synthetic generators. Real feeds bring late records,
   missing stations, duplicates and schema drift, which is what makes a
   pipeline worth showing.
2. **Real users.** A public URL and a subscribe button, not a notebook
   screenshot.
3. **Covers known gaps.** Delta Lake, change data capture, Kafka and streaming,
   medallion depth, and data governance with personal data.

It must stay clearly different from the FinGuard fraud pipeline and from the
[Quito Transport Platform](https://github.com/bernabecj/quito-transport-platform),
which is a daily batch on AWS. The differentiators are: real public source, a
public frontend, a change data capture stream, and real personal data that needs
masking and retention rules.

---

## 2. What the product does

**Public, no login**
- Map of Quito with current air quality per monitoring station.
- Plain answer to "is it safe to exercise outside right now in my neighborhood".
- Trend for the last 24 hours and last 7 days per station.
- Short explanation of why, using wind and temperature.

**For subscribers**
- Pick a neighborhood (nearest station) and a threshold.
- Receive a Telegram message when the threshold is crossed.
- Receive one message when conditions return to normal, not a message per reading.

---

## 3. Data sources

### Primary: REMMAQ through the Secretaría de Ambiente portal

The Red Metropolitana de Monitoreo Atmosférico de Quito (REMMAQ), run by the
Secretaría de Ambiente of the Municipio del Distrito Metropolitano de Quito.
Nine stations: Belisario, Carapungo, Centro, Cotocollao, El Camal, Guamaní,
Los Chillos, San Antonio, Tumbaco. (Jipijapa, in the first draft of this plan,
exists in no source.)

- **Live values**: `aireambiente.quito.gob.ec`, an Envista ARM web portal. Its
  map is fed by an ASP.NET page method, `POST .../Modules/Maps/GoogleMap.aspx/GetSitesInfo`
  with body `{}`, which returns every station's latest hourly values for eleven
  layers (CO, NO₂, O₃, SO₂, PM2.5, PM10, temperature, solar radiation, rain,
  wind speed and direction) plus the station IQCA with its health advisory. No
  key. Values appear about 70 minutes after the hour. Found by reading the
  page's JavaScript with `scripts/probe_sources.py`; it is not a documented API,
  and the data contract records that risk.
- **History**: `datosambiente.quito.gob.ec`, one `.rar` per parameter with a
  validated (`LIMPIO`) hourly xlsx since 2004, plus the network's QA/QC protocol
  ([summary](remmaq-qaqc.md)). Used for backfill and for the exploration notebook.
- Units: CO in mg/m³, other pollutants in µg/m³. The feed's IQCA (Índice
  Quiteño de Calidad del Aire) is usable for "now"; the full breakpoint table is
  still to be obtained for history and alerts.
- Verified 2026-10-16. Evidence and details: [docs/ANALYSIS.md](ANALYSIS.md),
  [docs/data-contracts.md](data-contracts.md), `notebooks/01_exploring.ipynb`.

### Demoted: OpenAQ
- The original primary source. API v3, free key in the `X-API-Key` header,
  base `https://api.openaq.org/v3`, Ecuador country id `137`.
- **Stale for Quito**: the REMMAQ stations stopped syncing on 2021-11-24 and the
  two AirGradient sensors in 2025. Confirmed with `/locations/{id}/latest` on
  2026-10-16.
- The client (`producers/openaq.py`) and `scripts/discover_stations.py` stay as
  the record of the discovery. No producer reads OpenAQ.

### Secondary: weather
- Open-Meteo, free, no key required, updated every 15 minutes.
- One request for all nine station coordinates:
  `https://api.open-meteo.com/v1/forecast?latitude=…&longitude=…&current=temperature_2m,relative_humidity_2m,wind_speed_10m,wind_direction_10m&timezone=UTC`
- Wind and temperature explain most pollution spikes, especially thermal
  inversions. This turns a dashboard into an explanation. REMMAQ's own
  meteorological layers take precedence where both exist; Open-Meteo is the
  gridded complement.
- Open-Meteo's air quality endpoint (Copernicus CAMS model, 0.1° grid, 5-day
  forecast) is reserved for a later phase as a city-level forecast layer.

### Checked and rejected as live sources (2026-10-16)
- **AirGradient open data**: 2 906 sensors worldwide, none within 25 km of Quito.
- **Sensor.Community**: no sensors in the area.
- **PurpleAir**: not probed, needs a key. Low expected coverage.

### Do not build on aqicn.org (World Air Quality Index)
It is the easiest option and it is a trap. Its usage policy forbids caching and
redistribution of the data, requires attribution, and restricts commercial use.
A project whose whole point is storing history cannot legally sit on a feed that
prohibits storing it. Use it only as a manual cross check when a number looks
wrong.

### Checked and rejected
- **Quito public transport real time feed.** Would be the most useful project of
  all, but Quito does not publish a GTFS or GTFS Realtime feed (verified in the
  Quito Transport Platform against transit.land and the Mobility Database).
  Blocked unless scraping vehicle positions, which is not worth building a
  portfolio on.

---

## 4. Architecture

```
REMMAQ portal ──┐
                ├──► Python producers ──► Kafka ──┐
Open-Meteo API ─┘                                 │
                                                  ├──► Databricks (Delta)
Postgres (subscribers) ──► Debezium ──► Kafka ────┘    bronze / silver / gold
                                                            │
                                   ┌────────────────────────┤
                                   ▼                        ▼
                         Postgres (serving)          Telegram bot
                                   │
                                   ▼
                          Next.js public site
```

| Layer | Choice | Notes |
|---|---|---|
| Producers | Python, one per source, stateless, at-least-once | GitHub Actions cron every 10 min (`.github/workflows/producers.yml`); Lambda + EventBridge is the documented alternative |
| Broker | Confluent Cloud free tier | Already familiar from FinGuard. Redpanda Serverless is the alternative |
| Operational database | Postgres on Supabase or Neon, free tier | Subscribers, thresholds, delivery log |
| Change capture | Debezium reading the Postgres write ahead log | Produces the subscriber change stream |
| Processing | Databricks Free Edition, Delta Lake, Unity Catalog | Triggered micro batch, not continuous. See section 7 |
| Serving store | Postgres table written back from gold | Databricks Free Edition cannot serve a public page |
| Frontend | Next.js on Vercel or Cloudflare Pages | Free tier, must work well on a phone |
| Alerts | Telegram bot | Free, no app store review, people actually use it |

The interesting technical piece is the join: the measurement stream meets the
subscriber table, which arrives as a change stream from Postgres, and the job
decides who gets notified. That is a stream to table join over slowly changing
data. It exists here because the product needs it, not as a demonstration.

---

## 5. Data model

### Kafka topics
| Topic | Key | Contents |
|---|---|---|
| `aq.measurements.raw` | station id (REMMAQ GUID) | One record per station per poll: the site document with all layers, as returned by the source |
| `weather.current.raw` | station id (same GUID) | Current conditions at each station's coordinates |
| `subscribers.cdc` | subscriber id | Debezium change events from Postgres |
| `alerts.outbound` | subscriber id | Alerts decided by the pipeline, consumed by the bot |
| `alerts.delivery` | subscriber id | Delivery result, success or failure, with latency |

Topic names live in [`producers/config.py`](../producers/config.py); the
message envelope and payload shapes are in [data-contracts.md](data-contracts.md).

### Delta tables

All tables are Delta (parquet files plus a transaction log) in Unity Catalog. What
distinguishes the layers is the *content*, not the file format: Bronze keeps the
source record exactly as it arrived, in whatever shape the source used; Silver
holds typed, long-format, deduplicated rows; Gold holds what the product reads.

**Landing formats, before Bronze**
| Source | Arrives as | Path into Bronze |
|---|---|---|
| REMMAQ live feed | JSON document per station, inside the producer envelope | Kafka `aq.measurements.raw` → triggered micro-batch |
| Open-Meteo | JSON per station, inside the envelope | Kafka `weather.current.raw` → same job |
| REMMAQ history | xlsx, one workbook per parameter, sheet `LIMPIO`, wide (one column per station), local time | one-off backfill job reading the files from a volume |
| Subscribers | Debezium change events (JSON) | Kafka `subscribers.cdc` |

**Bronze** (append only, nothing dropped, nothing cast)
- `bronze.measurements`: one row per Kafka record: `key`, `topic`, `partition`,
  `offset`, `kafka_timestamp`, the envelope fields (`source`, `schema_version`,
  `event_time`, `fetched_at`) and `payload` as the raw JSON string. Keeping the
  payload as a string rather than a struct means a new field from the source
  never fails the write.
- `bronze.measurements_history`: the xlsx rows as read, one row per workbook row
  with the original timestamp (local, drifting), the original column headers,
  plus `source_file` and `loaded_at`. Wide format preserved.
- `bronze.weather`: same approach as `bronze.measurements`.
- `bronze.subscribers_cdc`: raw change events, including the operation type.

**Silver** (typed, long format, one row per natural key)
- `silver.measurements`: one row per `(station_id, parameter, event_hour_utc)`.
  Columns: `station_id`, `parameter`, `event_hour_utc`, `value`, `unit`,
  `is_valid_at_source`, `quality_flags` (array: `mad`, `tail`), `origin`
  (`live` or `history`), `ingested_at`. Built from Bronze by: exploding the
  feed's `LayerInfos`; unpivoting the history; **rounding** history timestamps
  to the nearest hour and converting from `America/Guayaquil` to UTC;
  deduplicating on the key, live over history when both exist. Rules from
  `notebooks/01_exploring.ipynb`, section 3, 4 and 9.
- `silver.measurements_quarantine`: rows rejected by the physical-limit or
  spike rules, with `rule`, `threshold`, `value` and the Bronze reference.
  Never silently discard.
- `silver.stations`: station reference, slowly changing type 2, with a
  per-parameter availability window (`first_value`, `last_value`) rather than a
  single active flag.
- `silver.weather`: one row per `(station_id, event_time_utc)`.
- `silver.subscribers`: current state of each subscriber, rebuilt from the
  change stream with a merge. Personal fields masked in this layer.

**Gold**
- `gold.station_latest`: current value, source IQCA and category per station,
  plus `freshness_minutes` and a `status` of `ok`, `stale`, `down` or
  `not_measured` per parameter.
- `gold.station_hourly`: hourly series the charts read, with 24 h rolling
  PM2.5 and 8 h rolling O3 and CO for index computation.
- `gold.alert_candidates`: crossings detected, before the subscriber join.
- `gold.pipeline_health`: publication delay per poll, rows processed, stations
  offline, quarantine rate, alert delivery latency. This is the table that
  proves the pipeline is alive.

### Postgres (operational)
```sql
subscribers (
  id, telegram_chat_id, station_id, parameter,
  threshold, active, created_at, updated_at
)
alert_log (
  id, subscriber_id, station_id, parameter,
  value, sent_at, status
)
```

---

## 6. Alert logic

Avoid the two obvious failure modes: spam and silence.

- Compute the index per station from the latest reading.
- A crossing fires only when the value moves from below the threshold to above it,
  not on every reading above it.
- Apply hysteresis: the "back to normal" message fires only after the value drops
  a clear margin below the threshold, for example ten percent, so a value sitting
  on the line does not toggle.
- Cool down period per subscriber per parameter, for example six hours.
- If a station stops reporting for more than three hours, do not alert on stale
  data. Mark it as unavailable on the site instead.

---

## 7. Known weakness and how to answer it

Government reference stations publish roughly once per hour. This is not sub
second streaming, and an interviewer will ask why Kafka sits in front of hourly
data. Do not hide it.

**Answer one, be precise.** Describe it as "a continuously running pipeline with
end to end freshness of a few minutes after each publication, fanning in two
sources and fanning out to two consumers". Precision reads as experience.
Claiming real time big data over ten stations reads as inexperience. The broker
is justified by multiple producers and multiple consumers, plus replay, not by
throughput.

**Answer two, the fast stream is the human one.** Subscriptions, alert
deliveries, bot interactions and page views are genuine high frequency events
once the site has users, and they flow through the same pipeline. Measuring your
own alert latency and delivery failures is a real operational problem.

**Cost warning.** Databricks Free Edition is serverless only, so a continuously
running stream will consume the compute allowance fast. Use a trigger every five
or ten minutes instead. The source publishes hourly so nothing real is lost, and
"I chose triggered micro batch because the source publishes hourly and continuous
compute was not justified" is a strong interview answer.

---

## 8. Governance

Subscriber records contain a Telegram identifier and a neighborhood, which is
personal data. This gives a genuine reason to do governance properly rather than
as a tutorial exercise.

- Separate catalogs or schemas for public data and personal data.
- Column masking on the Telegram identifier in anything outside the operational
  layer.
- Retention rule: delete alert log rows after ninety days.
- A short data contract for each source: fields, types, update frequency,
  expected ranges, what happens on violation.
- Attribution on the public site: data from the Red Metropolitana de Monitoreo
  Atmosférico, Secretaría de Ambiente del DMQ, with a link to
  `ambiente.quito.gob.ec`; weather by Open-Meteo.
- A plain privacy note on the site saying what is stored and how to delete it.

---

## 9. Build plan

This must ship in small pieces. A deployed small thing beats an undeployed big
thing.

### Phase 0, setup (done)
- [x] Repository, licence, `.gitignore`, `.env.example`.
- [x] Poetry project, black and isort, pytest configured (no tests yet).
- [x] CI on GitHub Actions: formatting check and unit tests.
- [x] OpenAQ client (`producers/openaq.py`) and topic names (`producers/config.py`).
- [x] `scripts/discover_stations.py` to list the stations and sensors around Quito.

### Phase 1, about two weeks, this is the one that matters

Stage 1 (done, 2026-10-17):
- [x] Run `scripts/discover_stations.py`; found OpenAQ stale since 2021.
- [x] `scripts/probe_sources.py`: probe every candidate; found the REMMAQ portal
      and its page method; confirmed the network is live.
- [x] Producer one: `producers/remmaq_producer.py` → `aq.measurements.raw`.
- [x] Producer two (pulled forward from Phase 2): `producers/weather_producer.py`
      → `weather.current.raw`.
- [x] Shared producer code: envelope, file and Kafka sinks, polite HTTP with
      retry, env-driven settings, runner CLI with `--loop`.
- [x] Deployment: GitHub Actions cron every 10 minutes, file-sink dry run on demand.
- [x] Exploration notebook `notebooks/01_exploring.ipynb` over 22 years of history
      and the live feed; rules for Silver derived from it.
- [x] Tests: 98, no network, real captured payloads as fixtures. CI runs them.
- [x] Docs: `ANALYSIS.md`, `data-contracts.md`, this plan, README.

Stage 2 (next):
- [ ] Create the Kafka cluster (Confluent Cloud or Redpanda), the two topics,
      the GitHub secrets; first real send; confirm records in the topic.
- [ ] Bronze: `bronze.measurements` and `bronze.weather` from Kafka, triggered
      micro-batch every 10 minutes; `bronze.measurements_history` from the xlsx.
- [ ] Silver: `silver.measurements` (explode, unpivot, round, UTC, dedup),
      `silver.measurements_quarantine`, `silver.stations`, `silver.weather`.
- [ ] Gold: `station_latest` with `status` per parameter, `pipeline_health`.
- [ ] Obtain the IQCA breakpoint table and the remaining history exports
      (CO, SO₂, PM10).
- [ ] Push gold to Postgres.
- [ ] One page site with the map and current values. **Deploy it.**
- [ ] Link the live site from the README.

### Phase 2, about one week
- [ ] Hourly aggregates and the trend charts, backfilled from the history.
- [ ] Open-Meteo CAMS forecast layer (city level).
- [ ] Quarantine table and basic data quality checks with Great Expectations.
- [ ] `pipeline_health` table and a freshness indicator shown on the site.

### Phase 3, about two weeks
- [ ] Subscriber table in Postgres and a subscribe flow on the site.
- [ ] Debezium change capture into Kafka.
- [ ] Merge into `silver.subscribers`.
- [ ] Stream to table join producing `alerts.outbound`.
- [ ] Telegram bot consuming the topic and writing delivery results back.

### Phase 4, polish
- [ ] Masking, retention and the data contracts.
- [ ] Backfill of historical data for the trend views.
- [ ] README with the architecture diagram and the design decisions.
- [ ] A short written note on the latency question from section 7, so the answer
      is ready before anyone asks.

---

## 10. Repository layout

Target layout. Files marked *(exists)* are in the repo today; the rest are
added by the phase that needs them.

```
quito-air-conditions/
  producers/
    config.py              (exists) endpoints, coordinates, topic names, schema version
    settings.py            (exists) env-driven runtime settings, Kafka config
    http.py                (exists) session with User-Agent and retry policy
    stations.py            (exists) station reference: GUID, feed name, xlsx header, coordinates
    envelope.py            (exists) message envelope and serialisation
    sinks.py               (exists) FileSink and KafkaSink
    runner.py              (exists) CLI, run loop, exit codes
    remmaq.py              (exists) REMMAQ client
    openmeteo.py           (exists) Open-Meteo client
    remmaq_producer.py     (exists) producer one
    weather_producer.py    (exists) producer two
    openaq.py              (exists) OpenAQ client, discovery only
  pipelines/
    bronze_measurements.py
    bronze_history.py
    silver_measurements.py
    silver_subscribers.py
    gold_station_latest.py
    gold_alerts.py
  notebooks/
    01_exploring.ipynb     (exists) data exploration, findings and Silver rules
  bot/
    telegram_consumer.py
  web/
    (Next.js app)
  infra/
    debezium-connector.json
  scripts/
    discover_stations.py   (exists) OpenAQ station listing
    probe_sources.py       (exists) source freshness probe
  tests/                   (exists) 98 tests, fixtures are real captures
  docs/
    PROJECT_PLAN.md        (exists) this file
    ANALYSIS.md            (exists) sources, discoveries, objectives
    data-contracts.md      (exists) topic envelopes and payload shapes
    STAGE1_HANDOVER.md     (exists) changes, how to verify, how to break the tests
    architecture.md
    decisions.md
  .github/workflows/
    ci.yml                 (exists) format, requirements sync, tests
    producers.yml          (exists) cron every 10 minutes
  data/local/              gitignored: probe output, history files, file-sink output
  requirements.txt         (exists) exported from poetry.lock
  README.md                (exists)
```

---

## 11. Open decisions

- Kafka: Confluent Cloud free tier or Redpanda Serverless. Confluent is the
  known path, Redpanda has a simpler free tier. The producer's `KafkaSink`
  works with either (SASL_SSL / PLAIN).
- Producer scheduler: GitHub Actions cron is the version-one choice; Lambda +
  EventBridge if Actions' cron jitter becomes a problem.
- Which air quality index to show. The feed provides the IQCA (Quito's own
  index) with health text, so that is the default for "now". For trends and
  alerts the full IQCA breakpoint table is needed; showing the WHO 2021
  guideline alongside is more honest, since the notebook shows 30 % of
  station-days exceed WHO while almost none exceed the national limit.
- Whether to name the nearest station by neighborhood rather than by station
  name, which is friendlier but introduces a mapping to maintain.
- Domain name, or just use the free Vercel subdomain for version one.

---

## 12. Alternatives, if this stalls

- **Earthquake and volcano monitor for Ecuador.** United States Geological Survey
  worldwide feed, updated every minute. Simpler, genuinely real time, visually
  strong. Weaker on daily usefulness.
- **Flight traffic over Mariscal Sucre airport.** OpenSky Network, positions every
  few seconds, so genuinely high volume. Technically the most honest streaming
  case, least useful to actual people.
