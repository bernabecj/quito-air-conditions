# Project Plan

This is the planning doc for AirQuito: the pitch, the data sources, the target
architecture and the phased build plan. For setup instructions and what exists
today, see the [README](../README.md).

**Status:** Phase 0 (setup) done, Phase 1 not started.
**Created:** 2026-10-15

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

### Primary: OpenAQ
- API version 3, requires a free key, sent in the `X-API-Key` header.
- Base: `https://api.openaq.org/v3`
- Ecuador is country id `137`.
- Covers fine particulate matter (PM2.5), respirable particulate matter (PM10),
  ozone, nitrogen dioxide, sulphur dioxide, carbon monoxide, plus humidity and
  temperature for some stations.
- Values come in physical units, not as an index. The index is computed by us.
- All data served through the interface is public, with near real time and
  historical coverage.
- Underlying network for Quito: Red Metropolitana de Monitoreo Atmosférico,
  managed by the Secretaría de Ambiente of the Municipio del Distrito
  Metropolitano de Quito. Roughly ten stations (Cotocollao, Centro, Belisario,
  Carapungo, El Camal, Guamaní, Los Chillos, Tumbaco, Jipijapa, San Antonio).
  **Not yet verified against the API**: confirming which of these OpenAQ
  actually exposes, and how recently each reported, is Phase 1 step 1
  (`scripts/discover_stations.py`).

Endpoints that matter:
- `/v3/countries/137` to confirm available parameters.
- `/v3/locations?coordinates=-0.1807,-78.4678&radius=25000` to list Quito stations.
- `/v3/sensors/{id}/measurements` for history per sensor.
- `/v3/locations/{id}/latest` for the current values.

### Secondary: weather
- Open-Meteo, free, no key required.
- `https://api.open-meteo.com/v1/forecast?latitude=-0.1807&longitude=-78.4678&current=temperature_2m,relative_humidity_2m,wind_speed_10m,wind_direction_10m`
- Wind and temperature explain most pollution spikes, especially thermal
  inversions. This turns a dashboard into an explanation.

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
OpenAQ API ─────┐
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
| Producers | Python, one per source | Run on GitHub Actions cron or a small Fly.io machine |
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
| `aq.measurements.raw` | sensor id | One record per sensor reading, as returned by the source |
| `weather.current.raw` | station id | Current conditions near each station |
| `subscribers.cdc` | subscriber id | Debezium change events from Postgres |
| `alerts.outbound` | subscriber id | Alerts decided by the pipeline, consumed by the bot |
| `alerts.delivery` | subscriber id | Delivery result, success or failure, with latency |

Topic names live in [`producers/config.py`](../producers/config.py).

### Delta tables

**Bronze**
- `bronze.measurements`: exact payload plus ingestion timestamp and source name.
  Append only, nothing dropped, nothing cast.
- `bronze.weather`: same approach.
- `bronze.subscribers_cdc`: raw change events, including the operation type.

**Silver**
- `silver.measurements`: one row per station, parameter and measurement time.
  Deduplicated on that key. Units normalised. Obvious bad values quarantined
  rather than deleted, for example negative concentrations or values far outside
  the plausible range.
- `silver.stations`: station reference data, slowly changing, type 2.
- `silver.subscribers`: current state of each subscriber, rebuilt from the change
  stream with a merge. Personal fields masked in this layer.
- `silver.measurements_quarantine`: rejected rows with the reason. Never silently
  discard.

**Gold**
- `gold.station_latest`: current value and computed index per station.
- `gold.station_hourly`: hourly averages, the table the charts read.
- `gold.alert_candidates`: crossings detected, before the subscriber join.
- `gold.pipeline_health`: freshness per source, rows processed, quarantine rate,
  alert delivery latency. This is the table that proves the pipeline is alive.

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
  Atmosférico via OpenAQ, with a link.
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
- [ ] Get an OpenAQ key, run `scripts/discover_stations.py`, confirm the Quito
      stations and their sensor ids. Record the result in this doc.
- [ ] Producer one: poll OpenAQ, write to `aq.measurements.raw`.
- [ ] Bronze table, append only.
- [ ] Silver table with deduplication on station, parameter and measurement time.
- [ ] Gold table `station_latest`.
- [ ] Push gold to Postgres.
- [ ] One page site with the map and current values. **Deploy it.**
- [ ] Link the live site from the README.

### Phase 2, about one week
- [ ] Producer two: weather.
- [ ] Hourly aggregates and the trend charts.
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
    config.py              (exists) coordinates, endpoints, topic names
    openaq.py              (exists) OpenAQ v3 client
    openaq_producer.py
    weather_producer.py
  pipelines/
    bronze_measurements.py
    silver_measurements.py
    silver_subscribers.py
    gold_station_latest.py
    gold_alerts.py
  bot/
    telegram_consumer.py
  web/
    (Next.js app)
  infra/
    debezium-connector.json
  scripts/
    discover_stations.py   (exists) Phase 1 step 1
  tests/                   (exists, empty) pytest is configured in pyproject.toml
  docs/
    PROJECT_PLAN.md        (exists) this file
    architecture.md
    data-contracts.md
    decisions.md
  .github/workflows/       (exists) CI
  README.md                (exists)
```

---

## 11. Open decisions

- Kafka: Confluent Cloud free tier or Redpanda Serverless. Confluent is the
  known path, Redpanda has a simpler free tier.
- Which air quality index to use. The United States Environmental Protection
  Agency index is the most recognised, but Ecuador has its own index. Showing
  both, or showing the raw value with a plain language label, may be more honest.
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
