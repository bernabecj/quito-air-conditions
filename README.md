# AirQuito

Live air quality and alert service for Quito. A continuously running pipeline reads the city's public air quality measurements, stores them in a Delta Lake lakehouse, publishes a free public website, and sends Telegram alerts to people who subscribe to their neighborhood.

![CI](https://github.com/bernabecj/quito-air-conditions/actions/workflows/ci.yml/badge.svg)

> **Status: Phase 1, Stage 1 done.** Two producers (REMMAQ measurements, Open-Meteo weather) with file and Kafka sinks, a GitHub Actions cron to run them every 10 minutes, 98 tests, and an exploration notebook over 22 years of Quito's air quality history. No Delta tables or site yet. See [docs/PROJECT_PLAN.md](docs/PROJECT_PLAN.md) for the phase checklist, [docs/ANALYSIS.md](docs/ANALYSIS.md) for what was learned about the data, and [docs/STAGE1_HANDOVER.md](docs/STAGE1_HANDOVER.md) for how to verify everything.

---

## What it will do

**Public, no login**
- Map of Quito with current air quality per monitoring station.
- A plain answer to "is it safe to exercise outside right now in my neighborhood".
- 24 hour and 7 day trends per station.
- A short explanation of why, using wind and temperature.

**For subscribers**
- Pick a neighborhood (nearest station) and a threshold.
- Get one Telegram message when the threshold is crossed, and one when conditions return to normal. Not one per reading.

---

## Architecture

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

The core of the design is a stream to table join: hourly measurements meet the subscriber table, which arrives as a change data capture stream from Postgres, and the job decides who gets notified.

| Layer | Choice |
|---|---|
| Producers | Python, one per source, stateless, at-least-once; GitHub Actions cron every 10 minutes |
| Broker | Confluent Cloud free tier (Redpanda Serverless as the alternative) |
| Operational database | Postgres on Supabase or Neon |
| Change capture | Debezium on the Postgres write ahead log |
| Processing | Databricks Free Edition, Delta Lake, Unity Catalog, triggered micro batch |
| Serving store | Postgres, written back from gold |
| Frontend | Next.js on Vercel or Cloudflare Pages |
| Alerts | Telegram bot |

### Why Kafka in front of hourly data?

Reference stations publish once per hour, about 70 minutes after the hour, so this is not sub second streaming and does not claim to be. The broker is there because two sources fan in and two consumers fan out, and because replay matters. Producers are short-lived processes that poll every 10 minutes and exit; processing runs as a triggered micro batch rather than a continuous stream, since continuous compute is not justified by an hourly source. [Section 7 of the plan](docs/PROJECT_PLAN.md#7-known-weakness-and-how-to-answer-it) covers this in full.

---

## Data sources

| Source | What it provides | Access |
|---|---|---|
| **REMMAQ** via the Secretaría de Ambiente portal (`aireambiente.quito.gob.ec`) | Hourly PM2.5, PM10, O₃, NO₂, SO₂, CO, temperature, solar radiation, rain, wind, and the IQCA index with health advisories, for nine stations | One unauthenticated POST to the map's page method. Not a documented API; see [the data contract](docs/data-contracts.md) |
| **REMMAQ history** (`datosambiente.quito.gob.ec`) | Validated hourly series since 2004, one xlsx per parameter | Manual download, loaded by a backfill job |
| [Open-Meteo](https://open-meteo.com/) | Temperature, humidity, wind speed and direction at each station's coordinates | Free, no key |

**Not used: OpenAQ.** The plan started there, but its Quito stations stopped updating in November 2021. The client stays in `producers/openaq.py` as the record of that discovery. **Not used: aqicn.org.** Its terms forbid caching and redistributing the data, and this project exists to store history; it served once as a manual cross check that REMMAQ was still publishing.

---

## Getting started

Requires Python 3.11 or 3.12 and Java 17 or 21 (only for the exploration notebook).

**With Poetry** (the source of truth for dependencies):

```bash
poetry install --with dev               # producers, tests, formatting; creates .venv/
poetry install --with dev,notebook      # plus PySpark and Jupyter for notebooks/
cp .env.example .env                    # then fill in the keys you have
```

**With a plain venv** (sandboxes, Databricks, anywhere without Poetry). `requirements.txt`
is exported from `poetry.lock` and CI fails if the two drift:

```bash
python -m venv .venv && source .venv/bin/activate     # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

After changing dependencies: `poetry lock`, then
`poetry export --with dev,notebook --without-hashes -o requirements.txt`
(needs `poetry self add poetry-plugin-export` once), keeping the comment header.

### Run a producer

Each producer polls its source once, publishes, prints a JSON summary and exits. With
`SINK=file` (the default) records go to `data/local/sink/<topic>/<run>.jsonl`, which
`spark.read.json` reads directly; with `SINK=kafka` they go to the cluster in `.env`.

```bash
python -m producers.remmaq_producer                    # REMMAQ → aq.measurements.raw
python -m producers.weather_producer                   # Open-Meteo → weather.current.raw
python -m producers.remmaq_producer --sink file --out-dir /tmp/out
python -m producers.remmaq_producer --loop 600         # keep polling every 10 minutes
```

Exit code 1 means the source was unreachable after three attempts, answered in an
unexpected shape, or the sink could not confirm delivery. In production the same two
commands run from `.github/workflows/producers.yml` every 10 minutes; trigger it by hand
with `sink=file` to get the records as a downloadable artifact before a Kafka cluster exists.

### Explore the data

`notebooks/01_exploring.ipynb` runs on Databricks (data in a Unity Catalog volume) or
locally with PySpark. It needs the history xlsx files and a probe capture under
`data/local/`; see [docs/STAGE1_HANDOVER.md](docs/STAGE1_HANDOVER.md) for both paths.

```bash
cd notebooks && AIRQUITO_DATA_ROOT=../data/local jupyter nbconvert --to notebook --execute --inplace 01_exploring.ipynb
```

### Source discovery scripts

`scripts/probe_sources.py` makes one polite request per candidate data source and writes
a freshness report to `data/local/probe/`. This is how the live REMMAQ feed was found
after OpenAQ turned out to be stale (see [docs/ANALYSIS.md](docs/ANALYSIS.md)).

```bash
python -m scripts.probe_sources                       # all sources, 3 s between requests
python -m scripts.probe_sources --only secretaria --timeout 60
```

`scripts/discover_stations.py` lists the OpenAQ stations around Quito (needs `OPENAQ_API_KEY`).

### Format and test

```bash
black . && isort .
pytest                                                 # 98 tests, no network, < 1 s
```

CI runs the formatting check, the `requirements.txt` sync check and pytest on every push
and pull request.

---

## Repository layout

```
producers/        clients, envelope, sinks, runner and the two producers
notebooks/        01_exploring.ipynb, the data exploration record
scripts/          probe_sources.py (source freshness), discover_stations.py (OpenAQ)
tests/            98 tests; fixtures are real captures from the sources
docs/             PROJECT_PLAN, ANALYSIS, data-contracts, STAGE1_HANDOVER
.github/workflows ci.yml (checks) and producers.yml (cron every 10 minutes)
data/local/       gitignored: probe output, history files, file-sink output
```

`pipelines/`, `bot/`, `web/` and `infra/` arrive with the phases that need them. The target layout is in [the plan](docs/PROJECT_PLAN.md#10-repository-layout).

---

## Data attribution

Air quality data from the Red Metropolitana de Monitoreo Atmosférico (Secretaría de Ambiente, Municipio del Distrito Metropolitano de Quito), [ambiente.quito.gob.ec](https://ambiente.quito.gob.ec/). Weather data from [Open-Meteo](https://open-meteo.com/).

## License

[MIT](LICENSE)
