# AirQuito

Live air quality and alert service for Quito. A continuously running pipeline reads the city's public air quality measurements, stores them in a Delta Lake lakehouse, publishes a free public website, and sends Telegram alerts to people who subscribe to their neighborhood.

![CI](https://github.com/bernabecj/quito-air-conditions/actions/workflows/ci.yml/badge.svg)

> **Status: early setup.** The repository, tooling, CI and an OpenAQ client are in place. No pipeline runs yet. See [docs/PROJECT_PLAN.md](docs/PROJECT_PLAN.md) for the full plan and the phase checklist.

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

The core of the design is a stream to table join: hourly measurements meet the subscriber table, which arrives as a change data capture stream from Postgres, and the job decides who gets notified.

| Layer | Choice |
|---|---|
| Producers | Python, one per source, on a GitHub Actions cron or a small Fly.io machine |
| Broker | Confluent Cloud free tier (Redpanda Serverless as the alternative) |
| Operational database | Postgres on Supabase or Neon |
| Change capture | Debezium on the Postgres write ahead log |
| Processing | Databricks Free Edition, Delta Lake, Unity Catalog, triggered micro batch |
| Serving store | Postgres, written back from gold |
| Frontend | Next.js on Vercel or Cloudflare Pages |
| Alerts | Telegram bot |

### Why Kafka in front of hourly data?

Reference stations publish about once per hour, so this is not sub second streaming and does not claim to be. The broker is there because two sources fan in and two consumers fan out, and because replay matters. Processing runs as a triggered micro batch every few minutes rather than a continuous stream, since continuous compute is not justified by an hourly source. [Section 7 of the plan](docs/PROJECT_PLAN.md#7-known-weakness-and-how-to-answer-it) covers this in full.

---

## Data sources

| Source | What it provides | Access |
|---|---|---|
| [OpenAQ v3](https://docs.openaq.org/) | PM2.5, PM10, O₃, NO₂, SO₂, CO from Quito's Red Metropolitana de Monitoreo Atmosférico | Free key, `X-API-Key` header |
| [Open-Meteo](https://open-meteo.com/) | Temperature, humidity, wind speed and direction | Free, no key |

OpenAQ returns physical units, not an index. The index is computed in the pipeline.

**Not used: aqicn.org.** Its terms forbid caching and redistributing the data, and this project exists to store history. It is only used as a manual cross check.

---

## Getting started

Requires Python 3.11 and [Poetry](https://python-poetry.org/).

```bash
poetry install --with dev        # creates .venv/ in the project (see poetry.toml)
cp .env.example .env             # then add your OpenAQ key
```

Get a free OpenAQ key at [explore.openaq.org/register](https://explore.openaq.org/register).

List the stations around Quito and the sensors each one carries:

```bash
poetry run python -m scripts.discover_stations
poetry run python -m scripts.discover_stations --json data/local/quito_sensors.json
```

Format and test:

```bash
poetry run black . && poetry run isort .
poetry run pytest
```

CI runs the same formatting check and pytest on every push and pull request.

---

## Repository layout

```
producers/        source clients and producers (OpenAQ client today)
scripts/          one-off tools, e.g. station discovery
tests/            unit tests, pytest configured in pyproject.toml
docs/             project plan, later architecture, data contracts and decisions
.github/workflows CI
```

`pipelines/`, `bot/`, `web/` and `infra/` arrive with the phases that need them. The target layout is in [the plan](docs/PROJECT_PLAN.md#10-repository-layout).

---

## Data attribution

Air quality data from the Red Metropolitana de Monitoreo Atmosférico (Secretaría de Ambiente, Municipio del Distrito Metropolitano de Quito) via [OpenAQ](https://openaq.org/). Weather data from [Open-Meteo](https://open-meteo.com/).

## License

[MIT](LICENSE)
