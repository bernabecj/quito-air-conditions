"""
Constants shared by the producers: endpoints, coordinates, topic names.

Runtime settings that change per environment (sink choice, Kafka credentials,
output folders) live in producers/settings.py and come from the environment.
"""

QUITO_LAT = -0.1807
QUITO_LON = -78.4678
# Ecuador mainland is UTC-5 all year, no daylight saving.
QUITO_TZ = "America/Guayaquil"

# Sent on every request so the source can see who is asking. A descriptive
# agent is part of being a polite client of a public service.
USER_AGENT = "AirQuito/0.1 (+https://github.com/bernabecj/quito-air-conditions)"
HTTP_TIMEOUT = 30
# Retry policy for transient failures (connection errors, timeouts, 429, 5xx):
# attempts are spaced HTTP_BACKOFF_S * 2**n seconds plus jitter.
HTTP_MAX_ATTEMPTS = 3
HTTP_BACKOFF_S = 2.0

# Primary source: Red Metropolitana de Monitoreo Atmosferico de Quito (REMMAQ),
# Secretaria de Ambiente, served by the Envista ARM web portal. The map page
# loads its markers through an ASP.NET page method: a POST with an empty JSON
# body returning {"d": [site, ...]} with the latest hourly value of every
# parameter at every station. Found by reading the page's JavaScript
# (scripts/probe_sources.py, 2026-10-16); it is not a documented API, so the
# data contract records it as a fragile dependency.
REMMAQ_SITES_INFO_URL = "https://aireambiente.quito.gob.ec/Modules/Maps/GoogleMap.aspx/GetSitesInfo"
# Historical hourly series, one .rar per parameter, used for backfill and the
# exploration notebook.
REMMAQ_HISTORY_URL = "https://datosambiente.quito.gob.ec/"

# Secondary source: Open-Meteo, free and keyless. Weather explains pollution
# episodes (thermal inversions, calm winds); the CAMS air quality model adds a
# city-level forecast in a later phase.
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_CURRENT_FIELDS = [
    "temperature_2m",
    "relative_humidity_2m",
    "wind_speed_10m",
    "wind_direction_10m",
]
OPEN_METEO_AQ_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"

# OpenAQ v3. Kept for the discovery script and as the historical reference;
# its Quito stations stopped updating in 2021 and it is not a producer source.
OPENAQ_BASE_URL = "https://api.openaq.org/v3"
OPENAQ_API_KEY_ENV = "OPENAQ_API_KEY"
ECUADOR_COUNTRY_ID = 137
QUITO_RADIUS_M = 25_000

# Kafka topics. See docs/data-contracts.md for keys and envelopes.
TOPIC_MEASUREMENTS_RAW = "aq.measurements.raw"
TOPIC_WEATHER_RAW = "weather.current.raw"
TOPIC_SUBSCRIBERS_CDC = "subscribers.cdc"
TOPIC_ALERTS_OUTBOUND = "alerts.outbound"
TOPIC_ALERTS_DELIVERY = "alerts.delivery"

# Envelope schema version, bumped when a field is added, renamed or removed.
SCHEMA_VERSION = 1
