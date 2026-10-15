QUITO_LAT = -0.1807
QUITO_LON = -78.4678

# OpenAQ v3. The key goes in the X-API-Key header, never the query string, so
# it cannot leak into a logged URL.
OPENAQ_BASE_URL = "https://api.openaq.org/v3"
OPENAQ_API_KEY_ENV = "OPENAQ_API_KEY"
ECUADOR_COUNTRY_ID = 137
# 25 km is the maximum radius OpenAQ accepts, and it covers the Red Metropolitana
# stations from San Antonio in the north to Guamaní in the south, plus Tumbaco
# and Los Chillos in the valleys.
QUITO_RADIUS_M = 25_000
HTTP_TIMEOUT = 15

# Open-Meteo, free and keyless.
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_CURRENT_FIELDS = [
    "temperature_2m",
    "relative_humidity_2m",
    "wind_speed_10m",
    "wind_direction_10m",
]

# Kafka topics. See docs/PROJECT_PLAN.md section 5 for keys and contents.
TOPIC_MEASUREMENTS_RAW = "aq.measurements.raw"
TOPIC_WEATHER_RAW = "weather.current.raw"
TOPIC_SUBSCRIBERS_CDC = "subscribers.cdc"
TOPIC_ALERTS_OUTBOUND = "alerts.outbound"
TOPIC_ALERTS_DELIVERY = "alerts.delivery"
