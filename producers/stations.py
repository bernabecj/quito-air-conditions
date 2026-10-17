"""
Reference table of the REMMAQ stations, captured from the live feed on
2026-10-16 (data/local/probe/20261016T234146Z/secretaria_get_sites_info.json).

Three identifiers exist for each station and they must be joinable:
- `station_id`: the GUID the live feed uses (SourceSiteID). Message key.
- `name`: the display name in the feed (SiteName), sometimes without spaces.
- `history_column`: the column header in the historical xlsx exports.

El Camal reported no valid layer in the capture; it stays in the table because
a station that is offline is still a station, and the site must show it as
unavailable rather than forget it. Belisario is the station OpenAQ never had.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Station:
    station_id: str
    name: str
    history_column: str
    latitude: float
    longitude: float


STATIONS: tuple[Station, ...] = (
    Station(
        "f1f5d7e6-09f3-4966-ad15-f6ad47b89973", "Belisario", "BELISARIO", -0.184719, -78.495986
    ),
    Station(
        "35e166f8-0d20-4955-84ec-1180d8193d03", "Carapungo", "CARAPUNGO", -0.095472, -78.449809
    ),
    Station("1f090827-f3b4-4c46-9794-af4e2d46c126", "Centro", "CENTRO", -0.221393, -78.514005),
    Station(
        "da0e2ba0-92ab-4703-80b3-825144c34659", "Cotocollao", "COTOCOLLAO", -0.107777, -78.497222
    ),
    Station("f1634ea4-d0d0-4b18-b00a-e25e7c06c8be", "El Camal", "EL CAMAL", -0.25, -78.51),
    Station("dbf143fe-e4dc-4875-bdec-70cca4dde273", "Guamani", "GUAMANI", -0.333949, -78.553416),
    Station(
        "4cf14787-3ed1-4194-97b6-7f038d76f6d0", "LosChillos", "LOS CHILLOS", -0.297062, -78.455248
    ),
    Station(
        "96c776fd-b679-40e6-ae95-d657e05e3a84", "SanAntonio", "SAN ANTONIO", -0.009222, -78.448001
    ),
    Station("54c686c1-ad66-4c69-b6dc-54dc6ac9eecd", "Tumbaco", "TUMBACO", -0.214933, -78.403253),
)

STATIONS_BY_ID = {station.station_id: station for station in STATIONS}

# Layer names as the feed spells them, with the unit the feed reports.
# Pollutants first, then meteorology. CO is the only pollutant in mg/m3.
LAYERS = {
    "CO_mg": "mg/m3",
    "NO2_ug": "ug/m3",
    "OZONO_ug": "ug/m3",
    "SO2_ug": "ug/m3",
    "PM2.5_ug": "ug/m3",
    "PM10_ug": "ug/m3",
    "TEMP_AMB": "°C",
    "RAD_SOLAR": "W/m2",
    "LLUVIA": "mm",
    "RAP_VEC": "m/s",
    "DIR_VEC": "DEG",
}
POLLUTANT_LAYERS = ("CO_mg", "NO2_ug", "OZONO_ug", "SO2_ug", "PM2.5_ug", "PM10_ug")
