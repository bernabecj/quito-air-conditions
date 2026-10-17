"""
Probe every candidate live source of Quito air quality data and report freshness.

Phase 1, step 0. OpenAQ's Quito stations last reported in 2021 (see
data/local/quito_sensors.json), so before writing a producer we need evidence of
which source, if any, publishes current readings. This script makes one request
per candidate, records status, content type, the freshest timestamp it can find
and a payload sample, and writes a report you can share.

    python -m scripts.probe_sources
    python -m scripts.probe_sources --delay 5 --only openaq,openmeteo
    python -m scripts.probe_sources --only secretaria --timeout 60

Politeness, because an IP has been blocked before:
- one request per URL, no retries, a pause between requests (default 3 s),
- a descriptive User-Agent so a site owner can see who is asking,
- the OpenAQ key is only ever sent to api.openaq.org,
- HTML pages are fetched once and their links harvested, never crawled.

Optional keys, read from .env: OPENAQ_API_KEY, PURPLEAIR_API_KEY, WAQI_TOKEN.
Probes that need a missing key are skipped, not failed. WAQI is a cross check
only: its terms forbid storing the data, so it will never be a source.

Output: data/local/probe/<UTC timestamp>/ with report.md, report.json and one
raw file per probe (truncated to --max-bytes).
"""

import argparse
import json
import math
import os
import re
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin

import requests
from dotenv import load_dotenv

from producers.config import (
    OPEN_METEO_CURRENT_FIELDS,
    OPEN_METEO_URL,
    OPENAQ_API_KEY_ENV,
    OPENAQ_BASE_URL,
    QUITO_LAT,
    QUITO_LON,
    QUITO_RADIUS_M,
)

load_dotenv()

USER_AGENT = "AirQuito-probe/0.1 (+https://github.com/bernabecj/quito-air-conditions)"
DEFAULT_TIMEOUT = 20
CATALOG_PATH = Path("data/local/quito_sensors.json")
OUTPUT_ROOT = Path("data/local/probe")

# Fallback if the catalog file is missing: location ids from the 2026-10 discovery run.
FALLBACK_LOCATIONS = {
    42641: "Carapungo",
    42642: "Centro",
    42643: "Cotocollao",
    42644: "El Camal",
    42645: "Guamani",
    42646: "Los Chillos",
    42647: "San Antonio",
    42648: "Tumbaco",
    2834723: "Airgradient, Quito(E)",
    2838228: "AirGradient, Quito",
}

OPEN_METEO_AQ_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
OPEN_METEO_AQ_FIELDS = [
    "pm10",
    "pm2_5",
    "carbon_monoxide",
    "nitrogen_dioxide",
    "sulphur_dioxide",
    "ozone",
    "us_aqi",
]

# Secretaría de Ambiente and the municipal open data portals. These URLs are
# from memory and may 404 or redirect; the point is to land on any page of the
# site and harvest the links that mention air monitoring, so the real path is
# visible in the report without crawling.
# Probe runs of 2026-10-16: quitoambiente.gob.ec and datosabiertos.quito.gob.ec
# no longer resolve. The Secretaría's WordPress site, ambiente.quito.gob.ec,
# links through image banners to two applications, which are the real targets:
# aireambiente (live IQCA map, presumably) and datosambiente (historical data).
SECRETARIA_URLS = [
    "https://aireambiente.quito.gob.ec/",
    "http://datosambiente.quito.gob.ec/",
]
# Read from the map page's JavaScript (probe of 2026-10-16, no request made):
# the markers are loaded by an ASP.NET AJAX page method, a POST with an empty
# JSON body that returns {"d": [site, ...]} with SiteName, SourceSiteID,
# Latitude, Longitude, AqiValue, AqiCategory, AqiTime and LayerInfos
# [{ParameterName, Concentration, UnitName, IsValid, Message}]. This is the
# candidate live feed for producer one.
SECRETARIA_SITES_INFO_URL = (
    "https://aireambiente.quito.gob.ec/Modules/Maps/GoogleMap.aspx/GetSitesInfo"
)
LINK_KEYWORDS = re.compile(
    r"aire|monitoreo|remmaq|iqca|calidad|contamina|datos|api|csv|xls|json|descarg", re.I
)

ISO_TS = re.compile(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(?::\d{2})?(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?")
DMY_TS = re.compile(r"\b\d{1,2}/\d{1,2}/\d{4}(?:[ ,]+\d{1,2}:\d{2})?")


@dataclass
class Probe:
    group: str
    name: str
    url: str
    status: int | None = None
    content_type: str | None = None
    elapsed_s: float | None = None
    bytes: int | None = None
    freshest: str | None = None
    error: str | None = None
    skipped: str | None = None
    notes: list[str] = field(default_factory=list)
    raw_file: str | None = None


class LinkHarvester(HTMLParser):
    def __init__(self, base_url: str):
        super().__init__()
        self.base_url = base_url
        self.links: list[tuple[str, str]] = []
        self._href: str | None = None
        self._text: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            href = dict(attrs).get("href")
            if href:
                self._href = urljoin(self.base_url, href)
                self._text = []

    def handle_data(self, data):
        if self._href is not None:
            self._text.append(data.strip())

    def handle_endtag(self, tag):
        if tag == "a" and self._href is not None:
            self.links.append((self._href, " ".join(t for t in self._text if t)))
            self._href = None


def haversine_km(lat1, lon1, lat2, lon2) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def freshest_timestamp(text: str) -> str | None:
    """Max timestamp found by regex. Crude on purpose: the raw file has the truth."""
    found = ISO_TS.findall(text)
    if found:
        return max(found)
    found = DMY_TS.findall(text)
    if found:
        # dd/mm/yyyy does not sort lexically; show the values and let a human judge.
        return f"dd/mm/yyyy dates seen, e.g. {sorted(set(found))[-3:]}"
    return None


def load_catalog() -> tuple[dict[int, str], dict[int, tuple[float, float]]]:
    """Location id -> name, and location id -> (lat, lon), from the discovery output."""
    if not CATALOG_PATH.exists():
        print(f"{CATALOG_PATH} not found, using built-in location ids")
        return FALLBACK_LOCATIONS, {}
    names, coords = {}, {}
    for row in json.loads(CATALOG_PATH.read_text(encoding="utf-8")):
        names[row["location_id"]] = row["location_name"]
        if row.get("latitude") is not None:
            coords[row["location_id"]] = (row["latitude"], row["longitude"])
    return names, coords


class Runner:
    def __init__(self, out_dir: Path, delay: float, max_bytes: int, timeout: float):
        self.out_dir = out_dir
        self.delay = delay
        self.max_bytes = max_bytes
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers["User-Agent"] = USER_AGENT
        self.probes: list[Probe] = []
        self._first = True

    def fetch(
        self,
        probe: Probe,
        headers: dict | None = None,
        params: dict | None = None,
        json_body: dict | None = None,
    ):
        """One request (GET, or POST when json_body is given), no retries.
        Saves the raw body and fills the probe record."""
        if not self._first:
            time.sleep(self.delay)
        self._first = False
        print(f"  {probe.group:<12}{probe.name:<34}", end="", flush=True)
        try:
            t0 = time.monotonic()
            response = self.session.request(
                "POST" if json_body is not None else "GET",
                probe.url,
                headers=headers,
                params=params,
                json=json_body,
                timeout=self.timeout,
                allow_redirects=True,
            )
            probe.elapsed_s = round(time.monotonic() - t0, 2)
            probe.status = response.status_code
            probe.content_type = response.headers.get("Content-Type")
            probe.bytes = len(response.content)
            if response.url != probe.url and not params:
                probe.notes.append(f"redirected to {response.url}")
            if "charset" not in (probe.content_type or ""):
                response.encoding = response.apparent_encoding  # avoid mojibake on old sites
            text = response.text
            probe.freshest = freshest_timestamp(text[: self.max_bytes])
            ext = "json" if "json" in (probe.content_type or "") else "html"
            raw = self.out_dir / f"{probe.group}_{probe.name}.{ext}"
            raw.write_bytes(response.content[: self.max_bytes])
            probe.raw_file = raw.name
            print(f"{probe.status}  {probe.bytes:>8} B  {probe.elapsed_s:>5}s")
            self.probes.append(probe)
            return response
        except requests.RequestException as exc:
            probe.error = f"{type(exc).__name__}: {exc}"
            print(f"ERROR {probe.error[:80]}")
            self.probes.append(probe)
            return None

    def skip(self, probe: Probe, reason: str):
        probe.skipped = reason
        print(f"  {probe.group:<12}{probe.name:<34}skipped: {reason}")
        self.probes.append(probe)

    def json_or_none(self, response):
        try:
            return response.json()
        except ValueError:
            return None


# ----------------------------------------------------------------------------
# Probe groups
# ----------------------------------------------------------------------------


def probe_openaq(r: Runner, names: dict[int, str]):
    key = os.environ.get(OPENAQ_API_KEY_ENV)
    if not key:
        r.skip(Probe("openaq", "all", OPENAQ_BASE_URL), f"{OPENAQ_API_KEY_ENV} not set")
        return
    headers = {"X-API-Key": key}
    for location_id, name in names.items():
        probe = Probe(
            "openaq", f"latest_{location_id}", f"{OPENAQ_BASE_URL}/locations/{location_id}/latest"
        )
        probe.notes.append(name)
        response = r.fetch(probe, headers=headers)
        if response is None:
            continue
        payload = r.json_or_none(response)
        if payload and "results" in payload:
            stamps = [row.get("datetime", {}).get("utc") for row in payload["results"]]
            stamps = [s for s in stamps if s]
            probe.freshest = max(stamps) if stamps else None
            probe.notes.append(f"{len(payload['results'])} sensors in /latest")
        elif probe.status == 429:
            probe.notes.append("rate limited, stop and rerun later with a larger --delay")
            return


def probe_open_meteo(r: Runner, coords: dict[int, tuple[float, float]]):
    # Weather, city centre: confirms the secondary source works at all.
    probe = Probe("openmeteo", "weather_current", OPEN_METEO_URL)
    response = r.fetch(
        probe,
        params={
            "latitude": QUITO_LAT,
            "longitude": QUITO_LON,
            "current": ",".join(OPEN_METEO_CURRENT_FIELDS),
            "timezone": "UTC",
        },
    )
    if response is not None and (payload := r.json_or_none(response)):
        probe.freshest = (payload.get("current") or {}).get("time")

    # Air quality (Copernicus CAMS model). One call with every station's
    # coordinates: if the values are identical across stations, the grid cell is
    # bigger than the city and the source cannot drive a per-station map.
    points = list(coords.values()) or [(QUITO_LAT, QUITO_LON)]
    probe = Probe("openmeteo", "air_quality_all_stations", OPEN_METEO_AQ_URL)
    response = r.fetch(
        probe,
        params={
            "latitude": ",".join(str(p[0]) for p in points),
            "longitude": ",".join(str(p[1]) for p in points),
            "current": ",".join(OPEN_METEO_AQ_FIELDS),
            "timezone": "UTC",
        },
    )
    if response is not None and (payload := r.json_or_none(response)):
        items = payload if isinstance(payload, list) else [payload]
        currents = [item.get("current") or {} for item in items]
        probe.freshest = max((c.get("time") for c in currents if c.get("time")), default=None)
        pm25 = sorted({c.get("pm2_5") for c in currents})
        probe.notes.append(f"{len(items)} points, distinct pm2_5 values: {pm25}")
        if len(pm25) <= 1 and len(items) > 1:
            probe.notes.append("identical across stations: one grid cell covers the whole city")


def probe_secretaria(r: Runner):
    probe = Probe("secretaria", "get_sites_info", SECRETARIA_SITES_INFO_URL)
    response = r.fetch(
        probe,
        headers={
            "Content-Type": "application/json; charset=utf-8",
            "X-Requested-With": "XMLHttpRequest",
        },
        json_body={},
    )
    if response is not None and (payload := r.json_or_none(response)):
        sites = payload.get("d") if isinstance(payload, dict) else None
        if isinstance(sites, list):
            probe.notes.append(f"{len(sites)} sites")
            stamps = [str(site.get("AqiTime") or "") for site in sites]
            probe.freshest = max(stamps) if any(stamps) else None
            for site in sites:
                layers = site.get("LayerInfos") or []
                valid = [
                    f"{li.get('ParameterName')}={li.get('Concentration')} {li.get('UnitName') or ''}"
                    for li in layers
                    if li.get("IsValid")
                ]
                probe.notes.append(
                    f"{site.get('SiteName')} (id {site.get('SourceSiteID')}, "
                    f"{site.get('Latitude')},{site.get('Longitude')}) IQCA {site.get('AqiValue')} "
                    f"{site.get('AqiCategory')} at {site.get('AqiTime')}: {len(valid)}/{len(layers)} "
                    f"valid layers: {', '.join(valid)}"
                )
        else:
            probe.notes.append(f"unexpected shape, keys: {list(payload)[:10]}")

    for i, url in enumerate(SECRETARIA_URLS):
        probe = Probe("secretaria", f"page_{i}", url)
        response = r.fetch(probe)
        if response is None or probe.status != 200:
            continue
        harvester = LinkHarvester(response.url)
        try:
            harvester.feed(response.text)
        except Exception as exc:  # malformed HTML must not kill the run
            probe.notes.append(f"link harvest failed: {exc}")
            continue
        hits = [
            f"{text[:60]!r} -> {href}"
            for href, text in harvester.links
            if LINK_KEYWORDS.search(href) or LINK_KEYWORDS.search(text)
        ]
        probe.notes.append(f"{len(harvester.links)} links, {len(hits)} match air/data keywords")
        probe.notes.extend(dict.fromkeys(hits))  # dedupe, keep order
        # A live map is usually fed by XHR calls or an iframe, which never appear
        # as <a> links. List every url-like string that mentions data or a map.
        # Any quoted URL-like string anywhere in the page, including inside
        # inline scripts (fetch("..."), $.ajax({url: "..."})), filtered by keyword.
        candidates = re.findall(
            r"""['"]((?:https?:)?//[^'"\s<>]+|/[^'"\s<>]{3,})['"]""", response.text
        )
        embedded = sorted(
            set(
                m
                for m in candidates
                if re.search(
                    r"json|csv|xls|api|php|aspx?|ashx|mapa|map|datos|aire|iqca|ajax|estacion"
                    r"|station|arcgis|rest/services|wms|wfs|feature|layer|socket|graphql|export",
                    m,
                    re.I,
                )
                and not re.search(r"\.(css|js|png|jpg|jpeg|gif|svg|woff2?|ico)(\?|$)", m, re.I)
            )
        )
        if embedded:
            probe.notes.append(f"{len(embedded)} embedded data/map urls:")
            probe.notes.extend(f"    {urljoin(response.url, m)}" for m in embedded[:40])
        frames = re.findall(r"<iframe[^>]+src\s*=\s*['\"]([^'\"]+)", response.text, re.I)
        if frames:
            probe.notes.append(f"{len(frames)} iframes:")
            probe.notes.extend(f"    {urljoin(response.url, f)}" for f in frames[:20])
        scripts = re.findall(r"<script[^>]+src\s*=\s*['\"]([^'\"]+)", response.text, re.I)
        app_scripts = [
            u
            for u in scripts
            if not re.search(r"jquery|bootstrap|wp-includes|gtag|analytics", u, re.I)
        ]
        if app_scripts:
            probe.notes.append(
                f"{len(app_scripts)} application scripts (data endpoints may live inside):"
            )
            probe.notes.extend(f"    {urljoin(response.url, u)}" for u in app_scripts[:20])


def probe_airgradient(r: Runner):
    probe = Probe(
        "airgradient",
        "world_current",
        "https://api.airgradient.com/public/api/v1/world/locations/measures/current",
    )
    response = r.fetch(probe)
    if response is None or not (payload := r.json_or_none(response)):
        return
    if not isinstance(payload, list):
        probe.notes.append(f"unexpected shape: {type(payload).__name__}")
        return
    near = []
    for item in payload:
        lat, lon = item.get("latitude"), item.get("longitude")
        if lat is None or lon is None:
            continue
        if haversine_km(QUITO_LAT, QUITO_LON, lat, lon) <= QUITO_RADIUS_M / 1000:
            near.append(item)
    probe.notes.append(
        f"{len(payload)} sensors worldwide, {len(near)} within {QUITO_RADIUS_M // 1000} km"
    )
    stamps = [str(item.get("timestamp") or item.get("lastSeen") or "") for item in near]
    probe.freshest = max(stamps) if any(stamps) else None
    (r.out_dir / "airgradient_quito_subset.json").write_text(
        json.dumps(near, indent=2, ensure_ascii=False), encoding="utf-8"
    )


def probe_sensor_community(r: Runner):
    probe = Probe(
        "sensorcomm",
        "area_25km",
        f"https://data.sensor.community/airrohr/v1/filter/area={QUITO_LAT},{QUITO_LON},{QUITO_RADIUS_M // 1000}",
    )
    response = r.fetch(probe)
    if response is None or not (payload := r.json_or_none(response)):
        return
    if isinstance(payload, list):
        probe.notes.append(f"{len(payload)} readings in the last 5 minutes")
        stamps = [str(item.get("timestamp") or "") for item in payload]
        probe.freshest = max(stamps) if any(stamps) else None


def probe_purpleair(r: Runner):
    key = os.environ.get("PURPLEAIR_API_KEY")
    probe = Probe("purpleair", "bbox_quito", "https://api.purpleair.com/v1/sensors")
    if not key:
        r.skip(probe, "PURPLEAIR_API_KEY not set")
        return
    half = 0.25  # degrees, roughly 28 km
    response = r.fetch(
        probe,
        headers={"X-API-Key": key},
        params={
            "fields": "name,latitude,longitude,pm2.5,last_seen",
            "nwlat": QUITO_LAT + half,
            "nwlng": QUITO_LON - half,
            "selat": QUITO_LAT - half,
            "selng": QUITO_LON + half,
        },
    )
    if response is not None and (payload := r.json_or_none(response)):
        data = payload.get("data") or []
        probe.notes.append(f"{len(data)} sensors in bounding box")
        fields = payload.get("fields") or []
        if "last_seen" in fields and data:
            idx = fields.index("last_seen")
            newest = max(row[idx] for row in data)
            probe.freshest = datetime.fromtimestamp(newest, tz=timezone.utc).isoformat()


def probe_waqi(r: Runner):
    token = os.environ.get("WAQI_TOKEN")
    probe = Probe("waqi", "feed_quito_crosscheck", "https://api.waqi.info/feed/quito/")
    if not token:
        r.skip(probe, "WAQI_TOKEN not set (cross check only, never a source)")
        return
    response = r.fetch(probe, params={"token": token})
    if response is not None and (payload := r.json_or_none(response)):
        data = payload.get("data") or {}
        probe.freshest = (data.get("time") or {}).get("iso")
        probe.notes.append(f"station: {(data.get('city') or {}).get('name')}")
        attributions = [a.get("name") for a in data.get("attributions") or []]
        probe.notes.append(f"attributions: {attributions}")


GROUPS = {
    "openaq": lambda r, names, coords: probe_openaq(r, names),
    "openmeteo": lambda r, names, coords: probe_open_meteo(r, coords),
    "secretaria": lambda r, names, coords: probe_secretaria(r),
    "airgradient": lambda r, names, coords: probe_airgradient(r),
    "sensorcomm": lambda r, names, coords: probe_sensor_community(r),
    "purpleair": lambda r, names, coords: probe_purpleair(r),
    "waqi": lambda r, names, coords: probe_waqi(r),
}


# ----------------------------------------------------------------------------
# Report
# ----------------------------------------------------------------------------


def write_report(out_dir: Path, probes: list[Probe], started: datetime):
    (out_dir / "report.json").write_text(
        json.dumps([asdict(p) for p in probes], indent=2, ensure_ascii=False), encoding="utf-8"
    )
    lines = [
        "# Source probe",
        "",
        f"Run at {started.isoformat(timespec='seconds')} (UTC). One request per row, no retries.",
        "",
        "| group | probe | status | type | freshest timestamp seen | raw |",
        "|---|---|---|---|---|---|",
    ]
    for p in probes:
        status = p.skipped and "skipped" or p.error and "error" or str(p.status)
        ctype = (p.content_type or "").split(";")[0]
        lines.append(
            f"| {p.group} | {p.name} | {status} | {ctype} | {p.freshest or '-'} | {p.raw_file or '-'} |"
        )
    lines += ["", "## Notes", ""]
    for p in probes:
        detail = p.skipped or p.error
        if not p.notes and not detail:
            continue
        lines.append(f"### {p.group} / {p.name}")
        lines.append(f"`{p.url}`")
        if detail:
            lines.append(f"- {detail}")
        lines.extend(f"- {n}" for n in p.notes)
        lines.append("")
    (out_dir / "report.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--delay", type=float, default=3.0, help="seconds between requests")
    parser.add_argument("--only", help="comma separated groups to run: " + ",".join(GROUPS))
    parser.add_argument("--skip", help="comma separated groups to skip")
    parser.add_argument("--max-bytes", type=int, default=300_000, help="raw file truncation")
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT,
        help="seconds to wait per request; the Secretaría site is slow, try 60",
    )
    args = parser.parse_args()

    selected = list(GROUPS)
    if args.only:
        selected = [g for g in args.only.split(",") if g in GROUPS]
    if args.skip:
        selected = [g for g in selected if g not in args.skip.split(",")]

    started = datetime.now(timezone.utc)
    out_dir = OUTPUT_ROOT / started.strftime("%Y%m%dT%H%M%SZ")
    out_dir.mkdir(parents=True, exist_ok=True)
    names, coords = load_catalog()

    print(f"Probing {', '.join(selected)} with a {args.delay}s pause between requests")
    print(f"Output: {out_dir}\n")
    runner = Runner(out_dir, args.delay, args.max_bytes, args.timeout)
    for group in selected:
        GROUPS[group](runner, names, coords)

    write_report(out_dir, runner.probes, started)
    print(f"\nReport: {out_dir / 'report.md'}")
    print("Share the whole folder; the raw files are what the next step needs.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
