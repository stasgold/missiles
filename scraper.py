"""
Tzeva Adom historical alert scraper.

Data sources (all public static files — no scraping needed):
  https://www.tzevaadom.co.il/static/historical/all.json
      Array of sub-alert rows: [alert_id, threat_type_id, [heb_cities], unix_ts]
  https://www.tzevaadom.co.il/static/cities.json?v=<ver>
      City metadata: Hebrew name → {en, he, area, …}
  https://api.tzevaadom.co.il/lists-versions
      Returns {"cities": N, "polygons": N} — used to build versioned URLs.

Stores one row per (alert_id, timestamp, city) in alerts.db.
"""

import sqlite3
import logging
import argparse
from datetime import datetime, timezone

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = "https://www.tzevaadom.co.il"
API_BASE = "https://api.tzevaadom.co.il"
ALL_JSON_URL = f"{BASE_URL}/static/historical/all.json"
LISTS_VERSIONS_URL = f"{API_BASE}/lists-versions"
DEFAULT_DB = "/data/alerts.db"

# Threat type ID → English label (discovered by rendering the detail pages)
THREAT_TYPES: dict[int, str] = {
    0: "Red Alert",
    2: "Fear of Terrorists infiltration",
    3: "Earthquake",
    5: "Hostile aircraft intrusion",
    8: "Alert",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def init_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_id   INTEGER  NOT NULL,
            timestamp  TEXT     NOT NULL,
            city       TEXT     NOT NULL,
            alert_type TEXT     NOT NULL,
            UNIQUE(alert_id, timestamp, city)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ts   ON events (timestamp)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_city ON events (city)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_type ON events (alert_type)")
    conn.commit()
    return conn


def get_last_unix_ts(conn: sqlite3.Connection) -> int | None:
    """Return the unix timestamp of the most recent event in the DB, or None."""
    row = conn.execute("SELECT MAX(timestamp) FROM events").fetchone()
    if not row or row[0] is None:
        return None
    dt = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def bulk_insert(conn: sqlite3.Connection, rows: list[tuple]) -> int:
    """
    Insert (alert_id, timestamp, city, alert_type) tuples, skip duplicates.
    Returns number of new rows inserted.
    """
    before = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    conn.executemany(
        "INSERT OR IGNORE INTO events (alert_id, timestamp, city, alert_type) "
        "VALUES (?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    after = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    return after - before


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _get_json(session: requests.Session, url: str) -> object:
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "alerts-scraper/2.0",
        "Accept": "application/json",
    })
    return s


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_city_map(session: requests.Session) -> dict[str, str]:
    """
    Returns {hebrew_city_name: english_city_name}.
    Fetches the current version number first to get the right URL.
    """
    try:
        versions = _get_json(session, LISTS_VERSIONS_URL)
        cities_ver = versions.get("cities", 10)
    except Exception:
        cities_ver = 10  # fallback

    cities_url = f"{BASE_URL}/static/cities.json?v={cities_ver}"
    log.info("Fetching city map from %s", cities_url)
    data = _get_json(session, cities_url)

    city_map: dict[str, str] = {}
    for heb_name, info in data.get("cities", {}).items():
        en = info.get("en") or info.get("he") or heb_name
        city_map[heb_name] = en
    return city_map


def load_all_alerts(session: requests.Session) -> list:
    """
    Returns the raw list from all.json.
    Each element: [alert_id, threat_type_id, [heb_city, …], unix_timestamp]
    """
    log.info("Fetching all historical alerts from %s", ALL_JSON_URL)
    return _get_json(session, ALL_JSON_URL)


# ---------------------------------------------------------------------------
# Processing
# ---------------------------------------------------------------------------

def build_rows(
    all_data: list,
    city_map: dict[str, str],
    limit: int | None = None,
    since_unix_ts: int | None = None,
) -> list[tuple]:
    """
    Convert raw all.json entries into (alert_id, timestamp, city, alert_type)
    tuples ready for INSERT, one tuple per city.
    If since_unix_ts is set, only entries with unix_ts > since_unix_ts are kept.
    """
    rows: list[tuple] = []
    data = all_data if limit is None else all_data[:limit]

    for entry in data:
        if since_unix_ts is not None and entry[3] <= since_unix_ts:
            continue
        alert_id: int = entry[0]
        threat_id: int = entry[1]
        heb_cities: list[str] = entry[2]
        unix_ts: int = entry[3]

        alert_type = THREAT_TYPES.get(threat_id, f"Unknown (type {threat_id})")
        timestamp = datetime.fromtimestamp(unix_ts, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        for heb_city in heb_cities:
            city = city_map.get(heb_city, heb_city)  # fall back to Hebrew if no mapping
            rows.append((alert_id, timestamp, city, alert_type))

    return rows


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def _run_insert(conn: sqlite3.Connection, rows: list[tuple], db_path: str) -> None:
    """Batch-insert rows and log the result."""
    batch_size = 5000
    total_inserted = 0
    for start in range(0, len(rows), batch_size):
        total_inserted += bulk_insert(conn, rows[start : start + batch_size])
    log.info("Done. %d new rows inserted (total in DB: %d).",
             total_inserted,
             conn.execute("SELECT COUNT(*) FROM events").fetchone()[0])


def fetch_new(db_path: str = DEFAULT_DB) -> None:
    """Incremental update: only fetch and insert events newer than the latest in the DB."""
    session = make_session()
    conn = init_db(db_path)

    last_ts = get_last_unix_ts(conn)
    if last_ts is None:
        log.info("Database is empty — running full scrape instead.")
        conn.close()
        scrape(db_path=db_path)
        return

    since_dt = datetime.fromtimestamp(last_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    log.info("Last event in DB: %s UTC — fetching only newer events.", since_dt)

    city_map = load_city_map(session)
    log.info("Loaded %d city name mappings", len(city_map))

    all_data = load_all_alerts(session)
    log.info("Loaded %d sub-alert rows from all.json", len(all_data))

    rows = build_rows(all_data, city_map, since_unix_ts=last_ts)
    log.info("Built %d new event rows (after %s UTC), inserting into %s …",
             len(rows), since_dt, db_path)

    if not rows:
        log.info("No new events found.")
        conn.close()
        return

    _run_insert(conn, rows, db_path)
    conn.close()


def scrape(db_path: str = DEFAULT_DB, limit: int | None = None) -> None:
    session = make_session()
    conn = init_db(db_path)

    # Step 1: load lookup tables
    city_map = load_city_map(session)
    log.info("Loaded %d city name mappings", len(city_map))

    # Step 2: load all historical alert data (single request)
    all_data = load_all_alerts(session)
    log.info("Loaded %d sub-alert rows from all.json", len(all_data))

    # Step 3: transform and persist
    rows = build_rows(all_data, city_map, limit=limit)
    log.info("Built %d event rows, inserting into %s …", len(rows), db_path)

    _run_insert(conn, rows, db_path)
    conn.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch tzevaadom.co.il alert history into a SQLite database."
    )
    parser.add_argument(
        "--db", default=DEFAULT_DB, metavar="PATH",
        help=f"SQLite output path (default: {DEFAULT_DB})"
    )
    parser.add_argument(
        "--update", action="store_true",
        help="Only fetch events newer than the latest event already in the database"
    )
    parser.add_argument(
        "--limit", type=int, default=None, metavar="N",
        help="Process only the first N rows from all.json (for testing, full scrape only)"
    )
    args = parser.parse_args()
    if args.update:
        fetch_new(db_path=args.db)
    else:
        scrape(db_path=args.db, limit=args.limit)


if __name__ == "__main__":
    main()
