# Tzeva Adom Alert Scraper

Scrapes [tzevaadom.co.il/en/historical/](https://www.tzevaadom.co.il/en/historical/) and builds a SQLite database of Red Alert events with `timestamp`, `city`, and `alert_type`.

## Setup

```bash
pip install -r requirements.txt
```

## Usage

**Full scrape** (fetches every alert detail page for precise timestamps and threat types):
```bash
python scraper.py
```

**Fast mode** (list page only — uses region names as `alert_type`, approximate times):
```bash
python scraper.py --no-detail
```

**Custom database path:**
```bash
python scraper.py --db my_alerts.db
```

**Test with a small batch:**
```bash
python scraper.py --max-alerts 10
```

**Options:**
| Flag | Default | Description |
|---|---|---|
| `--db PATH` | `alerts.db` | SQLite output path |
| `--no-detail` | off | Skip detail pages (faster, less precise) |
| `--delay SECONDS` | `1.0` | Pause between HTTP requests |
| `--max-alerts N` | unlimited | Cap number of alert pages fetched |

## Database Schema

```sql
CREATE TABLE events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    alert_id   INTEGER  NOT NULL,   -- numeric ID from the site
    timestamp  TEXT     NOT NULL,   -- "YYYY-MM-DD HH:MM:SS"
    city       TEXT     NOT NULL,   -- city/settlement name
    alert_type TEXT     NOT NULL,   -- e.g. "Hostile aircraft intrusion"
    UNIQUE(alert_id, timestamp, city)
);
```

## Example queries

```sql
-- Count alerts per city
SELECT city, COUNT(*) AS alerts
FROM events
GROUP BY city
ORDER BY alerts DESC
LIMIT 20;

-- All events today
SELECT timestamp, city, alert_type
FROM events
WHERE timestamp >= date('now')
ORDER BY timestamp;

-- Events by threat type
SELECT alert_type, COUNT(*) AS total
FROM events
GROUP BY alert_type
ORDER BY total DESC;

-- Events for a specific city
SELECT timestamp, alert_type
FROM events
WHERE city = 'Kiryat Shmona'
ORDER BY timestamp DESC;
```

## Notes

- The scraper respects a 1-second delay between requests by default to avoid overloading the server.
- Data is inserted with `INSERT OR IGNORE` so re-running the scraper only adds new events.
- The site shows roughly the last 7 days on the historical page; run the scraper periodically to build a longer history.
