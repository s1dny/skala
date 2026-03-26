import sqlite3
from pathlib import Path

DB_PATH = Path("skala.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS climbers (
    username   TEXT PRIMARY KEY,
    elo        REAL DEFAULT 1500.0,
    rd         REAL DEFAULT 350.0,
    volatility REAL DEFAULT 0.06,
    matches    INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS routes (
    route_id   TEXT PRIMARY KEY,
    name       TEXT,
    grade      TEXT,
    elo        REAL DEFAULT 1500.0,
    rd         REAL DEFAULT 350.0,
    volatility REAL DEFAULT 0.06,
    matches    INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS ascents (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    climber    TEXT NOT NULL REFERENCES climbers(username),
    route_id   TEXT NOT NULL REFERENCES routes(route_id),
    route_name TEXT,
    grade      TEXT,
    tick_type  TEXT,
    date       TEXT,
    UNIQUE(climber, route_id, date)
);

CREATE TABLE IF NOT EXISTS scrape_progress (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""


def _migrate(conn: sqlite3.Connection):
    """Add columns that may be missing from older databases."""
    for table in ("climbers", "routes"):
        existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        if "rd" not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN rd REAL DEFAULT 350.0")
        if "volatility" not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN volatility REAL DEFAULT 0.06")


def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA)
    _migrate(conn)
    return conn


def upsert_climber(conn: sqlite3.Connection, username: str):
    conn.execute(
        "INSERT OR IGNORE INTO climbers (username) VALUES (?)",
        (username,),
    )


def upsert_route(conn: sqlite3.Connection, route_id: str, name: str | None = None, grade: str | None = None):
    conn.execute(
        """INSERT INTO routes (route_id, name, grade) VALUES (?, ?, ?)
           ON CONFLICT(route_id) DO UPDATE SET
               name = COALESCE(excluded.name, routes.name),
               grade = COALESCE(excluded.grade, routes.grade)""",
        (route_id, name, grade),
    )


def insert_ascent(
    conn: sqlite3.Connection,
    climber: str,
    route_id: str,
    route_name: str | None,
    grade: str | None,
    tick_type: str | None,
    date: str | None,
):
    conn.execute(
        """INSERT OR IGNORE INTO ascents (climber, route_id, route_name, grade, tick_type, date)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (climber, route_id, route_name, grade, tick_type, date),
    )


def get_progress(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute("SELECT value FROM scrape_progress WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else None


def set_progress(conn: sqlite3.Connection, key: str, value: str):
    conn.execute(
        "INSERT OR REPLACE INTO scrape_progress (key, value) VALUES (?, ?)",
        (key, value),
    )
    conn.commit()
