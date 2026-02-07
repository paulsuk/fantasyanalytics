"""SQLite database wrapper for franchise data."""

import sqlite3
from pathlib import Path

_PROJECT_DIR = Path(__file__).resolve().parent.parent
_SCHEMA_FILE = Path(__file__).resolve().parent / "schema.sql"
_DATA_DIR = _PROJECT_DIR / "data"


class Database:
    """SQLite database for a single franchise.

    Usage:
        db = Database("baseball")
        db.initialize()
        db.execute("INSERT INTO league ...")
        rows = db.fetchall("SELECT * FROM league")
        db.close()

    Or as a context manager:
        with Database("baseball") as db:
            db.fetchall("SELECT * FROM league")
    """

    def __init__(self, franchise_slug: str):
        self.slug = franchise_slug
        _DATA_DIR.mkdir(exist_ok=True)
        self.path = _DATA_DIR / f"{franchise_slug}.db"
        self._conn: sqlite3.Connection | None = None

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(str(self.path))
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
        return self._conn

    def initialize(self):
        """Create all tables from schema.sql if they don't exist."""
        schema = _SCHEMA_FILE.read_text()
        self.conn.executescript(schema)

    def execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        cursor = self.conn.execute(sql, params)
        self.conn.commit()
        return cursor

    def executemany(self, sql: str, params_list: list[tuple]) -> sqlite3.Cursor:
        cursor = self.conn.executemany(sql, params_list)
        self.conn.commit()
        return cursor

    def fetchone(self, sql: str, params: tuple = ()) -> sqlite3.Row | None:
        return self.conn.execute(sql, params).fetchone()

    def fetchall(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        return self.conn.execute(sql, params).fetchall()

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        self.initialize()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
