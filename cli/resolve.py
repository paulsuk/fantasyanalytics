"""League resolution and arg parsing helpers."""

from config import get_franchise_by_slug
from db import Database


def refresh_manager_names(db, franchise):
    """Update team.manager_name from current config for all synced data."""
    rows = db.fetchall(
        "SELECT DISTINCT manager_guid FROM team WHERE manager_guid != '' AND (manager_name IS NULL OR manager_name = '')"
    )
    for r in rows:
        name = franchise.manager_name(r["manager_guid"])
        if name:
            db.execute(
                "UPDATE team SET manager_name=? WHERE manager_guid=?",
                (name, r["manager_guid"]),
            )


def resolve_league_key(slug: str, season: int = None) -> tuple:
    """Get db + league_key for a franchise. Returns (Database, league_key).

    If no season specified, uses the latest season that has synced data.
    """
    franchise = get_franchise_by_slug(slug)
    if not franchise:
        print(f"Unknown franchise slug: '{slug}'")
        return None, None

    db = Database(slug)
    refresh_manager_names(db, franchise)

    if season:
        league_key = franchise.league_key_for_season(season)
        if not league_key:
            print(f"No league key for season {season}")
            db.close()
            return None, None
        row = db.fetchone("SELECT league_key FROM league WHERE league_key=?", (league_key,))
        if not row:
            print(f"No synced data for {slug} season {season}. Run: python main.py sync {slug} --season {season}")
            db.close()
            return None, None
        return db, league_key

    # No season specified — find latest synced season
    row = db.fetchone("SELECT league_key FROM league ORDER BY season DESC LIMIT 1")
    if not row:
        print(f"No synced data for {slug}. Run: python main.py sync {slug}")
        db.close()
        return None, None

    return db, row["league_key"]


def parse_season_arg(args: list) -> int | None:
    """Extract --season N from args."""
    if "--season" in args:
        idx = args.index("--season")
        if idx + 1 < len(args):
            return int(args[idx + 1])
    return None
