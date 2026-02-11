"""League and stat_category queries."""


def get_league(db, league_key: str):
    """Get full league metadata by league_key."""
    row = db.fetchone("SELECT * FROM league WHERE league_key=?", (league_key,))
    return dict(row) if row else None


def get_latest_league(db):
    """Get the most recently synced league (by season)."""
    row = db.fetchone("SELECT * FROM league ORDER BY season DESC LIMIT 1")
    return dict(row) if row else None


def get_league_week_info(db, league_key: str):
    """Get current_week, end_week, is_finished for a league."""
    return db.fetchone(
        "SELECT current_week, end_week, is_finished FROM league WHERE league_key=?",
        (league_key,),
    )


def get_all_seasons(db):
    """Get all synced seasons ordered by most recent first."""
    rows = db.fetchall(
        "SELECT league_key, season, name, is_finished FROM league ORDER BY season DESC"
    )
    return [dict(r) for r in rows]


def get_scoring_categories(db, league_key: str):
    """Get scoring stat categories for a league."""
    rows = db.fetchall(
        "SELECT stat_id, name, display_name, sort_order, position_type "
        "FROM stat_category "
        "WHERE league_key=? AND is_scoring_stat=1",
        (league_key,),
    )
    return [dict(r) for r in rows]


def get_all_leagues_with_end_week(db):
    """Get all leagues with season and end_week (for standings computation)."""
    return db.fetchall("SELECT league_key, season, end_week FROM league")


def get_distinct_scoring_categories(db):
    """Get unique scoring categories across all seasons (for records)."""
    rows = db.fetchall(
        "SELECT DISTINCT sc.stat_id, sc.display_name, sc.sort_order "
        "FROM stat_category sc "
        "JOIN league l ON sc.league_key = l.league_key "
        "WHERE sc.is_scoring_stat = 1 "
        "ORDER BY sc.display_name"
    )
    # Deduplicate by display_name (same stat across seasons)
    seen = set()
    unique = []
    for r in rows:
        dn = r["display_name"]
        if dn not in seen:
            seen.add(dn)
            unique.append(dict(r))
    return unique
