"""Transaction queries."""


def get_add_transactions(db, league_key: str):
    """All free agent/waiver add transactions for a league."""
    return db.fetchall(
        "SELECT tp.player_key, tr.week as add_week, tr.destination_team_key, "
        "       p.full_name, t.name as team_name, t.manager_name "
        "FROM transaction_player tp "
        "JOIN transaction_record tr ON tp.transaction_key = tr.transaction_key "
        "JOIN player p ON tp.player_key = p.player_key "
        "LEFT JOIN team t ON tr.league_key = t.league_key "
        "    AND tp.destination_team_key = t.team_key "
        "WHERE tr.league_key=? AND tp.type='add' "
        "    AND tp.source_type IN ('freeagents', 'waivers')",
        (league_key,),
    )


def get_recent_adds(db, league_key: str, team_key: str, limit: int):
    """Recent player adds for a team."""
    rows = db.fetchall(
        "SELECT p.full_name FROM transaction_player tp "
        "JOIN transaction_record tr ON tp.transaction_key=tr.transaction_key "
        "JOIN player p ON tp.player_key=p.player_key "
        "WHERE tr.league_key=? AND tp.destination_team_key=? AND tp.type='add' "
        "ORDER BY tr.timestamp DESC LIMIT ?",
        (league_key, team_key, limit),
    )
    return [r["full_name"] for r in rows]


def get_recent_drops(db, league_key: str, team_key: str, limit: int):
    """Recent player drops for a team."""
    rows = db.fetchall(
        "SELECT p.full_name FROM transaction_player tp "
        "JOIN transaction_record tr ON tp.transaction_key=tr.transaction_key "
        "JOIN player p ON tp.player_key=p.player_key "
        "WHERE tr.league_key=? AND tp.source_team_key=? AND tp.type='drop' "
        "ORDER BY tr.timestamp DESC LIMIT ?",
        (league_key, team_key, limit),
    )
    return [r["full_name"] for r in rows]


def get_week_transactions(db, league_key: str, week_start: str, week_end: str):
    """All transactions within a date range (for weekly recap)."""
    rows = db.fetchall(
        "SELECT tr.type, tr.timestamp, tr.faab_bid, "
        "       tp.player_key, tp.type as player_type, "
        "       tp.destination_team_key, tp.source_team_key, "
        "       p.full_name, "
        "       dt.name as dest_team_name, dt.manager_name as dest_manager, "
        "       st.name as src_team_name "
        "FROM transaction_record tr "
        "JOIN transaction_player tp ON tr.transaction_key=tp.transaction_key "
        "JOIN player p ON tp.player_key=p.player_key "
        "LEFT JOIN team dt ON tr.league_key=dt.league_key "
        "    AND tp.destination_team_key=dt.team_key "
        "LEFT JOIN team st ON tr.league_key=st.league_key "
        "    AND tp.source_team_key=st.team_key "
        "WHERE tr.league_key=? AND tr.timestamp >= ? AND tr.timestamp <= ? "
        "ORDER BY tr.timestamp",
        (league_key, week_start, week_end + "T23:59:59"),
    )
    return [dict(r) for r in rows]
