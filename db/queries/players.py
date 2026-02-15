"""Player, roster, and stat queries."""


def get_weekly_roster_stats(db, league_key: str, week: int, stat_ids: list[int]):
    """Batch query: all starters and their stats for a week.

    Returns rows with player_key, team_key, selected_position, full_name,
    team_name, manager_name, stat_id, value.
    """
    placeholders = ",".join("?" * len(stat_ids))
    return db.fetchall(
        f"SELECT wr.player_key, wr.team_key, wr.selected_position, "
        f"       p.full_name, t.name as team_name, t.manager_name, "
        f"       pws.stat_id, pws.value "
        f"FROM weekly_roster wr "
        f"JOIN player p ON wr.player_key = p.player_key "
        f"JOIN team t ON wr.league_key = t.league_key AND wr.team_key = t.team_key "
        f"LEFT JOIN player_weekly_stat pws "
        f"    ON wr.league_key = pws.league_key "
        f"    AND wr.week = pws.week "
        f"    AND wr.player_key = pws.player_key "
        f"    AND pws.stat_id IN ({placeholders}) "
        f"WHERE wr.league_key=? AND wr.week=? AND wr.is_starter=1",
        (*stat_ids, league_key, week),
    )


def get_category_leaders(db, league_key: str, week: int, stat_id: int,
                         order: str, limit: int):
    """Top players in a specific category for a week.

    order should be "DESC" or "ASC" based on sort_order.
    """
    return db.fetchall(
        f"SELECT pws.player_key, pws.value, p.full_name, "
        f"       t.name as team_name, t.manager_name "
        f"FROM player_weekly_stat pws "
        f"JOIN player p ON pws.player_key = p.player_key "
        f"JOIN weekly_roster wr ON pws.league_key = wr.league_key "
        f"    AND pws.week = wr.week AND pws.player_key = wr.player_key "
        f"JOIN team t ON wr.league_key = t.league_key AND wr.team_key = t.team_key "
        f"WHERE pws.league_key=? AND pws.week=? AND pws.stat_id=? "
        f"    AND wr.is_starter=1 AND pws.value IS NOT NULL "
        f"ORDER BY CAST(pws.value AS REAL) {order} "
        f"LIMIT ?",
        (league_key, week, stat_id, limit),
    )


def get_player_weekly_stats_sum(db, league_key: str, player_key: str,
                                from_week: int, stat_ids: list[int]):
    """Sum of player stats from a given week onward, grouped by stat_id."""
    placeholders = ",".join("?" * len(stat_ids))
    return db.fetchall(
        f"SELECT stat_id, SUM(CAST(value AS REAL)) as total "
        f"FROM player_weekly_stat "
        f"WHERE league_key=? AND player_key=? AND week>=? AND stat_id IN ({placeholders}) "
        f"GROUP BY stat_id",
        (league_key, player_key, from_week, *stat_ids),
    )


def get_max_roster_week(db, league_key: str, team_key: str) -> int | None:
    """Get the maximum week with roster data for a team in a league."""
    row = db.fetchone(
        "SELECT MAX(week) as max_week FROM weekly_roster "
        "WHERE league_key=? AND team_key=?",
        (league_key, team_key),
    )
    return row["max_week"] if row else None


def get_end_of_season_roster(db, league_key: str, team_key: str, week: int):
    """Players on a team's roster in the specified week."""
    return db.fetchall(
        "SELECT wr.player_key, p.full_name, p.primary_position, "
        "       wr.selected_position, wr.is_starter "
        "FROM weekly_roster wr "
        "JOIN player p ON wr.player_key = p.player_key "
        "WHERE wr.league_key=? AND wr.team_key=? AND wr.week=? "
        "ORDER BY wr.is_starter DESC, wr.selected_position",
        (league_key, team_key, week),
    )
