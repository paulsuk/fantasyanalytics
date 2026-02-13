"""Cross-season aggregation queries."""


def get_all_manager_teams(db):
    """All teams with their manager GUIDs and seasons, ordered by season."""
    return db.fetchall(
        "SELECT t.manager_guid, t.manager_name, t.team_key, t.name AS team_name, "
        "       t.finish, t.playoff_seed, "
        "       l.league_key, l.season, l.is_finished "
        "FROM team t JOIN league l ON t.league_key = l.league_key "
        "WHERE t.manager_guid IS NOT NULL "
        "ORDER BY l.season"
    )


def get_all_matchups_with_manager_guids(db):
    """All matchups annotated with manager GUIDs for cross-season analysis."""
    return db.fetchall(
        "SELECT m.league_key, m.week, m.winner_team_key, m.is_tied, "
        "       m.team_key_1, m.team_key_2, m.cats_won_1, m.cats_won_2, m.cats_tied, "
        "       m.is_playoffs, m.is_consolation, "
        "       t1.manager_guid AS guid_1, t2.manager_guid AS guid_2, "
        "       l.season "
        "FROM matchup m "
        "JOIN team t1 ON m.league_key = t1.league_key AND m.team_key_1 = t1.team_key "
        "JOIN team t2 ON m.league_key = t2.league_key AND m.team_key_2 = t2.team_key "
        "JOIN league l ON m.league_key = l.league_key"
    )


def get_category_record_holder(db, display_name: str, order: str):
    """Best single-week value for a category across all seasons.

    order should be "DESC" (higher is better) or "ASC" (lower is better).
    """
    return db.fetchone(
        f"SELECT tws.value, tws.week, t.manager_name, t.name AS team_name, "
        f"       l.season, sc.display_name "
        f"FROM team_weekly_score tws "
        f"JOIN team t ON tws.league_key = t.league_key AND tws.team_key = t.team_key "
        f"JOIN league l ON tws.league_key = l.league_key "
        f"JOIN stat_category sc ON tws.league_key = sc.league_key "
        f"    AND tws.stat_id = sc.stat_id "
        f"WHERE sc.display_name = ? AND sc.is_scoring_stat = 1 "
        f"ORDER BY tws.value {order} LIMIT 1",
        (display_name,),
    )


def get_all_regular_season_matchups_with_managers(db, include_playoffs: bool = False):
    """All matchups with manager info (for streak computation).

    By default only regular season. Set include_playoffs=True to include all.
    """
    where = "" if include_playoffs else "WHERE m.is_playoffs = 0 AND m.is_consolation = 0 "
    return db.fetchall(
        "SELECT m.team_key_1, m.team_key_2, m.winner_team_key, m.is_tied, "
        "       t1.manager_guid AS guid_1, t1.manager_name AS name_1, t1.name AS team_name_1, "
        "       t2.manager_guid AS guid_2, t2.manager_name AS name_2, t2.name AS team_name_2, "
        "       l.season, m.week "
        "FROM matchup m "
        "JOIN team t1 ON m.league_key = t1.league_key AND m.team_key_1 = t1.team_key "
        "JOIN team t2 ON m.league_key = t2.league_key AND m.team_key_2 = t2.team_key "
        "JOIN league l ON m.league_key = l.league_key "
        f"{where}"
        "ORDER BY l.season, m.week"
    )


def get_all_regular_season_matchup_scores(db, include_playoffs: bool = False):
    """All matchup scores with team info (for blowout/closest records).

    By default only regular season. Set include_playoffs=True to include all.
    """
    where = "" if include_playoffs else "WHERE m.is_playoffs = 0 AND m.is_consolation = 0"
    return db.fetchall(
        "SELECT m.cats_won_1, m.cats_won_2, m.cats_tied, m.is_tied, "
        "       t1.manager_name AS manager_1, t1.name AS team_name_1, "
        "       t2.manager_name AS manager_2, t2.name AS team_name_2, "
        "       l.season, m.week "
        "FROM matchup m "
        "JOIN team t1 ON m.league_key = t1.league_key AND m.team_key_1 = t1.team_key "
        "JOIN team t2 ON m.league_key = t2.league_key AND m.team_key_2 = t2.team_key "
        "JOIN league l ON m.league_key = l.league_key "
        f"{where}"
    )
