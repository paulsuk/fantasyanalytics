"""Matchup and matchup_category queries."""


def get_matchups_through_week(db, league_key: str, through_week: int):
    """Get all matchups up to and including a given week."""
    return db.fetchall(
        "SELECT team_key_1, team_key_2, cats_won_1, cats_won_2, "
        "       cats_tied, winner_team_key, is_tied "
        "FROM matchup WHERE league_key=? AND week<=?",
        (league_key, through_week),
    )


def get_team_category_results(db, league_key: str, team_key: str, through_week: int):
    """Per-category W-L-T results for a team through a given week."""
    return db.fetchall(
        "SELECT mc.stat_id, sc.display_name, mc.winner_team_key "
        "FROM matchup_category mc "
        "JOIN matchup m ON mc.league_key=m.league_key AND mc.week=m.week "
        "    AND mc.matchup_id=m.matchup_id "
        "JOIN stat_category sc ON mc.league_key=sc.league_key AND mc.stat_id=sc.stat_id "
        "WHERE mc.league_key=? AND mc.week<=? "
        "    AND (m.team_key_1=? OR m.team_key_2=?) "
        "    AND sc.is_scoring_stat=1",
        (league_key, through_week, team_key, team_key),
    )


def get_team_matchup_history(db, league_key: str, team_key: str, through_week: int):
    """Matchup results for a team ordered by week descending."""
    return db.fetchall(
        "SELECT week, winner_team_key, is_tied FROM matchup "
        "WHERE league_key=? AND week<=? AND (team_key_1=? OR team_key_2=?) "
        "ORDER BY week DESC",
        (league_key, through_week, team_key, team_key),
    )


def get_cross_season_h2h(db, team_key: str, opponent_key: str):
    """All-time H2H matchups between two team keys across all seasons."""
    return db.fetchall(
        "SELECT winner_team_key, is_tied FROM matchup "
        "WHERE league_key IN (SELECT league_key FROM league) "
        "    AND ((team_key_1=? AND team_key_2=?) OR (team_key_1=? AND team_key_2=?))",
        (team_key, opponent_key, opponent_key, team_key),
    )


def get_current_week_matchups(db, league_key: str, week: int):
    """Get team pairings for a specific week."""
    return db.fetchall(
        "SELECT team_key_1, team_key_2 FROM matchup "
        "WHERE league_key=? AND week=?",
        (league_key, week),
    )


def get_week_matchups(db, league_key: str, week: int):
    """Get full matchup data for a specific week."""
    return db.fetchall(
        "SELECT * FROM matchup WHERE league_key=? AND week=? ORDER BY matchup_id",
        (league_key, week),
    )


def get_matchup_categories(db, league_key: str, week: int, matchup_id: int):
    """Per-category breakdown for a specific matchup."""
    return db.fetchall(
        "SELECT mc.stat_id, sc.display_name, mc.team_1_value, "
        "       mc.team_2_value, mc.winner_team_key "
        "FROM matchup_category mc "
        "JOIN stat_category sc ON mc.league_key=sc.league_key AND mc.stat_id=sc.stat_id "
        "WHERE mc.league_key=? AND mc.week=? AND mc.matchup_id=? "
        "    AND sc.is_scoring_stat=1 "
        "ORDER BY sc.position_type, mc.stat_id",
        (league_key, week, matchup_id),
    )


def get_matchup_dates(db, league_key: str, week: int):
    """Get week_start and week_end dates for a matchup week."""
    return db.fetchone(
        "SELECT week_start, week_end FROM matchup WHERE league_key=? AND week=? LIMIT 1",
        (league_key, week),
    )


def get_regular_season_matchups(db, league_key: str):
    """All regular season (non-playoff, non-consolation) matchups."""
    return db.fetchall(
        "SELECT team_key_1, team_key_2, winner_team_key, is_tied "
        "FROM matchup WHERE league_key=? AND is_playoffs=0 AND is_consolation=0",
        (league_key,),
    )
