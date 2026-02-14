"""Keeper queries."""


def get_keepers_for_teams(db, team_keys: list[str]):
    """All keepers for a set of team keys (franchise across seasons)."""
    if not team_keys:
        return []
    placeholders = ",".join("?" * len(team_keys))
    return db.fetchall(
        f"SELECT k.player_name, k.player_key, k.season, k.round_cost, "
        f"       k.kept_from_season, k.team_key, p.primary_position "
        f"FROM keeper k "
        f"JOIN league l ON k.league_key = l.league_key "
        f"LEFT JOIN player p ON k.player_key = p.player_key "
        f"WHERE k.team_key IN ({placeholders}) "
        f"ORDER BY k.season, k.round_cost",
        tuple(team_keys),
    )


def get_roster_with_draft_costs(db, league_key: str, team_key: str, week: int):
    """Roster for a team/week with draft cost per player.

    Cost rules:
    - Drafted and never dropped -> team_pick_index (persists through trades)
    - Dropped at any point (even by another team) -> 24
    - Never drafted (FA only) -> 24
    """
    return db.fetchall(
        "WITH team_picks AS ("
        "  SELECT dp.player_key, "
        "    ROW_NUMBER() OVER (PARTITION BY dp.team_key ORDER BY dp.pick) as team_pick_idx "
        "  FROM draft_pick dp "
        "  WHERE dp.league_key = ?"
        "), "
        "dropped_players AS ("
        "  SELECT DISTINCT tp.player_key "
        "  FROM transaction_player tp "
        "  JOIN transaction_record tr ON tp.transaction_key = tr.transaction_key "
        "  WHERE tr.league_key = ? AND tp.type = 'drop'"
        ") "
        "SELECT wr.player_key, p.full_name, p.primary_position, "
        "  wr.selected_position, wr.is_starter, "
        "  CASE "
        "    WHEN tp_cost.player_key IS NULL THEN 24 "
        "    WHEN dropped.player_key IS NOT NULL THEN 24 "
        "    ELSE tp_cost.team_pick_idx "
        "  END as draft_cost "
        "FROM weekly_roster wr "
        "LEFT JOIN player p ON wr.player_key = p.player_key "
        "LEFT JOIN team_picks tp_cost ON tp_cost.player_key = wr.player_key "
        "LEFT JOIN dropped_players dropped ON dropped.player_key = wr.player_key "
        "WHERE wr.league_key = ? AND wr.team_key = ? AND wr.week = ? "
        "ORDER BY wr.is_starter DESC, wr.selected_position",
        (league_key, league_key, league_key, team_key, week),
    )


def get_keepers_by_season(db, league_key: str):
    """All keepers for a given season/league."""
    return db.fetchall(
        "SELECT k.player_name, k.player_key, k.team_key, k.round_cost, "
        "       k.kept_from_season, t.name AS team_name, t.manager_name "
        "FROM keeper k "
        "JOIN team t ON k.league_key = t.league_key AND k.team_key = t.team_key "
        "WHERE k.league_key = ? "
        "ORDER BY t.name, k.round_cost",
        (league_key,),
    )
