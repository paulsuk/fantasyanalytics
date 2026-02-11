"""Team queries."""


def get_all_teams(db, league_key: str):
    """Get all teams for a league."""
    return db.fetchall(
        "SELECT team_key, name, manager_name FROM team WHERE league_key=?",
        (league_key,),
    )


def get_team_info(db, league_key: str, team_key: str):
    """Get team name and manager_name."""
    return db.fetchone(
        "SELECT name, manager_name FROM team WHERE league_key=? AND team_key=?",
        (league_key, team_key),
    )


def get_league_team_keys(db, league_key: str):
    """Get team_keys for a league."""
    return db.fetchall(
        "SELECT team_key FROM team WHERE league_key=?", (league_key,)
    )


def get_teams_missing_manager_names(db):
    """Get distinct manager GUIDs that have no manager_name set."""
    return db.fetchall(
        "SELECT DISTINCT manager_guid FROM team "
        "WHERE manager_guid != '' AND (manager_name IS NULL OR manager_name = '')"
    )


def update_manager_name(db, name: str, guid: str):
    """Set manager_name for all teams with the given manager_guid."""
    db.execute(
        "UPDATE team SET manager_name=? WHERE manager_guid=?",
        (name, guid),
    )
