"""Shared utility functions for fantasy-analytics."""


def decode_name(obj) -> str:
    """Decode a yfpy name field that may be bytes, str, or have a .name attr."""
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")
    if hasattr(obj, "name"):
        return decode_name(obj.name)
    return str(obj)


def build_team_key(league_key: str, team_id: int) -> str:
    """Construct a Yahoo team key from a league key and team ID.

    Yahoo team keys follow: {league_key}.t.{team_id}
    e.g. "458.l.25845" + 3 -> "458.l.25845.t.3"
    """
    return f"{league_key}.t.{team_id}"


def is_mlb_league(db, league_key: str) -> bool:
    """Check if a league has batter/pitcher splits (i.e., is MLB)."""
    row = db.fetchone(
        "SELECT COUNT(*) as n FROM stat_category "
        "WHERE league_key=? AND position_type='P' AND is_scoring_stat=1",
        (league_key,),
    )
    return row["n"] > 0 if row else False
