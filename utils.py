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


def extract_player_id(player_key: str) -> str:
    """Extract stable player ID from a season-specific player key.

    Yahoo player keys follow: {game_key}.p.{player_id}
    e.g. "458.p.12345" -> "12345"

    The player_id is stable across seasons (same real-world player = same ID).
    """
    return player_key.rsplit(".p.", 1)[-1] if player_key else ""
