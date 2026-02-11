"""Multi-sport Yahoo Fantasy client wrapping yfpy."""

import os
from pathlib import Path

from dotenv import load_dotenv
from yfpy.query import YahooFantasySportsQuery

from config import (
    Franchise,
    get_franchises,
    get_default_franchise,
    get_franchise_by_slug,
)

load_dotenv()  # needed for YAHOO_CONSUMER_KEY/SECRET

_PROJECT_DIR = Path(__file__).resolve().parent.parent


def _parse_league_key(league_key: str) -> tuple[str, str]:
    """Parse a league key like '458.l.25845' into (game_id, league_id)."""
    parts = league_key.split(".")
    if len(parts) == 3:
        return parts[0], parts[2]
    return "", league_key


class YahooClient:
    """Yahoo Fantasy client that supports multiple sports and franchises.

    By default, uses the default franchise for each sport (from franchises.yaml).
    Can also target a specific franchise or historical season.
    """

    def __init__(self):
        self._consumer_key = os.getenv("YAHOO_CONSUMER_KEY")
        self._consumer_secret = os.getenv("YAHOO_CONSUMER_SECRET")

        if not self._consumer_key or not self._consumer_secret:
            raise ValueError(
                "Missing YAHOO_CONSUMER_KEY or YAHOO_CONSUMER_SECRET in .env"
            )

        self._queries: dict[str, YahooFantasySportsQuery] = {}

    def _make_query(self, game_code: str, league_key: str) -> YahooFantasySportsQuery:
        """Create a yfpy query for a specific league key."""
        game_id, league_id = _parse_league_key(league_key)

        return YahooFantasySportsQuery(
            league_id=league_id,
            game_code=game_code,
            game_id=int(game_id) if game_id else None,
            yahoo_consumer_key=self._consumer_key,
            yahoo_consumer_secret=self._consumer_secret,
            env_file_location=_PROJECT_DIR,
            save_token_data_to_env_file=True,
            env_var_fallback=True,
            browser_callback=True,
        )

    def _get_query(self, sport: str) -> YahooFantasySportsQuery:
        """Get or create the yfpy query for a sport's default franchise (current season)."""
        if sport in self._queries:
            return self._queries[sport]

        franchise = get_default_franchise(sport)
        if not franchise:
            raise ValueError(
                f"No franchise configured for '{sport}' in franchises.yaml"
            )

        query = self._make_query(sport, franchise.latest_league_key)
        self._queries[sport] = query
        return query

    def query_for_franchise(
        self, slug: str, season: int = None
    ) -> YahooFantasySportsQuery:
        """Get a yfpy query for a specific franchise and optional season.

        Useful for historical lookups across seasons.
        """
        franchise = get_franchise_by_slug(slug)
        if not franchise:
            raise ValueError(f"Unknown franchise slug: '{slug}'")

        target_season = season or franchise.latest_season
        league_key = franchise.league_key_for_season(target_season)
        if not league_key:
            available = sorted(franchise.seasons.keys())
            raise ValueError(
                f"Franchise '{slug}' has no season {target_season}. "
                f"Available: {available}"
            )

        return self._make_query(franchise.sport, league_key)

    # -- User / discovery methods --

    def get_user_query(self) -> YahooFantasySportsQuery:
        """Get a yfpy query for user-level operations (listing seasons, leagues, etc).

        Uses the first available default franchise to authenticate.
        """
        for sport in ("mlb", "nba"):
            franchise = get_default_franchise(sport)
            if franchise:
                return self._make_query(sport, franchise.latest_league_key)
        raise ValueError("No franchises configured in franchises.yaml")

    def get_current_user(self):
        """Get the authenticated user's info."""
        return self.get_user_query().get_current_user()

    def get_user_teams(self):
        """Get teams the user owns across all sports."""
        return self.get_user_query().get_user_teams()

    # -- League methods --

    def get_league(self, sport: str):
        """Get league info for the default franchise."""
        return self._get_query(sport).get_league_info()

    def get_settings(self, sport: str):
        """Get league settings (scoring, roster positions, etc)."""
        return self._get_query(sport).get_league_settings()

    def get_standings(self, sport: str):
        """Get league standings."""
        return self._get_query(sport).get_league_standings()

    def get_teams(self, sport: str):
        """Get all teams in the league."""
        return self._get_query(sport).get_league_teams()

    def get_scoreboard(self, sport: str, week: int):
        """Get matchups/scoreboard for a specific week."""
        return self._get_query(sport).get_league_scoreboard_by_week(week)

    def get_matchups(self, sport: str, week: int):
        """Alias for get_scoreboard."""
        return self.get_scoreboard(sport, week)

    # -- Player methods --

    def get_players(self, sport: str, limit: int = 25, start: int = 0):
        """Get league players (paginated)."""
        return self._get_query(sport).get_league_players(
            player_count_limit=limit, player_count_start=start
        )

    def get_player_stats(self, sport: str, player_key: str):
        """Get a player's season stats."""
        return self._get_query(sport).get_player_stats_for_season(
            player_key=player_key
        )

    # -- Team methods --

    def get_roster(self, sport: str, team_id):
        """Get a team's current roster."""
        return self._get_query(sport).get_team_roster_by_week(team_id=team_id)

    def get_team_stats(self, sport: str, team_id):
        """Get a team's stats."""
        return self._get_query(sport).get_team_stats(team_id=team_id)

    # -- Transaction methods --

    def get_transactions(self, sport: str):
        """Get recent league transactions (trades, adds, drops, waivers)."""
        return self._get_query(sport).get_league_transactions()

    def get_draft_results(self, sport: str):
        """Get draft results."""
        return self._get_query(sport).get_league_draft_results()

    # -- Weekly data methods (for sync) --

    def get_roster_with_stats(self, sport: str, team_id, week: int):
        """Get a team's roster with per-player stats for a specific week."""
        return self._get_query(sport).get_team_roster_player_stats_by_week(
            team_id=team_id, chosen_week=week
        )

    def get_team_stats_by_week(self, query: YahooFantasySportsQuery,
                                team_key: str, week: int) -> list:
        """Get team aggregate stats for a week via raw API call.

        Uses raw query because yfpy's get_team_stats_by_week has a bug
        (KeyError on 'team_projected_points' for historical weeks).
        Returns list of {'stat_id': int, 'value': float} dicts.
        """
        url = (
            f"https://fantasysports.yahooapis.com/fantasy/v2"
            f"/team/{team_key}/stats;type=week;week={week}"
        )
        response = query.query(url, ["team", "team_stats"], data_type_class=None)
        return [
            {"stat_id": s["stat"].stat_id, "value": s["stat"].value}
            for s in response.get("stats", [])
        ]

    # -- Stat categories --

    def get_stat_categories(self, sport: str):
        """Get the scoring stat categories for the league's game."""
        query = self._get_query(sport)
        game_id = query.get_current_game_metadata().game_id
        return query.get_game_stat_categories_by_game_id(game_id=game_id)
