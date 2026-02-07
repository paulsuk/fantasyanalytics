"""Multi-sport Yahoo Fantasy client wrapping yfpy."""

import os
from pathlib import Path

from dotenv import load_dotenv
from yfpy.query import YahooFantasySportsQuery

load_dotenv()

SPORT_CONFIGS = {
    "mlb": {
        "game_code": "mlb",
        "league_key_env": "MLB_LEAGUE_KEY",
    },
    "nba": {
        "game_code": "nba",
        "league_key_env": "NBA_LEAGUE_KEY",
    },
}


def _parse_league_id(league_key: str) -> str:
    """Extract numeric league ID from full key like '458.l.25845'."""
    parts = league_key.split(".")
    if len(parts) == 3:
        return parts[2]
    return league_key


class YahooClient:
    """Yahoo Fantasy client that supports multiple sports.

    Creates one yfpy query instance per sport. Sport is selected by passing
    "mlb" or "nba" to each method.
    """

    def __init__(self):
        self._consumer_key = os.getenv("YAHOO_CONSUMER_KEY")
        self._consumer_secret = os.getenv("YAHOO_CONSUMER_SECRET")

        if not self._consumer_key or not self._consumer_secret:
            raise ValueError(
                "Missing YAHOO_CONSUMER_KEY or YAHOO_CONSUMER_SECRET in .env"
            )

        self._queries: dict[str, YahooFantasySportsQuery] = {}
        self._env_file = Path(__file__).resolve().parent.parent / ".env"

    def _get_query(self, sport: str) -> YahooFantasySportsQuery:
        """Get or create the yfpy query instance for a sport."""
        if sport in self._queries:
            return self._queries[sport]

        config = SPORT_CONFIGS.get(sport)
        if not config:
            raise ValueError(f"Unknown sport '{sport}'. Use: {list(SPORT_CONFIGS)}")

        league_key = os.getenv(config["league_key_env"], "")
        if not league_key:
            raise ValueError(
                f"Missing {config['league_key_env']} in .env"
            )

        league_id = _parse_league_id(league_key)

        query = YahooFantasySportsQuery(
            league_id=league_id,
            game_code=config["game_code"],
            yahoo_consumer_key=self._consumer_key,
            yahoo_consumer_secret=self._consumer_secret,
            env_file_location=self._env_file,
            save_token_data_to_env_file=True,
            env_var_fallback=True,
            browser_callback=True,
        )
        self._queries[sport] = query
        return query

    # -- League methods --

    def get_league(self, sport: str):
        """Get league info."""
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

    # -- User methods --

    def get_current_user(self, sport: str):
        """Get the authenticated user's info."""
        return self._get_query(sport).get_current_user()

    def get_user_teams(self, sport: str):
        """Get teams the user owns."""
        return self._get_query(sport).get_user_teams()

    # -- Stat categories --

    def get_stat_categories(self, sport: str):
        """Get the scoring stat categories for the league's game."""
        query = self._get_query(sport)
        game_id = query.get_current_game_metadata().game_id
        return query.get_game_stat_categories_by_game_id(game_id=game_id)
