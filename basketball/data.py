"""NBA data client wrapping nba_api."""

import time

import pandas as pd
from nba_api.stats.endpoints import CommonAllPlayers, PlayerGameLogs
from nba_api.stats.static import players as nba_players


class NBADataClient:
    """Client for retrieving NBA stats from the official NBA API."""

    def get_player_id(self, name: str) -> int | None:
        """Look up an NBA player ID by name.

        Searches the nba_api static player list. Returns the player ID or None.
        """
        matches = nba_players.find_players_by_full_name(name)
        if matches:
            return matches[0]["id"]
        return None

    def get_player_game_logs(
        self,
        player_id: int,
        season: str = "2024-25",
    ) -> pd.DataFrame:
        """Get game-by-game stats for a player in a season.

        Args:
            player_id: NBA player ID.
            season: Season string like '2024-25'.

        Returns:
            DataFrame with one row per game (PTS, REB, AST, etc).
        """
        logs = PlayerGameLogs(
            season_nullable=season,
            player_id_nullable=player_id,
        ).get_data_frames()[0]
        time.sleep(0.5)  # respect rate limits
        return logs

    def get_all_players(self, season: str = "2024-25") -> pd.DataFrame:
        """Get all active NBA players for a season.

        Returns DataFrame with PERSON_ID, DISPLAY_FIRST_LAST, FROM_YEAR, TO_YEAR.
        """
        players = CommonAllPlayers(
            is_only_current_season=1,
            league_id="00",
            season=season,
        ).get_data_frames()[0]
        time.sleep(0.5)
        return players
