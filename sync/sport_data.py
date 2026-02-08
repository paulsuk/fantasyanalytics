"""Sport-specific data clients wrapping pybaseball and nba_api."""

import time
from datetime import datetime, timedelta

import pandas as pd
from pybaseball import statcast, batting_stats, pitching_stats, playerid_lookup
from nba_api.stats.endpoints import CommonAllPlayers, PlayerGameLogs
from nba_api.stats.static import players as nba_players


class MLBDataClient:
    """Client for retrieving MLB stats from Statcast, FanGraphs, and Baseball Reference."""

    def get_player_id(self, first_name: str, last_name: str) -> pd.DataFrame:
        """Look up a player's IDs (MLB, FanGraphs, Baseball Reference).

        Returns a DataFrame with columns like key_mlbam, key_fangraphs, etc.
        """
        return playerid_lookup(last_name, first_name)

    def get_statcast(
        self,
        player_name: str,
        days_back: int = 30,
    ) -> pd.DataFrame:
        """Get recent Statcast data for a player.

        Returns pitch-level data with launch_speed, launch_angle, etc.
        """
        first, last = player_name.split(" ", 1)
        lookup = self.get_player_id(first, last)
        if lookup.empty:
            return pd.DataFrame()

        player_id = int(lookup.iloc[0]["key_mlbam"])
        end = datetime.now().strftime("%Y-%m-%d")
        start = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        return statcast(start_dt=start, end_dt=end, player_id=player_id)

    def get_batting_stats(self, season: int = None) -> pd.DataFrame:
        """Get FanGraphs batting stats for a season."""
        if season is None:
            season = datetime.now().year
        return batting_stats(season)

    def get_pitching_stats(self, season: int = None) -> pd.DataFrame:
        """Get FanGraphs pitching stats for a season."""
        if season is None:
            season = datetime.now().year
        return pitching_stats(season)


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
