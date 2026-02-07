"""MLB data client wrapping pybaseball and MLB Stats API."""

import pandas as pd
from datetime import datetime, timedelta
from pybaseball import statcast, batting_stats, pitching_stats, playerid_lookup


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
