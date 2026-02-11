"""Data access layer — all database read queries organized by domain."""

from .leagues import (
    get_league,
    get_latest_league,
    get_league_week_info,
    get_all_seasons,
    get_scoring_categories,
    get_all_leagues_with_end_week,
    get_distinct_scoring_categories,
)
from .teams import (
    get_all_teams,
    get_team_info,
    get_league_team_keys,
    get_teams_missing_manager_names,
    update_manager_name,
)
from .matchups import (
    get_matchups_through_week,
    get_team_category_results,
    get_team_matchup_history,
    get_cross_season_h2h,
    get_current_week_matchups,
    get_week_matchups,
    get_matchup_categories,
    get_matchup_dates,
    get_regular_season_matchups,
)
from .players import (
    get_weekly_roster_stats,
    get_category_leaders,
    get_player_weekly_stats_sum,
)
from .transactions import (
    get_add_transactions,
    get_recent_adds,
    get_recent_drops,
    get_week_transactions,
)
from .history import (
    get_all_manager_teams,
    get_all_matchups_with_manager_guids,
    get_category_record_holder,
    get_all_regular_season_matchups_with_managers,
    get_all_regular_season_matchup_scores,
)
