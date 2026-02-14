"""Franchise detail: full history for a single franchise."""

from config import Franchise
from db import Database
from db.queries import (
    get_all_manager_teams,
    get_all_seasons,
    get_league_week_info,
    get_end_of_season_roster,
    get_transaction_counts_for_teams,
    get_trades_for_teams,
    get_week_matchups,
    get_team_info,
    get_keepers_for_teams,
    get_roster_with_draft_costs,
    get_players_dropped_in_week,
)
from analytics.history import ManagerHistory


class FranchiseDetail:
    """Full detail view for a single franchise."""

    def __init__(self, db: Database, franchise: Franchise, franchise_id: str):
        self.db = db
        self._franchise = franchise
        self._franchise_id = franchise_id
        self._fdef = self._find_franchise_def()
        self._team_keys_by_season = self._resolve_team_keys()

    def _find_franchise_def(self) -> dict:
        for fdef in self._franchise.franchise_list():
            if fdef["id"] == self._franchise_id:
                return fdef
        raise ValueError(f"Unknown franchise: {self._franchise_id}")

    def _resolve_team_keys(self) -> dict[int, str]:
        """Map season -> team_key for this franchise's owners."""
        all_teams = get_all_manager_teams(self.db)
        owner_guids = {o["guid"]: o for o in self._fdef["ownership"]}
        result: dict[int, str] = {}
        for t in all_teams:
            guid = t["manager_guid"]
            if guid not in owner_guids:
                continue
            o = owner_guids[guid]
            season = t["season"]
            if season < o["from"]:
                continue
            if o.get("to") is not None and season > o["to"]:
                continue
            result[season] = t["team_key"]
        return result

    def detail(self) -> dict:
        history = ManagerHistory(self.db, self._franchise)
        all_franchise_stats = history.franchise_stats()
        this = next((f for f in all_franchise_stats if f["id"] == self._franchise_id), None)
        if not this:
            raise ValueError(f"No stats for franchise: {self._franchise_id}")

        return {
            "overview": {
                "id": this["id"],
                "name": this["name"],
                "current_manager": this["current_manager"],
                "current_team_name": this["current_team_name"],
                "ownership": this["ownership"],
                "seasons": this["seasons"],
            },
            "stats": {
                "wins": this["wins"],
                "losses": this["losses"],
                "ties": this["ties"],
                "cat_wins": this["cat_wins"],
                "cat_losses": this["cat_losses"],
                "cat_ties": this["cat_ties"],
                "championships": this["championships"],
                "best_finish": self._best_finish(this["season_records"]),
                "worst_finish": self._worst_finish(this["season_records"]),
            },
            "season_records": this["season_records"],
            "manager_eras": self._manager_eras(history),
            "h2h": self._franchise_h2h(history),
            "rosters": self._end_of_season_rosters(),
            "keepers": self._keeper_history(),
            "roster_costs": self._roster_with_costs(),
            "transactions": self._transaction_summary(),
            "current_matchup": self._current_matchup(),
        }

    def _best_finish(self, season_records: list[dict]) -> int | None:
        finishes = [sr["finish"] for sr in season_records if sr.get("finish") is not None]
        return min(finishes) if finishes else None

    def _worst_finish(self, season_records: list[dict]) -> int | None:
        finishes = [sr["finish"] for sr in season_records if sr.get("finish") is not None]
        return max(finishes) if finishes else None

    def _manager_eras(self, history: ManagerHistory) -> list[dict]:
        """Per-owner stats within this franchise's ownership periods."""
        all_managers = history.managers()
        mgr_by_guid = {m["guid"]: m for m in all_managers}

        eras = []
        for owner in self._fdef["ownership"]:
            guid = owner["guid"]
            mgr = mgr_by_guid.get(guid)
            if not mgr:
                continue
            fr_from = owner["from"]
            fr_to = owner.get("to")

            wins = losses = ties = cat_wins = cat_losses = cat_ties = championships = 0
            seasons = []
            for sr in mgr["season_records"]:
                s = sr["season"]
                if s < fr_from:
                    continue
                if fr_to is not None and s > fr_to:
                    continue
                wins += sr["wins"]
                losses += sr["losses"]
                ties += sr["ties"]
                cat_wins += sr["cat_wins"]
                cat_losses += sr["cat_losses"]
                cat_ties += sr["cat_ties"]
                if sr.get("finish") == 1:
                    championships += 1
                seasons.append(s)

            eras.append({
                "name": mgr["name"],
                "guid": guid,
                "from": fr_from,
                "to": fr_to,
                "wins": wins,
                "losses": losses,
                "ties": ties,
                "cat_wins": cat_wins,
                "cat_losses": cat_losses,
                "cat_ties": cat_ties,
                "championships": championships,
                "seasons": sorted(seasons),
            })
        return eras

    def _franchise_h2h(self, history: ManagerHistory) -> list[dict]:
        """H2H record vs each other franchise, enriched with names."""
        full_matrix = history.franchise_h2h_matrix()
        my_row = full_matrix.get(self._franchise_id, {})

        name_map = {f["id"]: f["name"] for f in self._franchise.franchise_list()}
        result = []
        for fid, record in my_row.items():
            result.append({
                "franchise_id": fid,
                "name": name_map.get(fid, fid),
                "wins": record["wins"],
                "losses": record["losses"],
                "ties": record["ties"],
            })
        result.sort(key=lambda r: (r["wins"], -r["losses"]), reverse=True)
        return result

    def _end_of_season_rosters(self) -> dict[int, list[dict]]:
        """Final roster for each season, annotated with keeper badges."""
        seasons = get_all_seasons(self.db)
        season_info = {s["season"]: s["league_key"] for s in seasons}

        # Build keeper lookup: {(season, player_name_lower) -> round_cost}
        team_keys = list(self._team_keys_by_season.values())
        keeper_rows = get_keepers_for_teams(self.db, team_keys) if team_keys else []
        keeper_lookup: dict[tuple[int, str], int | None] = {}
        for kr in keeper_rows:
            keeper_lookup[(kr["season"], kr["player_name"].lower())] = kr["round_cost"]

        rosters: dict[int, list[dict]] = {}
        for season, team_key in sorted(self._team_keys_by_season.items()):
            league_key = season_info.get(season)
            if not league_key:
                continue
            week_info = get_league_week_info(self.db, league_key)
            if not week_info:
                continue
            last_week = week_info["end_week"] or week_info["current_week"]
            if not last_week:
                continue
            rows = get_end_of_season_roster(self.db, league_key, team_key, last_week)
            dropped = get_players_dropped_in_week(self.db, league_key, team_key, last_week)
            roster = []
            for r in rows:
                if r["player_key"] in dropped:
                    continue
                name_lower = r["full_name"].lower() if r["full_name"] else ""
                keeper_key = (season, name_lower)
                is_keeper = keeper_key in keeper_lookup
                entry = {
                    "full_name": r["full_name"],
                    "primary_position": r["primary_position"],
                    "selected_position": r["selected_position"],
                    "is_starter": bool(r["is_starter"]),
                }
                if is_keeper:
                    entry["is_keeper"] = True
                    entry["keeper_round"] = keeper_lookup[keeper_key]
                roster.append(entry)
            rosters[season] = roster
        return rosters

    def _keeper_history(self) -> list[dict]:
        """Per-season keeper selections for this franchise."""
        team_keys = list(self._team_keys_by_season.values())
        if not team_keys:
            return []
        rows = get_keepers_for_teams(self.db, team_keys)
        by_season: dict[int, list[dict]] = {}
        for r in rows:
            tenure = None
            if r["kept_from_season"] is not None:
                tenure = r["season"] - r["kept_from_season"] + 1
            by_season.setdefault(r["season"], []).append({
                "name": r["player_name"],
                "position": r["primary_position"],
                "round_cost": r["round_cost"],
                "kept_from_season": r["kept_from_season"],
                "tenure": tenure,
            })
        return [
            {"season": season, "keepers": keepers}
            for season, keepers in sorted(by_season.items())
        ]

    def _roster_with_costs(self) -> dict[int, list[dict]]:
        """End-of-season roster with draft costs. Baseball only."""
        if self._franchise.sport != "mlb":
            return {}

        seasons = get_all_seasons(self.db)
        season_info = {s["season"]: s["league_key"] for s in seasons}
        result: dict[int, list[dict]] = {}

        for season, team_key in sorted(self._team_keys_by_season.items()):
            league_key = season_info.get(season)
            if not league_key:
                continue
            week_info = get_league_week_info(self.db, league_key)
            if not week_info:
                continue
            last_week = week_info["end_week"] or week_info["current_week"]
            if not last_week:
                continue
            rows = get_roster_with_draft_costs(
                self.db, league_key, team_key, last_week
            )
            dropped = get_players_dropped_in_week(self.db, league_key, team_key, last_week)
            result[season] = [
                {
                    "full_name": r["full_name"],
                    "primary_position": r["primary_position"],
                    "selected_position": r["selected_position"],
                    "is_starter": bool(r["is_starter"]),
                    "draft_cost": r["draft_cost"],
                }
                for r in rows
                if r["player_key"] not in dropped
            ]
        return result

    def _current_matchup(self) -> dict | None:
        """Latest matchup for the latest unfinished season."""
        seasons = get_all_seasons(self.db)
        for s in seasons:  # already ordered by season DESC
            if s["is_finished"]:
                continue
            season = s["season"]
            if season not in self._team_keys_by_season:
                continue
            league_key = s["league_key"]
            team_key = self._team_keys_by_season[season]
            # Find the latest week that has matchup data for this team
            row = self.db.fetchone(
                "SELECT MAX(week) AS latest_week FROM matchup "
                "WHERE league_key=? AND (team_key_1=? OR team_key_2=?)",
                (league_key, team_key, team_key),
            )
            if not row or not row["latest_week"]:
                continue
            latest_week = row["latest_week"]
            matchups = get_week_matchups(self.db, league_key, latest_week)
            for m in matchups:
                if team_key not in (m["team_key_1"], m["team_key_2"]):
                    continue
                is_team_1 = m["team_key_1"] == team_key
                opp_key = m["team_key_2"] if is_team_1 else m["team_key_1"]
                opp_info = get_team_info(self.db, league_key, opp_key)
                return {
                    "season": season,
                    "week": latest_week,
                    "opponent_team_name": opp_info["name"] if opp_info else "Unknown",
                    "opponent_manager": opp_info["manager_name"] if opp_info else "Unknown",
                    "cats_won": m["cats_won_1"] if is_team_1 else m["cats_won_2"],
                    "cats_lost": m["cats_won_2"] if is_team_1 else m["cats_won_1"],
                    "cats_tied": m["cats_tied"],
                    "is_playoffs": bool(m["is_playoffs"]),
                }
        return None

    def _transaction_summary(self) -> dict:
        """Per-season add/drop counts + trade details."""
        team_keys = list(self._team_keys_by_season.values())
        if not team_keys:
            return {"counts": [], "trades": []}

        counts_rows = get_transaction_counts_for_teams(self.db, team_keys)
        counts = [
            {"season": r["season"], "adds": r["adds"], "drops": r["drops"]}
            for r in counts_rows
        ]

        trade_rows = get_trades_for_teams(self.db, team_keys)
        # Group by transaction_key for multi-player trades
        trades_by_key: dict[str, dict] = {}
        for r in trade_rows:
            tk = r["transaction_key"]
            if tk not in trades_by_key:
                trades_by_key[tk] = {
                    "season": r["season"],
                    "week": r["week"],
                    "timestamp": r["timestamp"],
                    "players": [],
                }
            trades_by_key[tk]["players"].append({
                "name": r["player_name"],
                "source_team": r["source_team_name"] or "Unknown",
                "dest_team": r["dest_team_name"] or "Unknown",
            })

        trades = sorted(trades_by_key.values(), key=lambda t: t["timestamp"])

        return {"counts": counts, "trades": trades}
