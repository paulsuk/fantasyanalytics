"""Cross-season manager history and league records."""

from config import Franchise
from db import Database
from db.queries import (
    get_all_manager_teams,
    get_all_matchups_with_manager_guids,
    get_distinct_scoring_categories,
    get_category_record_holder,
    get_all_regular_season_matchups_with_managers,
    get_all_regular_season_matchup_scores,
)


class ManagerHistory:
    """Cross-season manager stats and H2H records for a franchise."""

    def __init__(self, db: Database, franchise: Franchise):
        self.db = db
        self._franchise = franchise
        self._current_guids = franchise.current_manager_guids

    def _load_manager_teams(self) -> dict[str, list[dict]]:
        """Map manager_guid -> list of {league_key, season, team_key, team_name, finish, ...}."""
        rows = get_all_manager_teams(self.db)
        by_guid: dict[str, list[dict]] = {}
        for r in rows:
            guid = r["manager_guid"]
            if guid not in by_guid:
                by_guid[guid] = []
            by_guid[guid].append({
                "league_key": r["league_key"],
                "season": r["season"],
                "team_key": r["team_key"],
                "team_name": r["team_name"],
                "manager_name": r["manager_name"],
                "finish": r["finish"],
                "playoff_seed": r["playoff_seed"],
                "is_finished": r["is_finished"],
            })
        return by_guid

    def _all_matchups_with_guids(self) -> list:
        """All matchups annotated with manager GUIDs."""
        return get_all_matchups_with_manager_guids(self.db)

    def managers(self) -> list[dict]:
        """All managers with cross-season aggregate stats."""
        manager_teams = self._load_manager_teams()
        matchups = self._all_matchups_with_guids()

        # Aggregate W-L-T per manager guid (regular season only)
        records: dict[str, dict] = {}
        for guid, teams in manager_teams.items():
            name = self._franchise.manager_name(guid) or teams[0]["manager_name"] or guid
            records[guid] = {
                "guid": guid,
                "name": name,
                "is_current": guid in self._current_guids,
                "seasons": sorted(set(t["season"] for t in teams)),
                "wins": 0, "losses": 0, "ties": 0,
                "playoff_wins": 0, "playoff_losses": 0,
                "championships": 0,
                "regular_season_firsts": 0,
                "best_finish": None,
                "worst_finish": None,
            }

        # Build team_key -> guid lookup and team_key -> team info
        tk_to_guid: dict[str, str] = {}
        tk_to_info: dict[str, dict] = {}
        for guid, teams in manager_teams.items():
            for t in teams:
                tk_to_guid[t["team_key"]] = guid
                tk_to_info[t["team_key"]] = {
                    "season": t["season"],
                    "team_name": t["team_name"],
                }

        # Per-season per-team records
        season_records: dict[str, dict[int, dict]] = {}  # guid -> {season -> {w,l,t,team_name}}
        for guid, teams in manager_teams.items():
            season_records[guid] = {}
            for t in teams:
                season_records[guid][t["season"]] = {
                    "season": t["season"],
                    "team_name": t["team_name"],
                    "wins": 0, "losses": 0, "ties": 0,
                    "cat_wins": 0, "cat_losses": 0, "cat_ties": 0,
                    "finish": t.get("finish"),
                    "playoff_seed": t.get("playoff_seed"),
                }

        for m in matchups:
            g1, g2 = m["guid_1"], m["guid_2"]
            if g1 not in records or g2 not in records:
                continue

            is_playoff = m["is_playoffs"] or m["is_consolation"]

            season = m["season"]

            if is_playoff:
                if m["is_tied"]:
                    pass
                elif m["winner_team_key"] == m["team_key_1"]:
                    records[g1]["playoff_wins"] += 1
                    records[g2]["playoff_losses"] += 1
                else:
                    records[g2]["playoff_wins"] += 1
                    records[g1]["playoff_losses"] += 1
            else:
                # Category W-L-T (per-category results within the matchup)
                c1, c2, ct = m["cats_won_1"] or 0, m["cats_won_2"] or 0, m["cats_tied"] or 0
                if season in season_records.get(g1, {}):
                    season_records[g1][season]["cat_wins"] += c1
                    season_records[g1][season]["cat_losses"] += c2
                    season_records[g1][season]["cat_ties"] += ct
                if season in season_records.get(g2, {}):
                    season_records[g2][season]["cat_wins"] += c2
                    season_records[g2][season]["cat_losses"] += c1
                    season_records[g2][season]["cat_ties"] += ct

                # Matchup W-L-T (weekly outcome)
                if m["is_tied"]:
                    records[g1]["ties"] += 1
                    records[g2]["ties"] += 1
                    if season in season_records.get(g1, {}):
                        season_records[g1][season]["ties"] += 1
                    if season in season_records.get(g2, {}):
                        season_records[g2][season]["ties"] += 1
                elif m["winner_team_key"] == m["team_key_1"]:
                    records[g1]["wins"] += 1
                    records[g2]["losses"] += 1
                    if season in season_records.get(g1, {}):
                        season_records[g1][season]["wins"] += 1
                    if season in season_records.get(g2, {}):
                        season_records[g2][season]["losses"] += 1
                else:
                    records[g2]["wins"] += 1
                    records[g1]["losses"] += 1
                    if season in season_records.get(g2, {}):
                        season_records[g2][season]["wins"] += 1
                    if season in season_records.get(g1, {}):
                        season_records[g1][season]["losses"] += 1

        # Championships, finishes, and regular season firsts from DB
        for guid, teams in manager_teams.items():
            if guid not in records:
                continue
            for t in teams:
                finish = t.get("finish")
                playoff_seed = t.get("playoff_seed")
                is_finished = t.get("is_finished")

                # Best/worst finish uses final rank (playoff placement)
                if finish is not None:
                    best = records[guid]["best_finish"]
                    worst = records[guid]["worst_finish"]
                    records[guid]["best_finish"] = min(best, finish) if best else finish
                    records[guid]["worst_finish"] = max(worst, finish) if worst else finish

                    # Championship = finished season + final rank 1 (won playoffs)
                    if is_finished and finish == 1:
                        records[guid]["championships"] += 1

                # Regular season first = playoff_seed 1 (best regular season record)
                if playoff_seed is not None and playoff_seed == 1:
                    records[guid]["regular_season_firsts"] += 1

        # Attach per-season breakdowns
        for guid, rec in records.items():
            sr = season_records.get(guid, {})
            rec["season_records"] = sorted(sr.values(), key=lambda x: x["season"])

        result = sorted(records.values(), key=lambda r: (r["wins"], -r["losses"]), reverse=True)
        return result

    def h2h_matrix(self) -> dict:
        """Pairwise H2H records between all managers.

        Returns {guid_a: {guid_b: {wins, losses, ties}, ...}, ...}
        """
        matchups = self._all_matchups_with_guids()

        matrix: dict[str, dict[str, dict]] = {}

        for m in matchups:
            g1, g2 = m["guid_1"], m["guid_2"]
            if g1 == g2:
                continue

            for ga, gb, tk_a in [(g1, g2, m["team_key_1"]), (g2, g1, m["team_key_2"])]:
                if ga not in matrix:
                    matrix[ga] = {}
                if gb not in matrix[ga]:
                    matrix[ga][gb] = {"wins": 0, "losses": 0, "ties": 0}

                if m["is_tied"]:
                    matrix[ga][gb]["ties"] += 1
                elif m["winner_team_key"] == tk_a:
                    matrix[ga][gb]["wins"] += 1
                else:
                    matrix[ga][gb]["losses"] += 1

        return matrix

    def franchise_h2h_matrix(self) -> dict:
        """Pairwise H2H records between franchises (not managers).

        Returns {franchise_id_a: {franchise_id_b: {wins, losses, ties}, ...}, ...}
        Only includes matchups where both managers resolve to a franchise.
        """
        if not self._franchise.has_franchises:
            return {}

        matchups = self._all_matchups_with_guids()
        matrix: dict[str, dict[str, dict]] = {}

        for m in matchups:
            g1, g2 = m["guid_1"], m["guid_2"]
            season = m["season"]
            fid1 = self._franchise.resolve_franchise(g1, season)
            fid2 = self._franchise.resolve_franchise(g2, season)
            if not fid1 or not fid2 or fid1 == fid2:
                continue

            for fa, fb, tk_a in [(fid1, fid2, m["team_key_1"]), (fid2, fid1, m["team_key_2"])]:
                if fa not in matrix:
                    matrix[fa] = {}
                if fb not in matrix[fa]:
                    matrix[fa][fb] = {"wins": 0, "losses": 0, "ties": 0}

                if m["is_tied"]:
                    matrix[fa][fb]["ties"] += 1
                elif m["winner_team_key"] == tk_a:
                    matrix[fa][fb]["wins"] += 1
                else:
                    matrix[fa][fb]["losses"] += 1

        return matrix

    def franchise_stats(self) -> list[dict]:
        """Franchise-level aggregate stats, combining all managers per franchise."""
        if not self._franchise.has_franchises:
            return []

        manager_list = self.managers()
        mgr_by_guid = {m["guid"]: m for m in manager_list}
        franchise_defs = self._franchise.franchise_list()

        results = []
        for fdef in franchise_defs:
            wins = losses = ties = championships = 0
            all_seasons: list[int] = []
            season_records: list[dict] = []

            for owner in fdef["ownership"]:
                guid = owner["guid"]
                mgr = mgr_by_guid.get(guid)
                if not mgr:
                    continue
                fr_from = owner["from"]
                fr_to = owner.get("to")

                for sr in mgr["season_records"]:
                    s = sr["season"]
                    if s < fr_from:
                        continue
                    if fr_to is not None and s > fr_to:
                        continue
                    wins += sr["wins"]
                    losses += sr["losses"]
                    ties += sr["ties"]
                    if sr.get("finish") == 1:
                        championships += 1
                    all_seasons.append(s)
                    season_records.append({
                        **sr,
                        "manager": mgr["name"],
                    })

            season_records.sort(key=lambda x: x["season"])
            current_team_name = season_records[-1]["team_name"] if season_records else fdef["name"]

            results.append({
                "id": fdef["id"],
                "name": fdef["name"],
                "current_manager": fdef["current_manager"],
                "current_team_name": current_team_name,
                "ownership": fdef["ownership"],
                "seasons": sorted(set(all_seasons)),
                "wins": wins,
                "losses": losses,
                "ties": ties,
                "championships": championships,
                "season_records": season_records,
            })

        results.sort(key=lambda r: (r["wins"], -r["losses"]), reverse=True)
        return results


class LeagueRecords:
    """All-time league records across all seasons for a franchise."""

    def __init__(self, db: Database, include_playoffs: bool = False):
        self.db = db
        self._include_playoffs = include_playoffs

    def records(self) -> dict:
        """Compute all-time records."""
        return {
            "category_records": self._category_records(),
            "streaks": self._streaks(),
            "matchup_records": self._matchup_records(),
        }

    def _category_records(self) -> list[dict]:
        """Best single-week values for each scoring category."""
        unique_cats = get_distinct_scoring_categories(self.db)

        results = []
        for cat in unique_cats:
            dn = cat["display_name"]
            higher_is_better = cat["sort_order"] == 1
            order = "DESC" if higher_is_better else "ASC"

            row = get_category_record_holder(self.db, dn, order)
            if row:
                results.append({
                    "category": dn,
                    "value": row["value"],
                    "manager": row["manager_name"],
                    "team_name": row["team_name"],
                    "season": row["season"],
                    "week": row["week"],
                    "higher_is_better": higher_is_better,
                })

        return results

    def _streaks(self) -> dict:
        """Longest win and loss streaks across all seasons."""
        rows = get_all_regular_season_matchups_with_managers(self.db, self._include_playoffs)

        # Track per-manager streaks across all seasons
        active: dict[str, dict] = {}
        undefeated: dict[str, dict] = {}
        best_win = {"manager": "", "team_name": "", "streak": 0}
        best_loss = {"manager": "", "team_name": "", "streak": 0}
        best_undefeated = {"manager": "", "team_name": "", "streak": 0}

        def _check(guid: str, name: str, team_name: str, result: str):
            if guid not in active:
                active[guid] = {"type": None, "count": 0, "name": name, "team_name": team_name}
            a = active[guid]
            a["name"] = name
            a["team_name"] = team_name
            if result == a["type"]:
                a["count"] += 1
            else:
                a["type"] = result
                a["count"] = 1

            if result == "W" and a["count"] > best_win["streak"]:
                best_win["streak"] = a["count"]
                best_win["manager"] = name
                best_win["team_name"] = team_name
            elif result == "L" and a["count"] > best_loss["streak"]:
                best_loss["streak"] = a["count"]
                best_loss["manager"] = name
                best_loss["team_name"] = team_name

            if guid not in undefeated:
                undefeated[guid] = {"count": 0, "name": name, "team_name": team_name}
            u = undefeated[guid]
            u["name"] = name
            u["team_name"] = team_name
            if result == "L":
                u["count"] = 0
            else:
                u["count"] += 1
                if u["count"] > best_undefeated["streak"]:
                    best_undefeated["streak"] = u["count"]
                    best_undefeated["manager"] = name
                    best_undefeated["team_name"] = team_name

        for r in rows:
            if r["is_tied"]:
                _check(r["guid_1"], r["name_1"], r["team_name_1"], "T")
                _check(r["guid_2"], r["name_2"], r["team_name_2"], "T")
            elif r["winner_team_key"] == r["team_key_1"]:
                _check(r["guid_1"], r["name_1"], r["team_name_1"], "W")
                _check(r["guid_2"], r["name_2"], r["team_name_2"], "L")
            else:
                _check(r["guid_2"], r["name_2"], r["team_name_2"], "W")
                _check(r["guid_1"], r["name_1"], r["team_name_1"], "L")

        return {
            "longest_win_streak": best_win,
            "longest_loss_streak": best_loss,
            "longest_undefeated_streak": best_undefeated,
        }

    def _matchup_records(self) -> dict:
        """Biggest blowout and closest match."""
        rows = get_all_regular_season_matchup_scores(self.db, self._include_playoffs)

        biggest_blowout = None
        closest_match = None
        max_margin = 0
        min_margin = 999

        for r in rows:
            c1, c2 = r["cats_won_1"], r["cats_won_2"]
            margin = abs(c1 - c2)

            if margin > max_margin:
                max_margin = margin
                is_1 = c1 > c2
                biggest_blowout = {
                    "winner": r["manager_1"] if is_1 else r["manager_2"],
                    "loser": r["manager_2"] if is_1 else r["manager_1"],
                    "winner_team": r["team_name_1"] if is_1 else r["team_name_2"],
                    "loser_team": r["team_name_2"] if is_1 else r["team_name_1"],
                    "score": f"{max(c1,c2)}-{min(c1,c2)}-{r['cats_tied']}",
                    "season": r["season"],
                    "week": r["week"],
                }

            if margin > 0 and margin < min_margin:
                min_margin = margin
                is_1 = c1 > c2
                closest_match = {
                    "winner": r["manager_1"] if is_1 else r["manager_2"],
                    "loser": r["manager_2"] if is_1 else r["manager_1"],
                    "winner_team": r["team_name_1"] if is_1 else r["team_name_2"],
                    "loser_team": r["team_name_2"] if is_1 else r["team_name_1"],
                    "score": f"{max(c1,c2)}-{min(c1,c2)}-{r['cats_tied']}",
                    "season": r["season"],
                    "week": r["week"],
                }

        return {
            "biggest_blowout": biggest_blowout,
            "closest_match": closest_match,
        }
