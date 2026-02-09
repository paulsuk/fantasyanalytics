"""Cross-season manager history and league records."""

from db import Database


class ManagerHistory:
    """Cross-season manager stats and H2H records for a franchise."""

    def __init__(self, db: Database):
        self.db = db

    def _load_manager_teams(self) -> dict[str, list[dict]]:
        """Map manager_guid -> list of {league_key, season, team_key, team_name}."""
        rows = self.db.fetchall(
            "SELECT t.manager_guid, t.manager_name, t.team_key, t.name AS team_name, "
            "       l.league_key, l.season "
            "FROM team t JOIN league l ON t.league_key = l.league_key "
            "WHERE t.manager_guid IS NOT NULL "
            "ORDER BY l.season"
        )
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
            })
        return by_guid

    def _all_matchups_with_guids(self) -> list[dict]:
        """All matchups annotated with manager GUIDs."""
        return self.db.fetchall(
            "SELECT m.league_key, m.week, m.winner_team_key, m.is_tied, "
            "       m.team_key_1, m.team_key_2, m.cats_won_1, m.cats_won_2, m.cats_tied, "
            "       m.is_playoffs, m.is_consolation, "
            "       t1.manager_guid AS guid_1, t2.manager_guid AS guid_2, "
            "       l.season "
            "FROM matchup m "
            "JOIN team t1 ON m.league_key = t1.league_key AND m.team_key_1 = t1.team_key "
            "JOIN team t2 ON m.league_key = t2.league_key AND m.team_key_2 = t2.team_key "
            "JOIN league l ON m.league_key = l.league_key"
        )

    def managers(self) -> list[dict]:
        """All managers with cross-season aggregate stats."""
        manager_teams = self._load_manager_teams()
        matchups = self._all_matchups_with_guids()

        # Aggregate W-L-T per manager guid (regular season only)
        records: dict[str, dict] = {}
        for guid, teams in manager_teams.items():
            name = teams[0]["manager_name"] or guid
            records[guid] = {
                "guid": guid,
                "name": name,
                "seasons": sorted(set(t["season"] for t in teams)),
                "wins": 0, "losses": 0, "ties": 0,
                "playoff_wins": 0, "playoff_losses": 0,
                "championships": 0,
                "best_finish": None,
                "worst_finish": None,
            }

        # Build team_key -> guid lookup
        tk_to_guid: dict[str, str] = {}
        for guid, teams in manager_teams.items():
            for t in teams:
                tk_to_guid[t["team_key"]] = guid

        for m in matchups:
            g1, g2 = m["guid_1"], m["guid_2"]
            if g1 not in records or g2 not in records:
                continue

            is_playoff = m["is_playoffs"] or m["is_consolation"]

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
                if m["is_tied"]:
                    records[g1]["ties"] += 1
                    records[g2]["ties"] += 1
                elif m["winner_team_key"] == m["team_key_1"]:
                    records[g1]["wins"] += 1
                    records[g2]["losses"] += 1
                else:
                    records[g2]["wins"] += 1
                    records[g1]["losses"] += 1

        # Championships and finishes from final standings per season
        leagues = self.db.fetchall("SELECT league_key, season, end_week FROM league")
        for lg in leagues:
            lk, end_week = lg["league_key"], lg["end_week"]
            teams = self.db.fetchall(
                "SELECT team_key FROM team WHERE league_key=?", (lk,)
            )
            # Compute standings from regular season matchups
            team_records: dict[str, dict] = {}
            for t in teams:
                tk = t["team_key"]
                team_records[tk] = {"w": 0, "l": 0}

            reg_matchups = self.db.fetchall(
                "SELECT team_key_1, team_key_2, winner_team_key, is_tied "
                "FROM matchup WHERE league_key=? AND is_playoffs=0 AND is_consolation=0",
                (lk,)
            )
            for rm in reg_matchups:
                tk1, tk2 = rm["team_key_1"], rm["team_key_2"]
                if tk1 in team_records and tk2 in team_records:
                    if rm["is_tied"]:
                        pass
                    elif rm["winner_team_key"] == tk1:
                        team_records[tk1]["w"] += 1
                        team_records[tk2]["l"] += 1
                    else:
                        team_records[tk2]["w"] += 1
                        team_records[tk1]["l"] += 1

            ranked = sorted(team_records.items(), key=lambda x: (x[1]["w"], -x[1]["l"]), reverse=True)
            for i, (tk, _) in enumerate(ranked):
                finish = i + 1
                guid = tk_to_guid.get(tk)
                if guid and guid in records:
                    best = records[guid]["best_finish"]
                    worst = records[guid]["worst_finish"]
                    records[guid]["best_finish"] = min(best, finish) if best else finish
                    records[guid]["worst_finish"] = max(worst, finish) if worst else finish
                    if finish == 1:
                        records[guid]["championships"] += 1

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


class LeagueRecords:
    """All-time league records across all seasons for a franchise."""

    def __init__(self, db: Database):
        self.db = db

    def records(self) -> dict:
        """Compute all-time records."""
        return {
            "category_records": self._category_records(),
            "streaks": self._streaks(),
            "matchup_records": self._matchup_records(),
        }

    def _category_records(self) -> list[dict]:
        """Best single-week values for each scoring category."""
        # Get all scoring stat categories (use latest season's categories as reference)
        cats = self.db.fetchall(
            "SELECT DISTINCT sc.stat_id, sc.display_name, sc.sort_order "
            "FROM stat_category sc "
            "JOIN league l ON sc.league_key = l.league_key "
            "WHERE sc.is_scoring_stat = 1 "
            "ORDER BY sc.display_name"
        )
        # Deduplicate by display_name (same stat across seasons)
        seen = set()
        unique_cats = []
        for c in cats:
            if c["display_name"] not in seen:
                seen.add(c["display_name"])
                unique_cats.append(c)

        results = []
        for cat in unique_cats:
            dn = cat["display_name"]
            higher_is_better = cat["sort_order"] == 1
            order = "DESC" if higher_is_better else "ASC"

            row = self.db.fetchone(
                f"SELECT tws.value, tws.week, t.manager_name, l.season, "
                f"       sc.display_name "
                f"FROM team_weekly_score tws "
                f"JOIN team t ON tws.league_key = t.league_key AND tws.team_key = t.team_key "
                f"JOIN league l ON tws.league_key = l.league_key "
                f"JOIN stat_category sc ON tws.league_key = sc.league_key "
                f"    AND tws.stat_id = sc.stat_id "
                f"WHERE sc.display_name = ? AND sc.is_scoring_stat = 1 "
                f"ORDER BY tws.value {order} LIMIT 1",
                (dn,),
            )
            if row:
                results.append({
                    "category": dn,
                    "value": row["value"],
                    "manager": row["manager_name"],
                    "season": row["season"],
                    "week": row["week"],
                    "higher_is_better": higher_is_better,
                })

        return results

    def _streaks(self) -> dict:
        """Longest win and loss streaks across all seasons."""
        # Get all matchups ordered by season + week per team
        rows = self.db.fetchall(
            "SELECT m.team_key_1, m.team_key_2, m.winner_team_key, m.is_tied, "
            "       t1.manager_guid AS guid_1, t1.manager_name AS name_1, "
            "       t2.manager_guid AS guid_2, t2.manager_name AS name_2, "
            "       l.season, m.week "
            "FROM matchup m "
            "JOIN team t1 ON m.league_key = t1.league_key AND m.team_key_1 = t1.team_key "
            "JOIN team t2 ON m.league_key = t2.league_key AND m.team_key_2 = t2.team_key "
            "JOIN league l ON m.league_key = l.league_key "
            "WHERE m.is_playoffs = 0 AND m.is_consolation = 0 "
            "ORDER BY l.season, m.week"
        )

        # Track per-manager streaks across all seasons
        # guid -> {current_type, current_count, name}
        active: dict[str, dict] = {}
        best_win = {"manager": "", "streak": 0}
        best_loss = {"manager": "", "streak": 0}

        def _check(guid: str, name: str, result: str):
            if guid not in active:
                active[guid] = {"type": None, "count": 0, "name": name}
            a = active[guid]
            a["name"] = name
            if result == a["type"]:
                a["count"] += 1
            else:
                a["type"] = result
                a["count"] = 1

            if result == "W" and a["count"] > best_win["streak"]:
                best_win["streak"] = a["count"]
                best_win["manager"] = name
            elif result == "L" and a["count"] > best_loss["streak"]:
                best_loss["streak"] = a["count"]
                best_loss["manager"] = name

        for r in rows:
            if r["is_tied"]:
                _check(r["guid_1"], r["name_1"], "T")
                _check(r["guid_2"], r["name_2"], "T")
            elif r["winner_team_key"] == r["team_key_1"]:
                _check(r["guid_1"], r["name_1"], "W")
                _check(r["guid_2"], r["name_2"], "L")
            else:
                _check(r["guid_2"], r["name_2"], "W")
                _check(r["guid_1"], r["name_1"], "L")

        return {
            "longest_win_streak": best_win,
            "longest_loss_streak": best_loss,
        }

    def _matchup_records(self) -> dict:
        """Biggest blowout and closest match."""
        rows = self.db.fetchall(
            "SELECT m.cats_won_1, m.cats_won_2, m.cats_tied, m.is_tied, "
            "       t1.manager_name AS manager_1, t2.manager_name AS manager_2, "
            "       l.season, m.week "
            "FROM matchup m "
            "JOIN team t1 ON m.league_key = t1.league_key AND m.team_key_1 = t1.team_key "
            "JOIN team t2 ON m.league_key = t2.league_key AND m.team_key_2 = t2.team_key "
            "JOIN league l ON m.league_key = l.league_key "
            "WHERE m.is_playoffs = 0 AND m.is_consolation = 0"
        )

        biggest_blowout = None
        closest_match = None
        max_margin = 0
        min_margin = 999

        for r in rows:
            c1, c2 = r["cats_won_1"], r["cats_won_2"]
            margin = abs(c1 - c2)

            if margin > max_margin:
                max_margin = margin
                winner = r["manager_1"] if c1 > c2 else r["manager_2"]
                loser = r["manager_2"] if c1 > c2 else r["manager_1"]
                biggest_blowout = {
                    "winner": winner,
                    "loser": loser,
                    "score": f"{max(c1,c2)}-{min(c1,c2)}-{r['cats_tied']}",
                    "season": r["season"],
                    "week": r["week"],
                }

            if 0 < margin < min_margin or (margin == 0 and not r["is_tied"]):
                pass  # ties with margin 0 are actual ties, skip

            if margin > 0 and margin < min_margin:
                min_margin = margin
                winner = r["manager_1"] if c1 > c2 else r["manager_2"]
                loser = r["manager_2"] if c1 > c2 else r["manager_1"]
                closest_match = {
                    "winner": winner,
                    "loser": loser,
                    "score": f"{max(c1,c2)}-{min(c1,c2)}-{r['cats_tied']}",
                    "season": r["season"],
                    "week": r["week"],
                }

        return {
            "biggest_blowout": biggest_blowout,
            "closest_match": closest_match,
        }
