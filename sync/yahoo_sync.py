"""Yahoo Fantasy API -> SQLite sync pipeline."""

import time
from datetime import datetime, timezone

from config import get_franchise_by_slug, add_managers, Franchise, bench_positions
from db import Database
from yahoo.client import YahooClient
from utils import decode_name, build_team_key


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class YahooSync:
    """Syncs Yahoo Fantasy data into a franchise's SQLite database."""

    def __init__(self, slug: str, delay: float = 0.5):
        self.franchise = get_franchise_by_slug(slug)
        if not self.franchise:
            raise ValueError(f"Unknown franchise slug: '{slug}'")
        self.client = YahooClient()
        self.db = Database(slug)
        self.db.initialize()
        self.delay = delay  # seconds between API calls
        self._bench_positions = bench_positions(self.franchise.sport)

    def _wait(self):
        time.sleep(self.delay)

    def _is_synced(self, league_key: str, sync_type: str, week: int = 0) -> bool:
        row = self.db.fetchone(
            "SELECT status FROM sync_log WHERE league_key=? AND sync_type=? AND week=?",
            (league_key, sync_type, week),
        )
        return row is not None and row["status"] == "completed"

    def _log_start(self, league_key: str, sync_type: str, week: int = 0):
        self.db.execute(
            "INSERT OR REPLACE INTO sync_log (league_key, sync_type, week, started_at, status) "
            "VALUES (?, ?, ?, ?, 'running')",
            (league_key, sync_type, week, _now_iso()),
        )

    def _log_complete(self, league_key: str, sync_type: str, week: int = 0,
                      records: int = 0):
        self.db.execute(
            "UPDATE sync_log SET completed_at=?, status='completed', records_written=? "
            "WHERE league_key=? AND sync_type=? AND week=?",
            (_now_iso(), records, league_key, sync_type, week),
        )

    def _log_fail(self, league_key: str, sync_type: str, week: int = 0,
                  error: str = ""):
        self.db.execute(
            "UPDATE sync_log SET completed_at=?, status='failed', error_message=? "
            "WHERE league_key=? AND sync_type=? AND week=?",
            (_now_iso(), error, league_key, sync_type, week),
        )

    # ------------------------------------------------------------------
    # Metadata sync
    # ------------------------------------------------------------------

    def sync_metadata(self, query, league_key: str):
        """Sync league info, settings, stat categories, and teams."""
        if self._is_synced(league_key, "metadata"):
            print(f"  [skip] metadata already synced for {league_key}")
            return

        self._log_start(league_key, "metadata")
        try:
            league = query.get_league_info()
            settings = query.get_league_settings()
            self._wait()

            # Teams (fetch before transaction to avoid holding lock during API calls)
            teams = query.get_league_teams()
            self._wait()
            standings = query.get_league_standings()
            self._wait()

            # Build waiver/faab from standings teams
            standings_map = {}
            for t in standings.teams:
                standings_map[t.team_key] = t

            with self.db.transaction():
                # League table
                self.db.execute(
                    "INSERT OR REPLACE INTO league VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        league_key,
                        league.season,
                        decode_name(league.name),
                        league.num_teams,
                        league.scoring_type,
                        len([s for s in settings.stat_categories.stats
                             if not getattr(s, "is_only_display_stat", 0)]),
                        league.current_week,
                        league.start_week,
                        league.end_week,
                        settings.playoff_start_week,
                        1 if getattr(settings, "uses_faab", False) else 0,
                        getattr(league, "is_finished", 0),
                        _now_iso(),
                    ),
                )

                # Stat categories
                records = 0
                for s in settings.stat_categories.stats:
                    stat = getattr(s, "stat", s)
                    self.db.execute(
                        "INSERT OR REPLACE INTO stat_category VALUES (?,?,?,?,?,?,?,?,?)",
                        (
                            league_key,
                            stat.stat_id,
                            getattr(stat, "name", ""),
                            getattr(stat, "display_name", ""),
                            getattr(stat, "display_name", ""),  # abbr = display_name
                            getattr(stat, "sort_order", 1),
                            getattr(stat, "position_type", None),
                            getattr(s, "is_only_display_stat", 0),
                            0 if getattr(s, "is_only_display_stat", 0) else 1,
                        ),
                    )
                    records += 1

                # Teams
                for team in teams:
                    st = standings_map.get(team.team_key, team)
                    mgrs = getattr(team, "managers", [])
                    mgr = mgrs[0] if mgrs else None
                    guid = getattr(mgr, "guid", "") if mgr else ""
                    nickname = getattr(mgr, "nickname", "") if mgr else ""
                    resolved_name = self.franchise.manager_name(guid) or ""

                    self.db.execute(
                        "INSERT OR REPLACE INTO team VALUES (?,?,?,?,?,?,?,?,?)",
                        (
                            league_key,
                            team.team_key,
                            team.team_id,
                            decode_name(team.name),
                            guid,
                            nickname,
                            resolved_name,
                            getattr(st, "waiver_priority", None),
                            getattr(st, "faab_balance", None),
                        ),
                    )
                    records += 1

                self._log_complete(league_key, "metadata", records=records)
            print(f"  [done] metadata: {records} records")

        except Exception as e:
            self._log_fail(league_key, "metadata", error=str(e))
            raise

    # ------------------------------------------------------------------
    # Draft sync
    # ------------------------------------------------------------------

    def sync_draft(self, query, league_key: str):
        """Sync draft results."""
        if self._is_synced(league_key, "draft"):
            print(f"  [skip] draft already synced for {league_key}")
            return

        self._log_start(league_key, "draft")
        try:
            picks = query.get_league_draft_results()
            self._wait()

            with self.db.transaction():
                records = 0
                for pick in picks:
                    self.db.execute(
                        "INSERT OR REPLACE INTO draft_pick VALUES (?,?,?,?,?,?)",
                        (
                            league_key,
                            pick.pick,
                            pick.round,
                            pick.team_key,
                            pick.player_key,
                            getattr(pick, "cost", None),
                        ),
                    )
                    records += 1

                self._log_complete(league_key, "draft", records=records)
            print(f"  [done] draft: {records} picks")

        except Exception as e:
            self._log_fail(league_key, "draft", error=str(e))
            raise

    # ------------------------------------------------------------------
    # Transactions sync
    # ------------------------------------------------------------------

    def sync_transactions(self, query, league_key: str):
        """Sync all league transactions."""
        if self._is_synced(league_key, "transactions"):
            print(f"  [skip] transactions already synced for {league_key}")
            return

        self._log_start(league_key, "transactions")
        try:
            txns = query.get_league_transactions()
            self._wait()

            with self.db.transaction():
                records = 0
                for txn in txns:
                    txn_key = txn.transaction_key
                    self.db.execute(
                        "INSERT OR REPLACE INTO transaction_record VALUES (?,?,?,?,?,?,?,?,?)",
                        (
                            txn_key,
                            league_key,
                            getattr(txn, "type", ""),
                            getattr(txn, "status", ""),
                            getattr(txn, "timestamp", ""),
                            None,  # week — computed later from matchup date ranges
                            getattr(txn, "trader_team_key", None),
                            getattr(txn, "tradee_team_key", None),
                            getattr(txn, "faab_bid", None),
                        ),
                    )

                    players = getattr(txn, "players", []) or []
                    for p in players:
                        player = getattr(p, "player", p)
                        td = getattr(player, "transaction_data", None)
                        self.db.execute(
                            "INSERT OR REPLACE INTO transaction_player VALUES (?,?,?,?,?,?,?)",
                            (
                                txn_key,
                                getattr(player, "player_key", ""),
                                getattr(td, "source_type", "") if td else "",
                                getattr(td, "source_team_key", None) if td else None,
                                getattr(td, "destination_type", "") if td else "",
                                getattr(td, "destination_team_key", None) if td else None,
                                getattr(td, "type", "") if td else "",
                            ),
                        )

                    records += 1

                self._log_complete(league_key, "transactions", records=records)
            print(f"  [done] transactions: {records} records")

        except Exception as e:
            self._log_fail(league_key, "transactions", error=str(e))
            raise

    # ------------------------------------------------------------------
    # Weekly data sync
    # ------------------------------------------------------------------

    def sync_week(self, query, league_key: str, week: int, num_teams: int,
                  stat_categories: list):
        """Sync one week: matchups, team stats, rosters, player stats."""
        if self._is_synced(league_key, "weekly", week):
            print(f"  [skip] week {week} already synced")
            return

        self._log_start(league_key, "weekly", week)
        try:
          with self.db.transaction():
            # 1. Scoreboard — matchup results
            scoreboard = query.get_league_scoreboard_by_week(week)
            self._wait()

            scoring_stats = [c for c in stat_categories if c["is_scoring"]]

            records = 0
            for idx, matchup in enumerate(scoreboard.matchups):
                t1, t2 = matchup.teams[0], matchup.teams[1]
                pts1 = t1.team_points.total if t1.team_points else 0
                pts2 = t2.team_points.total if t2.team_points else 0

                cats_tied = len(scoring_stats) - int(pts1) - int(pts2)
                self.db.execute(
                    "INSERT OR REPLACE INTO matchup VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        league_key, week, idx,
                        t1.team_key, t2.team_key,
                        int(pts1), int(pts2),
                        max(cats_tied, 0),
                        getattr(matchup, "winner_team_key", None),
                        getattr(matchup, "is_tied", 0),
                        getattr(matchup, "is_playoffs", 0),
                        getattr(matchup, "is_consolation", 0),
                        getattr(matchup, "week_start", ""),
                        getattr(matchup, "week_end", ""),
                    ),
                )
                records += 1

            # 2. Team aggregate stats + per-category matchup results
            team_stats = {}
            for team_id in range(1, num_teams + 1):
                team_key = build_team_key(league_key, team_id)
                try:
                    stats = self.client.get_team_stats_by_week(query, team_key, week)
                    self._wait()

                    for s in stats:
                        self.db.execute(
                            "INSERT OR REPLACE INTO team_weekly_score VALUES (?,?,?,?,?)",
                            (league_key, week, team_key, s["stat_id"], s["value"]),
                        )
                        records += 1

                    team_stats[team_key] = {s["stat_id"]: s["value"] for s in stats}
                except Exception as e:
                    print(f"    [warn] failed to get stats for {team_key} week {week}: {e}")

            # Derive per-category matchup results
            for idx, matchup in enumerate(scoreboard.matchups):
                t1_key = matchup.teams[0].team_key
                t2_key = matchup.teams[1].team_key
                t1_stats = team_stats.get(t1_key, {})
                t2_stats = team_stats.get(t2_key, {})

                for cat in scoring_stats:
                    sid = cat["stat_id"]
                    v1 = t1_stats.get(sid)
                    v2 = t2_stats.get(sid)

                    winner = None
                    if v1 is not None and v2 is not None:
                        try:
                            f1, f2 = float(v1), float(v2)
                            higher_is_better = cat["sort_order"] == 1
                            if higher_is_better:
                                winner = t1_key if f1 > f2 else (t2_key if f2 > f1 else None)
                            else:
                                winner = t1_key if f1 < f2 else (t2_key if f2 < f1 else None)
                        except (ValueError, TypeError):
                            pass

                    self.db.execute(
                        "INSERT OR REPLACE INTO matchup_category VALUES (?,?,?,?,?,?,?)",
                        (league_key, week, idx, sid, v1, v2, winner),
                    )
                    records += 1

            # 3. Rosters + player stats
            for team_id in range(1, num_teams + 1):
                try:
                    roster = query.get_team_roster_player_stats_by_week(
                        team_id=team_id, chosen_week=week
                    )
                    self._wait()

                    team_key = build_team_key(league_key, team_id)
                    for p in roster:
                        player_key = p.player_key
                        pos = p.selected_position
                        selected_pos = pos.position if pos else None
                        is_bench = 1 if selected_pos in self._bench_positions else 0

                        # Player master record
                        name_obj = p.name
                        full_name = name_obj.full if hasattr(name_obj, "full") else str(name_obj)
                        self.db.execute(
                            "INSERT OR IGNORE INTO player VALUES (?,?,?,?,?,?,?,?,?)",
                            (
                                player_key,
                                getattr(p, "player_id", None),
                                full_name,
                                getattr(name_obj, "first", ""),
                                getattr(name_obj, "last", ""),
                                getattr(p, "editorial_team_abbr", ""),
                                getattr(p, "display_position", ""),
                                ",".join(
                                    ep.position
                                    for ep in (getattr(p, "eligible_positions", []) or [])
                                    if hasattr(ep, "position")
                                ),
                                getattr(p, "headshot_url", ""),
                            ),
                        )

                        # Weekly roster
                        self.db.execute(
                            "INSERT OR REPLACE INTO weekly_roster VALUES (?,?,?,?,?,?)",
                            (league_key, week, team_key, player_key,
                             selected_pos, 0 if is_bench else 1),
                        )

                        # Player weekly stats
                        ps = p.player_stats
                        if ps and ps.stats:
                            for s in ps.stats:
                                self.db.execute(
                                    "INSERT OR REPLACE INTO player_weekly_stat VALUES (?,?,?,?,?)",
                                    (league_key, week, player_key, s.stat_id, s.value),
                                )

                        records += 1

                except Exception as e:
                    print(f"    [warn] failed roster for team {team_id} week {week}: {e}")

            self._log_complete(league_key, "weekly", week, records=records)
          print(f"  [done] week {week}: {records} records")

        except Exception as e:
            self._log_fail(league_key, "weekly", week, error=str(e))
            raise

    # ------------------------------------------------------------------
    # Manager check
    # ------------------------------------------------------------------

    def _check_unconfigured_managers(self, league_key: str):
        """After sync, auto-add any unconfigured managers to franchises.yaml."""
        rows = self.db.fetchall(
            "SELECT DISTINCT manager_guid, manager_nickname FROM team "
            "WHERE league_key=? AND manager_guid != ''",
            (league_key,),
        )
        to_add = {}
        for r in rows:
            guid = r["manager_guid"]
            if not self.franchise.manager_name(guid):
                nickname = r["manager_nickname"]
                to_add[guid] = {"name": nickname, "short_name": nickname}

        if not to_add:
            return

        added = add_managers(self.franchise.slug, to_add)
        if added:
            # Reload franchise config so subsequent syncs see the new managers
            self.franchise = get_franchise_by_slug(self.franchise.slug)
            names = [to_add[g]["name"] for g in added]
            print(f"  [config] Added {len(added)} manager(s) to franchises.yaml: {', '.join(names)}")
            print(f"           Edit franchises.yaml to set full names if needed.")
        print()

    # ------------------------------------------------------------------
    # Transaction week backfill
    # ------------------------------------------------------------------

    def _backfill_transaction_weeks(self, league_key: str):
        """Compute transaction_record.week from matchup date ranges."""
        weeks = self.db.fetchall(
            "SELECT DISTINCT week, week_start, week_end FROM matchup "
            "WHERE league_key=? ORDER BY week",
            (league_key,),
        )
        if not weeks:
            return

        txns = self.db.fetchall(
            "SELECT transaction_key, timestamp FROM transaction_record "
            "WHERE league_key=? AND week IS NULL AND timestamp IS NOT NULL "
            "    AND timestamp != ''",
            (league_key,),
        )
        if not txns:
            return

        updated = 0
        with self.db.transaction():
            for txn in txns:
                # Convert Unix epoch to ISO date
                try:
                    ts_epoch = int(txn["timestamp"])
                    ts_date = datetime.fromtimestamp(ts_epoch, tz=timezone.utc).strftime("%Y-%m-%d")
                except (ValueError, TypeError):
                    ts_date = txn["timestamp"]

                # Find which week contains this date
                assigned_week = weeks[-1]["week"]  # default to last week
                for w in weeks:
                    if w["week_start"] and ts_date <= w["week_end"]:
                        assigned_week = w["week"]
                        break

                self.db.execute(
                    "UPDATE transaction_record SET week=? WHERE transaction_key=?",
                    (assigned_week, txn["transaction_key"]),
                )
                updated += 1

        if updated:
            print(f"  [done] backfilled week for {updated} transactions")

    # ------------------------------------------------------------------
    # Full season sync
    # ------------------------------------------------------------------

    def sync_season(self, season: int):
        """Full sync for one season: metadata, draft, transactions, all weeks."""
        league_key = self.franchise.league_key_for_season(season)
        if not league_key:
            available = sorted(self.franchise.seasons.keys())
            raise ValueError(f"No league key for season {season}. Available: {available}")

        query = self.client.query_for_franchise(self.franchise.slug, season)

        print(f"\nSyncing {self.franchise.slug} season {season} ({league_key})")

        # Metadata (league, settings, categories, teams)
        self.sync_metadata(query, league_key)

        # Draft
        self.sync_draft(query, league_key)

        # Transactions
        self.sync_transactions(query, league_key)

        # Get league info for week range
        league_row = self.db.fetchone(
            "SELECT * FROM league WHERE league_key=?", (league_key,)
        )
        num_teams = league_row["num_teams"]
        end_week = league_row["end_week"]
        is_finished = league_row["is_finished"]

        # Determine how many weeks to sync
        if is_finished:
            weeks_to_sync = range(1, end_week + 1)
        else:
            current_week = league_row["current_week"]
            # Sync completed weeks (current_week - 1 for in-progress seasons)
            weeks_to_sync = range(1, current_week)

        # Load stat categories for matchup_category derivation
        cat_rows = self.db.fetchall(
            "SELECT stat_id, sort_order, is_scoring_stat FROM stat_category WHERE league_key=?",
            (league_key,),
        )
        stat_categories = [
            {
                "stat_id": r["stat_id"],
                "sort_order": r["sort_order"],
                "is_scoring": r["is_scoring_stat"],
            }
            for r in cat_rows
        ]

        # Weekly data
        for week in weeks_to_sync:
            self.sync_week(query, league_key, week, num_teams, stat_categories)

        # Backfill transaction weeks now that matchup dates are available
        self._backfill_transaction_weeks(league_key)

        self._check_unconfigured_managers(league_key)
        print(f"Season {season} sync complete.\n")

    def sync_all(self):
        """Sync all configured seasons for this franchise."""
        for season in sorted(self.franchise.seasons):
            self.sync_season(season)

    def sync_incremental(self):
        """Sync only the latest unsynced week for the current season."""
        season = self.franchise.latest_season
        league_key = self.franchise.latest_league_key
        query = self.client.query_for_franchise(self.franchise.slug, season)

        print(f"\nIncremental sync for {self.franchise.slug} season {season}")

        # Ensure metadata exists
        self.sync_metadata(query, league_key)

        league_row = self.db.fetchone(
            "SELECT * FROM league WHERE league_key=?", (league_key,)
        )
        num_teams = league_row["num_teams"]
        current_week = league_row["current_week"]

        cat_rows = self.db.fetchall(
            "SELECT stat_id, sort_order, is_scoring_stat FROM stat_category WHERE league_key=?",
            (league_key,),
        )
        stat_categories = [
            {"stat_id": r["stat_id"], "sort_order": r["sort_order"],
             "is_scoring": r["is_scoring_stat"]}
            for r in cat_rows
        ]

        # Sync any unsynced weeks up to current
        end = current_week if league_row["is_finished"] else current_week
        for week in range(1, end + 1):
            self.sync_week(query, league_key, week, num_teams, stat_categories)

        # Re-sync transactions (always refresh)
        self.db.execute(
            "DELETE FROM sync_log WHERE league_key=? AND sync_type='transactions'",
            (league_key,),
        )
        self.sync_transactions(query, league_key)

        print("Incremental sync complete.\n")

    def close(self):
        self.db.close()
