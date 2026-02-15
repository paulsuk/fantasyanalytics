"""Yahoo Fantasy API -> SQLite sync pipeline."""

import time
from datetime import datetime, timezone

from config import get_franchise_by_slug, add_managers, Franchise, bench_positions
from db import Database
from sync.yahoo_client import YahooClient
from utils import decode_name, build_team_key, extract_player_id


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
                # Extract week info with validation
                # Yahoo returns unreliable week data for historical seasons
                # (e.g. start/end swapped, current_week from the live season)
                start_week = league.start_week
                end_week = league.end_week
                current_week = league.current_week
                playoff_start_week = settings.playoff_start_week
                is_finished = getattr(league, "is_finished", 0)

                # Fix swapped start/end weeks
                if start_week and end_week and start_week > end_week:
                    start_week, end_week = end_week, start_week

                # For finished seasons, current_week should equal end_week
                if is_finished and end_week:
                    current_week = end_week

                # Sanity check: if end_week seems wrong but playoff_start_week
                # is valid, estimate end_week (playoffs are typically 2-3 weeks)
                if playoff_start_week and (not end_week or end_week < playoff_start_week):
                    end_week = playoff_start_week + 2
                    if is_finished:
                        current_week = end_week

                # League table
                self.db.execute(
                    "INSERT OR REPLACE INTO league VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        league_key,
                        league.season,
                        decode_name(league.name),
                        self.franchise.sport,
                        league.num_teams,
                        league.scoring_type,
                        len([s for s in settings.stat_categories.stats
                             if not getattr(s, "is_only_display_stat", 0)]),
                        current_week,
                        start_week,
                        end_week,
                        playoff_start_week,
                        1 if getattr(settings, "uses_faab", False) else 0,
                        is_finished,
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

                    # Extract standings rank info
                    # Yahoo rank = final placement (playoff result); 1 = champion
                    # playoff_seed = regular season finish (seed entering playoffs)
                    ts = getattr(st, "team_standings", None)
                    finish = None
                    playoff_seed = None
                    if ts:
                        rank_val = getattr(ts, "rank", None)
                        finish = int(rank_val) if rank_val is not None else None
                        seed_val = getattr(ts, "playoff_seed", None)
                        playoff_seed = int(seed_val) if seed_val is not None else None

                    self.db.execute(
                        "INSERT OR REPLACE INTO team VALUES (?,?,?,?,?,?,?,?,?,?,?)",
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
                            finish,
                            playoff_seed,
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
    # League week repair
    # ------------------------------------------------------------------

    def _repair_league_weeks(self, league_key: str):
        """Fix league start/end/current week from actual matchup data.

        Yahoo returns unreliable week metadata for historical seasons.
        The matchup data is the source of truth.
        """
        info = self.db.fetchone(
            "SELECT MIN(week) as min_w, MAX(week) as max_w, "
            "MIN(CASE WHEN is_playoffs=1 OR is_consolation=1 "
            "    THEN week ELSE NULL END) as first_playoff "
            "FROM matchup WHERE league_key=?",
            (league_key,),
        )
        if not info or not info["min_w"]:
            return

        league = self.db.fetchone(
            "SELECT start_week, end_week, is_finished FROM league WHERE league_key=?",
            (league_key,),
        )
        if not league:
            return

        start_w = info["min_w"]
        end_w = info["max_w"]
        current_w = end_w if league["is_finished"] else league["end_week"]
        needs_fix = (
            league["start_week"] != start_w
            or league["end_week"] != end_w
        )

        if needs_fix:
            self.db.execute(
                "UPDATE league SET start_week=?, end_week=?, current_week=? "
                "WHERE league_key=?",
                (start_w, end_w, current_w, league_key),
            )
            if info["first_playoff"]:
                self.db.execute(
                    "UPDATE league SET playoff_start_week=? WHERE league_key=?",
                    (info["first_playoff"], league_key),
                )
            print(f"  [fix] league weeks corrected: {start_w}-{end_w}")

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

        # Repair league week metadata from actual matchup data
        # (Yahoo returns unreliable week info for historical seasons)
        self._repair_league_weeks(league_key)

        self._check_unconfigured_managers(league_key)
        print(f"Season {season} sync complete.\n")

    def sync_keepers(self, verbose: bool = False):
        """Sync keeper designations from Yahoo for all seasons.

        Queries week 1 rosters and checks Player.is_keeper for each player.
        Keepers are only available from season 2 onward (first season has no keepers).

        Args:
            verbose: If True, print detailed diagnostic info for every player's
                     is_keeper value (useful for debugging missing keepers).
        """
        print(f"\nSyncing keepers for {self.franchise.slug}")

        for season in sorted(self.franchise.seasons):
            league_key = self.franchise.league_key_for_season(season)
            if not league_key:
                continue

            if self._is_synced(league_key, "keepers"):
                print(f"  [skip] keepers already synced for {season}")
                continue

            league_row = self.db.fetchone(
                "SELECT num_teams FROM league WHERE league_key=?", (league_key,)
            )
            if not league_row:
                print(f"  [skip] no metadata for {season} — sync season first")
                continue

            num_teams = league_row["num_teams"]
            query = self.client.query_for_franchise(self.franchise.slug, season)

            self._log_start(league_key, "keepers")
            try:
                total = 0
                with self.db.transaction():
                    for team_id in range(1, num_teams + 1):
                        team_key = build_team_key(league_key, team_id)
                        try:
                            roster = query.get_team_roster_player_stats_by_week(
                                team_id=team_id, chosen_week=1
                            )
                            self._wait()

                            if verbose:
                                print(f"    Team {team_id} ({team_key}): {len(roster)} players")

                            team_keepers = 0
                            for p in roster:
                                ik = getattr(p, "is_keeper", None)
                                name_obj = p.name
                                full_name = name_obj.full if hasattr(name_obj, "full") else str(name_obj)

                                # Determine keeper status — handle dict, int, and other formats
                                if isinstance(ik, dict):
                                    is_kept = bool(ik.get("kept"))
                                elif isinstance(ik, (int, float)):
                                    is_kept = bool(ik)
                                else:
                                    is_kept = False

                                if verbose:
                                    status = "KEEPER" if is_kept else "      "
                                    safe_name = full_name.encode("ascii", "replace").decode()
                                    print(f"      {status} {safe_name:30s} is_keeper={ik!r} (type={type(ik).__name__})")

                                if not is_kept:
                                    continue

                                player_key = p.player_key
                                self.db.execute(
                                    "INSERT OR REPLACE INTO keeper "
                                    "(league_key, team_key, player_key, player_name, "
                                    "season, round_cost, kept_from_season) "
                                    "VALUES (?,?,?,?,?,?,?)",
                                    (league_key, team_key, player_key, full_name,
                                     season, None, None),
                                )
                                total += 1
                                team_keepers += 1

                            if verbose:
                                print(f"      -> {team_keepers} keepers for team {team_id}")

                        except Exception as e:
                            print(f"    [warn] team {team_id} week 1 failed: {e}")

                    # Draft-based fallback for basketball
                    if self.franchise.sport == "nba":
                        added = self._draft_keeper_fallback(
                            league_key, season, num_teams,
                        )
                        total += added

                    # Enrich round_cost from draft picks (team pick index)
                    if total > 0:
                        self.db.execute(
                            "UPDATE keeper SET round_cost = sub.team_pick_idx "
                            "FROM ("
                            "  SELECT dp.player_key, dp.team_key, "
                            "    ROW_NUMBER() OVER (PARTITION BY dp.team_key ORDER BY dp.pick) as team_pick_idx "
                            "  FROM draft_pick dp "
                            "  WHERE dp.league_key = ?"
                            ") sub "
                            "WHERE keeper.league_key = ? "
                            "  AND keeper.team_key = sub.team_key "
                            "  AND keeper.player_key = sub.player_key",
                            (league_key, league_key),
                        )

                    self._log_complete(league_key, "keepers", records=total)
                print(f"  [done] {season}: {total} keepers")

            except Exception as e:
                self._log_fail(league_key, "keepers", error=str(e))
                print(f"  [fail] {season}: {e}")

        # Compute kept_from_season across all seasons
        self._compute_kept_from_season()

        print("Keeper sync complete.\n")

    def _draft_keeper_fallback(self, league_key: str, season: int,
                               num_teams: int,
                               keepers_per_team: int = 4) -> int:
        """Fill missing keepers from draft picks for basketball leagues.

        Yahoo's is_keeper flag is incomplete for many teams/seasons.
        Basketball keepers are either the first N or last N draft picks
        per team. This method auto-detects the mode from existing Yahoo
        keepers and fills in the gaps.

        Returns the number of keepers added.
        """
        # Count existing Yahoo-flagged keepers per team
        existing = self.db.fetchall(
            "SELECT team_key, COUNT(*) as cnt FROM keeper "
            "WHERE league_key=? GROUP BY team_key",
            (league_key,),
        )
        existing_counts = {r["team_key"]: r["cnt"] for r in existing}
        total_existing = sum(existing_counts.values())

        if total_existing == 0:
            # No Yahoo keepers at all — probably first season, skip
            return 0

        expected_total = num_teams * keepers_per_team
        if total_existing >= expected_total:
            return 0  # All keepers accounted for

        # Get draft picks with per-team pick index
        picks = self.db.fetchall(
            "SELECT dp.team_key, dp.player_key, p.full_name, "
            "  ROW_NUMBER() OVER (PARTITION BY dp.team_key ORDER BY dp.pick) "
            "    as team_pick_idx, "
            "  COUNT(*) OVER (PARTITION BY dp.team_key) as total_picks "
            "FROM draft_pick dp "
            "LEFT JOIN player p ON dp.player_key = p.player_key "
            "WHERE dp.league_key = ? "
            "ORDER BY dp.team_key, dp.pick",
            (league_key,),
        )
        if not picks:
            return 0

        total_picks_per_team = picks[0]["total_picks"]

        # Build set of existing keeper player_keys
        keeper_pks = set()
        for kr in self.db.fetchall(
            "SELECT player_key FROM keeper WHERE league_key=?",
            (league_key,),
        ):
            if kr["player_key"]:
                keeper_pks.add(kr["player_key"])

        # Auto-detect mode: count Yahoo keepers in first-N vs last-N
        first_n = set(range(1, keepers_per_team + 1))
        last_n_start = total_picks_per_team - keepers_per_team + 1
        last_n = set(range(last_n_start, total_picks_per_team + 1))

        first_count = 0
        last_count = 0
        for p in picks:
            if p["player_key"] in keeper_pks:
                if p["team_pick_idx"] in first_n:
                    first_count += 1
                elif p["team_pick_idx"] in last_n:
                    last_count += 1

        if first_count >= last_count:
            keeper_positions = first_n
            mode = "first"
        else:
            keeper_positions = last_n
            mode = "last"

        # Insert missing keepers from draft picks at detected positions
        added = 0
        for p in picks:
            if p["team_pick_idx"] not in keeper_positions:
                continue
            if p["player_key"] in keeper_pks:
                continue  # Already a keeper

            full_name = p["full_name"] or "Unknown"
            self.db.execute(
                "INSERT OR REPLACE INTO keeper "
                "(league_key, team_key, player_key, player_name, "
                "season, round_cost, kept_from_season) "
                "VALUES (?,?,?,?,?,?,?)",
                (league_key, p["team_key"], p["player_key"], full_name,
                 season, p["team_pick_idx"], None),
            )
            added += 1

        if added:
            print(f"    [draft fallback] {mode}-{keepers_per_team} mode: "
                  f"added {added} keepers ({total_existing} from Yahoo)")

        return added

    def _compute_kept_from_season(self):
        """Compute kept_from_season using consecutive-season runs.

        Groups keepers by (franchise_id, player_key) — franchise_id is
        resolved from manager GUID via config, so it stays stable even
        when team numbers change across seasons. Uses gap-and-island to
        find consecutive runs and sets kept_from_season = earliest in run.
        """
        raw_rows = self.db.fetchall(
            "SELECT k.rowid, k.team_key, k.player_key, k.season, "
            "       t.manager_guid "
            "FROM keeper k "
            "JOIN team t ON k.league_key = t.league_key "
            "  AND k.team_key = t.team_key "
            "ORDER BY k.player_key, k.season"
        )
        if not raw_rows:
            return

        # Resolve each keeper's franchise_id from manager GUID
        rows = []
        for r in raw_rows:
            guid = r["manager_guid"] or ""
            rows.append({
                "rowid": r["rowid"],
                "player_key": r["player_key"],
                "season": r["season"],
                "franchise_id": (
                    self.franchise.resolve_franchise(guid, r["season"]) or guid
                ),
            })

        from itertools import groupby

        def gk(r):
            return (r["franchise_id"], extract_player_id(r["player_key"]))

        rows_sorted = sorted(rows, key=lambda r: (gk(r), r["season"]))

        updates = []
        for _key, group in groupby(rows_sorted, key=gk):
            entries = list(group)
            current_start = entries[0]["season"]
            for i, entry in enumerate(entries):
                if i > 0 and entry["season"] != entries[i - 1]["season"] + 1:
                    current_start = entry["season"]
                updates.append((current_start, entry["rowid"]))

        with self.db.transaction():
            for kept_from, rowid in updates:
                self.db.execute(
                    "UPDATE keeper SET kept_from_season = ? WHERE rowid = ?",
                    (kept_from, rowid),
                )

        if updates:
            print(f"  [done] computed kept_from_season for {len(updates)} keepers")

    def sync_standings(self):
        """Re-fetch standings for all seasons to update finish/playoff_seed.

        Only updates team table (finish, playoff_seed) — does NOT touch league metadata.
        Safer than full metadata re-sync for historical seasons.
        """
        for season in sorted(self.franchise.seasons):
            league_key = self.franchise.league_key_for_season(season)
            if not league_key:
                continue

            query = self.client.query_for_franchise(self.franchise.slug, season)
            try:
                standings = query.get_league_standings()
                self._wait()

                updated = 0
                with self.db.transaction():
                    for team in standings.teams:
                        ts = getattr(team, "team_standings", None)
                        if not ts:
                            continue
                        rank_val = getattr(ts, "rank", None)
                        seed_val = getattr(ts, "playoff_seed", None)
                        finish = int(rank_val) if rank_val is not None else None
                        playoff_seed = int(seed_val) if seed_val is not None else None

                        self.db.execute(
                            "UPDATE team SET finish=?, playoff_seed=? "
                            "WHERE league_key=? AND team_key=?",
                            (finish, playoff_seed, league_key, team.team_key),
                        )
                        updated += 1

                print(f"  [done] {season} standings: {updated} teams updated")

            except Exception as e:
                print(f"  [warn] {season} standings failed: {e}")

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

        # Sync completed weeks (current_week is the in-progress week per Yahoo)
        end = league_row["end_week"] if league_row["is_finished"] else current_week - 1
        for week in range(1, end + 1):
            self.sync_week(query, league_key, week, num_teams, stat_categories)

        # Re-sync transactions (always refresh)
        self.db.execute(
            "DELETE FROM sync_log WHERE league_key=? AND sync_type='transactions'",
            (league_key,),
        )
        self.sync_transactions(query, league_key)

        # Backfill transaction weeks + repair league week metadata
        self._backfill_transaction_weeks(league_key)
        self._repair_league_weeks(league_key)

        print("Incremental sync complete.\n")

    def close(self):
        self.db.close()
