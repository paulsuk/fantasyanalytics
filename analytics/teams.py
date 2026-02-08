"""Team profile assembly for power rankings."""

from dataclasses import dataclass, field
from db import Database
from analytics.value import PlayerValue


@dataclass
class TeamProfile:
    """All data needed for a power rankings blurb about one team."""
    team_key: str
    team_name: str
    manager: str
    # Record
    wins: int = 0
    losses: int = 0
    ties: int = 0
    rank: int = 0
    prev_rank: int = 0  # rank from previous week
    # Recent form
    streak: int = 0          # positive = win streak, negative = loss streak
    last_3: list[str] = field(default_factory=list)  # ["W", "L", "W"]
    # Category analysis
    cat_strengths: list[str] = field(default_factory=list)  # top 3 categories
    cat_weaknesses: list[str] = field(default_factory=list)  # bottom 3 categories
    # MVP / Disappointment
    mvp_name: str = ""
    mvp_z: float = 0.0
    mvp_line: dict = field(default_factory=dict)
    disappointment_name: str = ""
    disappointment_z: float = 0.0
    disappointment_line: dict = field(default_factory=dict)
    # Recent transactions
    recent_adds: list[str] = field(default_factory=list)
    recent_drops: list[str] = field(default_factory=list)
    # This week's matchup
    opponent_key: str = ""
    opponent_name: str = ""
    h2h_record: str = ""  # all-time H2H vs this opponent


class TeamProfiler:
    """Builds team profiles from synced data for a given week."""

    def __init__(self, db: Database, league_key: str):
        self.db = db
        self.league_key = league_key

    def standings(self, through_week: int) -> list[dict]:
        """Compute standings through a given week from matchup results.

        Returns list of {team_key, team_name, manager, wins, losses, ties, rank}
        sorted by wins desc, then losses asc.
        """
        teams = self.db.fetchall(
            "SELECT team_key, name, manager_name FROM team WHERE league_key=?",
            (self.league_key,),
        )

        records = {}
        for t in teams:
            records[t["team_key"]] = {
                "team_key": t["team_key"],
                "team_name": t["name"],
                "manager": t["manager_name"] or "",
                "wins": 0, "losses": 0, "ties": 0,
            }

        matchups = self.db.fetchall(
            "SELECT team_key_1, team_key_2, cats_won_1, cats_won_2, "
            "       cats_tied, winner_team_key, is_tied "
            "FROM matchup WHERE league_key=? AND week<=?",
            (self.league_key, through_week),
        )

        for m in matchups:
            tk1, tk2 = m["team_key_1"], m["team_key_2"]
            if tk1 not in records or tk2 not in records:
                continue
            if m["is_tied"]:
                records[tk1]["ties"] += 1
                records[tk2]["ties"] += 1
            elif m["winner_team_key"] == tk1:
                records[tk1]["wins"] += 1
                records[tk2]["losses"] += 1
            elif m["winner_team_key"] == tk2:
                records[tk2]["wins"] += 1
                records[tk1]["losses"] += 1

        ranked = sorted(
            records.values(),
            key=lambda r: (r["wins"], -r["losses"]),
            reverse=True,
        )
        for i, r in enumerate(ranked):
            r["rank"] = i + 1

        return ranked

    def _team_category_record(self, team_key: str, through_week: int) -> dict[str, dict]:
        """Per-category W-L record for a team (how many weeks they won each cat)."""
        rows = self.db.fetchall(
            "SELECT mc.stat_id, sc.display_name, mc.winner_team_key "
            "FROM matchup_category mc "
            "JOIN matchup m ON mc.league_key=m.league_key AND mc.week=m.week "
            "    AND mc.matchup_id=m.matchup_id "
            "JOIN stat_category sc ON mc.league_key=sc.league_key AND mc.stat_id=sc.stat_id "
            "WHERE mc.league_key=? AND mc.week<=? "
            "    AND (m.team_key_1=? OR m.team_key_2=?) "
            "    AND sc.is_scoring_stat=1",
            (self.league_key, through_week, team_key, team_key),
        )

        cats = {}
        for r in rows:
            dn = r["display_name"]
            if dn not in cats:
                cats[dn] = {"wins": 0, "losses": 0, "ties": 0}
            if r["winner_team_key"] == team_key:
                cats[dn]["wins"] += 1
            elif r["winner_team_key"] is None:
                cats[dn]["ties"] += 1
            else:
                cats[dn]["losses"] += 1

        return cats

    def _recent_form(self, team_key: str, through_week: int, n: int = 3) -> tuple[list[str], int]:
        """Last N matchup results and current streak."""
        matchups = self.db.fetchall(
            "SELECT week, winner_team_key, is_tied FROM matchup "
            "WHERE league_key=? AND week<=? AND (team_key_1=? OR team_key_2=?) "
            "ORDER BY week DESC",
            (self.league_key, through_week, team_key, team_key),
        )

        results = []
        for m in matchups:
            if m["is_tied"]:
                results.append("T")
            elif m["winner_team_key"] == team_key:
                results.append("W")
            else:
                results.append("L")

        last_n = results[:n]

        # Compute streak
        streak = 0
        if results:
            streak_type = results[0]
            for r in results:
                if r == streak_type:
                    streak += 1
                else:
                    break
            if streak_type == "L":
                streak = -streak
            elif streak_type == "T":
                streak = 0

        return last_n, streak

    def _h2h_record(self, team_key: str, opponent_key: str) -> str:
        """All-time H2H record vs an opponent across all synced seasons."""
        matchups = self.db.fetchall(
            "SELECT winner_team_key, is_tied FROM matchup "
            "WHERE league_key IN (SELECT league_key FROM league) "
            "    AND ((team_key_1=? AND team_key_2=?) OR (team_key_1=? AND team_key_2=?))",
            (team_key, opponent_key, opponent_key, team_key),
        )
        w, l, t = 0, 0, 0
        for m in matchups:
            if m["is_tied"]:
                t += 1
            elif m["winner_team_key"] == team_key:
                w += 1
            else:
                l += 1
        return f"{w}-{l}-{t}" if t else f"{w}-{l}"

    def _recent_transactions(self, team_key: str, last_n: int = 5) -> tuple[list[str], list[str]]:
        """Recent adds and drops for a team."""
        adds = self.db.fetchall(
            "SELECT p.full_name FROM transaction_player tp "
            "JOIN transaction_record tr ON tp.transaction_key=tr.transaction_key "
            "JOIN player p ON tp.player_key=p.player_key "
            "WHERE tr.league_key=? AND tp.destination_team_key=? AND tp.type='add' "
            "ORDER BY tr.timestamp DESC LIMIT ?",
            (self.league_key, team_key, last_n),
        )
        drops = self.db.fetchall(
            "SELECT p.full_name FROM transaction_player tp "
            "JOIN transaction_record tr ON tp.transaction_key=tr.transaction_key "
            "JOIN player p ON tp.player_key=p.player_key "
            "WHERE tr.league_key=? AND tp.source_team_key=? AND tp.type='drop' "
            "ORDER BY tr.timestamp DESC LIMIT ?",
            (self.league_key, team_key, last_n),
        )
        return (
            [r["full_name"] for r in adds],
            [r["full_name"] for r in drops],
        )

    def build_profiles(self, week: int) -> list[TeamProfile]:
        """Build full team profiles for power rankings as of a given week."""
        current_standings = self.standings(week)
        prev_standings = self.standings(week - 1) if week > 1 else []
        prev_rank_map = {s["team_key"]: s["rank"] for s in prev_standings}

        # Get current week's matchups for opponent info
        matchup_map = {}
        matchups = self.db.fetchall(
            "SELECT team_key_1, team_key_2 FROM matchup "
            "WHERE league_key=? AND week=?",
            (self.league_key, week),
        )
        for m in matchups:
            matchup_map[m["team_key_1"]] = m["team_key_2"]
            matchup_map[m["team_key_2"]] = m["team_key_1"]

        # Player value for MVP/disappointment — compute ONCE for all teams
        pv = PlayerValue(self.db, self.league_key)
        all_players = pv._compute_rankings(week, pv.categories, limit=None)
        from collections import defaultdict
        players_by_team: dict[str, list] = defaultdict(list)
        for p in all_players:
            players_by_team[p.team_key].append(p)

        profiles = []
        for s in current_standings:
            tk = s["team_key"]
            profile = TeamProfile(
                team_key=tk,
                team_name=s["team_name"],
                manager=s["manager"],
                wins=s["wins"],
                losses=s["losses"],
                ties=s["ties"],
                rank=s["rank"],
                prev_rank=prev_rank_map.get(tk, s["rank"]),
            )

            # Recent form
            profile.last_3, profile.streak = self._recent_form(tk, week)

            # Category strengths/weaknesses
            cat_record = self._team_category_record(tk, week)
            if cat_record:
                sorted_cats = sorted(
                    cat_record.items(),
                    key=lambda x: x[1]["wins"] / max(x[1]["wins"] + x[1]["losses"], 1),
                    reverse=True,
                )
                profile.cat_strengths = [c[0] for c in sorted_cats[:3]]
                profile.cat_weaknesses = [c[0] for c in sorted_cats[-3:]]

            # MVP / Disappointment (best and worst z-score player this week)
            team_roster = players_by_team.get(tk, [])
            if team_roster:
                mvp = team_roster[0]
                profile.mvp_name = mvp.name
                profile.mvp_z = mvp.z_total
                profile.mvp_line = mvp.stat_line

                worst = team_roster[-1]
                profile.disappointment_name = worst.name
                profile.disappointment_z = worst.z_total
                profile.disappointment_line = worst.stat_line

            # Recent transactions
            profile.recent_adds, profile.recent_drops = self._recent_transactions(tk)

            # Opponent info
            opp_key = matchup_map.get(tk)
            if opp_key:
                profile.opponent_key = opp_key
                opp = self.db.fetchone(
                    "SELECT name, manager_name FROM team WHERE league_key=? AND team_key=?",
                    (self.league_key, opp_key),
                )
                if opp:
                    profile.opponent_name = opp["name"]
                profile.h2h_record = self._h2h_record(tk, opp_key)

            profiles.append(profile)

        return profiles
