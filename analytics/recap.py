"""Weekly recap data assembler — structures all data needed for a recap."""

from dataclasses import dataclass, field
from db import Database
from analytics.value import PlayerValue, PlayerRank
from analytics.teams import TeamProfiler, TeamProfile


@dataclass
class MatchupSummary:
    """One matchup result with category detail."""
    team_1_name: str
    team_1_manager: str
    team_2_name: str
    team_2_manager: str
    cats_won_1: int
    cats_won_2: int
    cats_tied: int
    winner_name: str
    is_playoffs: bool
    is_consolation: bool
    # Per-category breakdown
    categories: list[dict] = field(default_factory=list)
    # [{display_name, team_1_value, team_2_value, winner: 1|2|None}]


@dataclass
class WeeklyRecap:
    """All data assembled for one week's recap."""
    league_key: str
    league_name: str
    season: int
    week: int
    week_start: str
    week_end: str
    # Matchups
    matchups: list[MatchupSummary] = field(default_factory=list)
    # Awards
    batter_of_week: PlayerRank | None = None
    pitcher_of_week: PlayerRank | None = None
    player_of_week: PlayerRank | None = None  # for NBA
    # Standings
    standings: list[dict] = field(default_factory=list)
    # Team profiles (for power rankings)
    profiles: list[TeamProfile] = field(default_factory=list)
    # Transactions this week
    transactions: list[dict] = field(default_factory=list)


class RecapAssembler:
    """Assembles all data for a weekly recap from the database."""

    def __init__(self, db: Database, league_key: str):
        self.db = db
        self.league_key = league_key

    def _league_info(self) -> dict:
        row = self.db.fetchone(
            "SELECT * FROM league WHERE league_key=?", (self.league_key,)
        )
        return dict(row) if row else {}

    def _is_mlb(self) -> bool:
        """Check if this league has batter/pitcher position types."""
        row = self.db.fetchone(
            "SELECT COUNT(*) as n FROM stat_category "
            "WHERE league_key=? AND position_type='P' AND is_scoring_stat=1",
            (self.league_key,),
        )
        return row["n"] > 0 if row else False

    def _build_matchups(self, week: int) -> list[MatchupSummary]:
        matchups = self.db.fetchall(
            "SELECT * FROM matchup WHERE league_key=? AND week=? ORDER BY matchup_id",
            (self.league_key, week),
        )

        results = []
        for m in matchups:
            t1 = self.db.fetchone(
                "SELECT name, manager_name FROM team WHERE league_key=? AND team_key=?",
                (self.league_key, m["team_key_1"]),
            )
            t2 = self.db.fetchone(
                "SELECT name, manager_name FROM team WHERE league_key=? AND team_key=?",
                (self.league_key, m["team_key_2"]),
            )
            winner = self.db.fetchone(
                "SELECT name FROM team WHERE league_key=? AND team_key=?",
                (self.league_key, m["winner_team_key"]),
            ) if m["winner_team_key"] else None

            # Per-category details
            cats = self.db.fetchall(
                "SELECT mc.stat_id, sc.display_name, mc.team_1_value, "
                "       mc.team_2_value, mc.winner_team_key "
                "FROM matchup_category mc "
                "JOIN stat_category sc ON mc.league_key=sc.league_key AND mc.stat_id=sc.stat_id "
                "WHERE mc.league_key=? AND mc.week=? AND mc.matchup_id=? "
                "    AND sc.is_scoring_stat=1 "
                "ORDER BY sc.position_type, mc.stat_id",
                (self.league_key, week, m["matchup_id"]),
            )

            cat_details = []
            for c in cats:
                if c["winner_team_key"] == m["team_key_1"]:
                    w = 1
                elif c["winner_team_key"] == m["team_key_2"]:
                    w = 2
                else:
                    w = None
                cat_details.append({
                    "display_name": c["display_name"],
                    "team_1_value": c["team_1_value"],
                    "team_2_value": c["team_2_value"],
                    "winner": w,
                })

            results.append(MatchupSummary(
                team_1_name=t1["name"] if t1 else "",
                team_1_manager=t1["manager_name"] or "" if t1 else "",
                team_2_name=t2["name"] if t2 else "",
                team_2_manager=t2["manager_name"] or "" if t2 else "",
                cats_won_1=m["cats_won_1"],
                cats_won_2=m["cats_won_2"],
                cats_tied=m["cats_tied"],
                winner_name=winner["name"] if winner else "Tie",
                is_playoffs=bool(m["is_playoffs"]),
                is_consolation=bool(m["is_consolation"]),
                categories=cat_details,
            ))

        return results

    def _week_transactions(self, week: int) -> list[dict]:
        """Transactions that occurred during this week (by matchup dates)."""
        m = self.db.fetchone(
            "SELECT week_start, week_end FROM matchup WHERE league_key=? AND week=? LIMIT 1",
            (self.league_key, week),
        )
        if not m:
            return []

        rows = self.db.fetchall(
            "SELECT tr.type, tr.timestamp, tr.faab_bid, "
            "       tp.player_key, tp.type as player_type, "
            "       tp.destination_team_key, tp.source_team_key, "
            "       p.full_name, "
            "       dt.name as dest_team_name, dt.manager_name as dest_manager, "
            "       st.name as src_team_name "
            "FROM transaction_record tr "
            "JOIN transaction_player tp ON tr.transaction_key=tp.transaction_key "
            "JOIN player p ON tp.player_key=p.player_key "
            "LEFT JOIN team dt ON tr.league_key=dt.league_key "
            "    AND tp.destination_team_key=dt.team_key "
            "LEFT JOIN team st ON tr.league_key=st.league_key "
            "    AND tp.source_team_key=st.team_key "
            "WHERE tr.league_key=? AND tr.timestamp >= ? AND tr.timestamp <= ? "
            "ORDER BY tr.timestamp",
            (self.league_key, m["week_start"], m["week_end"] + "T23:59:59"),
        )

        return [dict(r) for r in rows]

    def build(self, week: int) -> WeeklyRecap:
        """Assemble a complete weekly recap."""
        league = self._league_info()
        is_mlb = self._is_mlb()

        recap = WeeklyRecap(
            league_key=self.league_key,
            league_name=league.get("name", ""),
            season=league.get("season", 0),
            week=week,
            week_start="",
            week_end="",
        )

        # Week dates from first matchup
        m = self.db.fetchone(
            "SELECT week_start, week_end FROM matchup WHERE league_key=? AND week=? LIMIT 1",
            (self.league_key, week),
        )
        if m:
            recap.week_start = m["week_start"]
            recap.week_end = m["week_end"]

        # Matchups
        recap.matchups = self._build_matchups(week)

        # Awards
        pv = PlayerValue(self.db, self.league_key)
        if is_mlb:
            batters = pv.top_batters(week, limit=1)
            pitchers = pv.top_pitchers(week, limit=1)
            recap.batter_of_week = batters[0] if batters else None
            recap.pitcher_of_week = pitchers[0] if pitchers else None
        else:
            players = pv.top_players(week, limit=1)
            recap.player_of_week = players[0] if players else None

        # Standings
        profiler = TeamProfiler(self.db, self.league_key)
        recap.standings = profiler.standings(week)

        # Team profiles
        recap.profiles = profiler.build_profiles(week)

        # Transactions
        recap.transactions = self._week_transactions(week)

        return recap
