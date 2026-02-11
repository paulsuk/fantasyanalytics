"""Z-score based player value calculator and weekly awards."""

from dataclasses import dataclass
from db import Database
from db.queries import (
    get_scoring_categories,
    get_weekly_roster_stats,
    get_category_leaders,
    get_add_transactions,
    get_player_weekly_stats_sum,
)


@dataclass
class PlayerRank:
    """A player with their composite z-score and per-category values."""
    player_key: str
    name: str
    team_key: str
    team_name: str
    manager: str
    position: str
    z_total: float
    stat_line: dict[str, float]  # {display_name: raw_value}
    z_scores: dict[str, float]   # {display_name: z_score}


class PlayerValue:
    """Compute z-score player rankings from Yahoo weekly stats.

    For MLB, splits into batter and pitcher pools using stat_category.position_type.
    For NBA, all stats are in one pool.
    """

    def __init__(self, db: Database, league_key: str):
        self.db = db
        self.league_key = league_key
        self._categories = None

    @property
    def categories(self) -> list[dict]:
        """Scoring categories for this league (cached)."""
        if self._categories is None:
            self._categories = get_scoring_categories(self.db, self.league_key)
        return self._categories

    def _batting_cats(self) -> list[dict]:
        return [c for c in self.categories if c["position_type"] == "B"]

    def _pitching_cats(self) -> list[dict]:
        return [c for c in self.categories if c["position_type"] == "P"]

    def _compute_rankings(self, week: int, cats: list[dict],
                          limit: int | None = 10) -> list[PlayerRank]:
        """Compute z-score rankings for a set of categories over one week.

        Only includes players who were starters (not bench) and have at least
        one non-zero stat value in the target categories.
        """
        cat_ids = [c["stat_id"] for c in cats]
        cat_map = {c["stat_id"]: c for c in cats}

        rows = get_weekly_roster_stats(self.db, self.league_key, week, cat_ids)

        # Group by player_key
        player_stats = {}
        for row in rows:
            pkey = row["player_key"]
            if pkey not in player_stats:
                player_stats[pkey] = {"info": row, "vals": {}}
            if row["stat_id"] is not None:
                val = float(row["value"]) if row["value"] else 0.0
                player_stats[pkey]["vals"][row["stat_id"]] = val

        # Filter out players with all zeros in target categories
        player_stats = {
            pkey: ps for pkey, ps in player_stats.items()
            if any(ps["vals"].get(cid, 0) != 0 for cid in cat_ids)
        }

        if not player_stats:
            return []

        # Compute mean and stdev per category across all qualifying players
        from statistics import mean, stdev

        cat_stats = {}
        for cid in cat_ids:
            values = [ps["vals"].get(cid, 0.0) for ps in player_stats.values()]
            m = mean(values) if values else 0.0
            sd = stdev(values) if len(values) > 1 else 1.0
            cat_stats[cid] = {"mean": m, "stdev": sd if sd > 0 else 1.0}

        # Compute z-scores per player
        results = []
        for pkey, ps in player_stats.items():
            info = ps["info"]
            z_scores = {}
            stat_line = {}
            z_total = 0.0

            for cid in cat_ids:
                cat = cat_map[cid]
                raw = ps["vals"].get(cid, 0.0)
                z = (raw - cat_stats[cid]["mean"]) / cat_stats[cid]["stdev"]

                # Flip sign for "lower is better" (ERA, WHIP, TO)
                if cat["sort_order"] == 0:
                    z = -z

                display = cat["display_name"]
                z_scores[display] = round(z, 2)
                stat_line[display] = raw
                z_total += z

            results.append(PlayerRank(
                player_key=pkey,
                name=info["full_name"],
                team_key=info["team_key"],
                team_name=info["team_name"],
                manager=info["manager_name"] or "",
                position=info["selected_position"],
                z_total=round(z_total, 2),
                stat_line=stat_line,
                z_scores=z_scores,
            ))

        results.sort(key=lambda r: r.z_total, reverse=True)
        return results[:limit] if limit is not None else results

    def top_batters(self, week: int, limit: int = 10) -> list[PlayerRank]:
        """Top batters for a week by z-score over batting categories."""
        cats = self._batting_cats()
        if not cats:
            # NBA or no position_type — use all categories
            cats = self.categories
        return self._compute_rankings(week, cats, limit)

    def top_pitchers(self, week: int, limit: int = 10) -> list[PlayerRank]:
        """Top pitchers for a week by z-score over pitching categories."""
        cats = self._pitching_cats()
        if not cats:
            return []
        return self._compute_rankings(week, cats, limit)

    def top_players(self, week: int, limit: int = 10) -> list[PlayerRank]:
        """Top players across all scoring categories (for NBA or combined)."""
        return self._compute_rankings(week, self.categories, limit)

    def category_leaders(self, week: int, stat_id: int,
                         limit: int = 10) -> list[dict]:
        """Top players in a specific stat category for a week."""
        cat = next((c for c in self.categories if c["stat_id"] == stat_id), None)
        if not cat:
            return []

        order = "DESC" if cat["sort_order"] == 1 else "ASC"
        rows = get_category_leaders(
            self.db, self.league_key, week, stat_id, order, limit
        )
        return [
            {
                "player_key": r["player_key"],
                "name": r["full_name"],
                "team_name": r["team_name"],
                "manager": r["manager_name"] or "",
                "value": float(r["value"]) if r["value"] else 0.0,
            }
            for r in rows
        ]

    def best_pickups(self, since_week: int, limit: int = 10) -> list[dict]:
        """Best free agent pickups since a given week, ranked by z-score.

        Finds players added via transaction after since_week, then computes
        their cumulative z-score value from the week they were added.
        """
        adds = get_add_transactions(self.db, self.league_key)

        if not adds:
            return []

        # For each pickup, sum their stat values across weeks since pickup
        cat_ids = [c["stat_id"] for c in self.categories]
        results = []
        for add in adds:
            add_week = add["add_week"]
            if add_week is None or add_week < since_week:
                continue

            stats = get_player_weekly_stats_sum(
                self.db, self.league_key, add["player_key"], add_week, cat_ids
            )
            if not stats:
                continue

            total_val = sum(float(s["total"]) for s in stats if s["total"])
            results.append({
                "player_key": add["player_key"],
                "name": add["full_name"],
                "team_name": add["team_name"] or "",
                "manager": add["manager_name"] or "",
                "add_week": add_week,
                "total_value": round(total_val, 1),
            })

        results.sort(key=lambda r: r["total_value"], reverse=True)
        return results[:limit]
