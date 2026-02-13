"""Analytics endpoints: recap, teams, managers, records, playoffs."""

from dataclasses import asdict

from fastapi import APIRouter, HTTPException, Query
from config import get_franchise_by_slug
from db import Database
from db.queries import get_playoff_bracket
from analytics.recap import RecapAssembler
from analytics.teams import TeamProfiler
from analytics.history import ManagerHistory, LeagueRecords
from routes.leagues import resolve_league, resolve_week

router = APIRouter(prefix="/api")


@router.get("/{slug}/recap")
def recap(
    slug: str,
    week: int | None = Query(default=None),
    season: int | None = Query(default=None),
):
    """Get weekly recap data."""
    db = Database(slug)
    try:
        league_key = resolve_league(slug, db, season)
        week = resolve_week(db, league_key, week)

        assembler = RecapAssembler(db, league_key)
        recap_data = assembler.build(week)

        return {
            "league_key": recap_data.league_key,
            "league_name": recap_data.league_name,
            "season": recap_data.season,
            "week": recap_data.week,
            "week_start": recap_data.week_start,
            "week_end": recap_data.week_end,
            "matchups": [
                {
                    "team_1_name": m.team_1_name,
                    "team_1_manager": m.team_1_manager,
                    "team_2_name": m.team_2_name,
                    "team_2_manager": m.team_2_manager,
                    "cats_won_1": m.cats_won_1,
                    "cats_won_2": m.cats_won_2,
                    "cats_tied": m.cats_tied,
                    "winner_name": m.winner_name,
                    "is_playoffs": m.is_playoffs,
                    "is_consolation": m.is_consolation,
                    "categories": m.categories,
                }
                for m in recap_data.matchups
            ],
            "batter_of_week": asdict(recap_data.batter_of_week) if recap_data.batter_of_week else None,
            "pitcher_of_week": asdict(recap_data.pitcher_of_week) if recap_data.pitcher_of_week else None,
            "player_of_week": asdict(recap_data.player_of_week) if recap_data.player_of_week else None,
            "standings": recap_data.standings,
            "profiles": [
                {
                    "team_name": p.team_name,
                    "manager": p.manager,
                    "wins": p.wins,
                    "losses": p.losses,
                    "ties": p.ties,
                    "rank": p.rank,
                    "prev_rank": p.prev_rank,
                    "streak": p.streak,
                    "last_3": p.last_3,
                    "cat_strengths": p.cat_strengths,
                    "cat_weaknesses": p.cat_weaknesses,
                    "mvp_name": p.mvp_name,
                    "mvp_z": p.mvp_z,
                }
                for p in recap_data.profiles
            ],
        }
    finally:
        db.close()


@router.get("/{slug}/teams")
def teams(
    slug: str,
    week: int | None = Query(default=None),
    season: int | None = Query(default=None),
):
    """Get power rankings / team profiles."""
    db = Database(slug)
    try:
        league_key = resolve_league(slug, db, season)
        week = resolve_week(db, league_key, week)

        profiler = TeamProfiler(db, league_key)
        profiles = profiler.build_profiles(week)

        return {
            "league_key": league_key,
            "week": week,
            "profiles": [
                {
                    "team_key": p.team_key,
                    "team_name": p.team_name,
                    "manager": p.manager,
                    "wins": p.wins,
                    "losses": p.losses,
                    "ties": p.ties,
                    "rank": p.rank,
                    "prev_rank": p.prev_rank,
                    "streak": p.streak,
                    "last_3": p.last_3,
                    "cat_strengths": p.cat_strengths,
                    "cat_weaknesses": p.cat_weaknesses,
                    "mvp_name": p.mvp_name,
                    "mvp_z": p.mvp_z,
                    "opponent_name": p.opponent_name,
                    "h2h_record": p.h2h_record,
                }
                for p in profiles
            ],
        }
    finally:
        db.close()


@router.get("/{slug}/managers")
def managers(slug: str):
    """Get all managers with cross-season records and H2H matrix."""
    franchise = get_franchise_by_slug(slug)
    if not franchise:
        raise HTTPException(status_code=404, detail=f"Unknown franchise: {slug}")

    db = Database(slug)
    try:
        history = ManagerHistory(db, franchise)
        result = {
            "managers": history.managers(),
            "h2h": history.h2h_matrix(),
        }
        if franchise.has_franchises:
            result["franchises"] = franchise.franchise_list()
            result["franchise_h2h"] = history.franchise_h2h_matrix()
            result["franchise_stats"] = history.franchise_stats()
        return result
    finally:
        db.close()


@router.get("/{slug}/records")
def records(
    slug: str,
    include_playoffs: bool = Query(default=False),
):
    """Get all-time league records."""
    franchise = get_franchise_by_slug(slug)
    if not franchise:
        raise HTTPException(status_code=404, detail=f"Unknown franchise: {slug}")

    db = Database(slug)
    try:
        lr = LeagueRecords(db, include_playoffs=include_playoffs)
        return lr.records()
    finally:
        db.close()


@router.get("/{slug}/playoffs")
def playoffs(
    slug: str,
    season: int | None = Query(default=None),
):
    """Get playoff bracket for a season."""
    db = Database(slug)
    try:
        league_key = resolve_league(slug, db, season)
        rows = get_playoff_bracket(db, league_key)

        # Group by week and bracket type
        rounds: list[dict] = []
        current_week = None
        current_round: dict | None = None

        for r in rows:
            if r["week"] != current_week:
                current_week = r["week"]
                current_round = {"week": current_week, "matchups": [], "consolation": []}
                rounds.append(current_round)

            matchup = {
                "team_1_name": r["team_name_1"],
                "team_1_manager": r["manager_1"],
                "team_1_seed": r["seed_1"],
                "team_2_name": r["team_name_2"],
                "team_2_manager": r["manager_2"],
                "team_2_seed": r["seed_2"],
                "cats_won_1": r["cats_won_1"],
                "cats_won_2": r["cats_won_2"],
                "cats_tied": r["cats_tied"],
                "winner": r["team_name_1"] if r["winner_team_key"] == r["team_key_1"]
                    else (r["team_name_2"] if r["winner_team_key"] == r["team_key_2"] else None),
                "is_tied": bool(r["is_tied"]),
            }

            if r["is_consolation"]:
                current_round["consolation"].append(matchup)
            else:
                current_round["matchups"].append(matchup)

        return {"league_key": league_key, "rounds": rounds}
    finally:
        db.close()
