"""FastAPI server for fantasy analytics API."""

from dataclasses import asdict

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from config import get_franchises, get_franchise_by_slug
from db import Database
from db.queries import get_league, get_latest_league, get_league_week_info, get_all_seasons
from analytics.recap import RecapAssembler
from analytics.teams import TeamProfiler
from analytics.value import PlayerValue
from analytics.history import ManagerHistory, LeagueRecords

app = FastAPI(title="Fantasy Analytics API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:5173",
        "https://paulsuk.github.io",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _resolve_league(slug: str, db: Database, season: int | None) -> str:
    """Resolve a league key for a franchise slug and optional season."""
    franchise = get_franchise_by_slug(slug)
    if not franchise:
        raise HTTPException(status_code=404, detail=f"Unknown franchise: {slug}")

    if season:
        league_key = franchise.league_key_for_season(season)
        if not league_key:
            raise HTTPException(status_code=404, detail=f"No league key for season {season}")
        if not get_league(db, league_key):
            raise HTTPException(status_code=404, detail=f"No synced data for {slug} season {season}")
        return league_key

    row = get_latest_league(db)
    if not row:
        raise HTTPException(status_code=404, detail=f"No synced data for {slug}")
    return row["league_key"]


def _resolve_week(db: Database, league_key: str, week: int | None) -> int:
    """Resolve week number, defaulting to latest completed week."""
    if week:
        return week
    row = get_league_week_info(db, league_key)
    if not row:
        raise HTTPException(status_code=404, detail="League not found in DB")
    return row["end_week"] if row["is_finished"] else max(row["current_week"] - 1, 1)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/franchises")
def list_franchises():
    """List all configured franchises."""
    franchises = get_franchises()
    result = []
    for sport, franchise_list in franchises.items():
        for f in franchise_list:
            result.append({
                "sport": sport,
                "name": f.name,
                "slug": f.slug,
                "is_default": f.is_default,
                "seasons": {int(k): v for k, v in sorted(f.seasons.items(), reverse=True)},
                "latest_season": f.latest_season,
            })
    return result


@app.get("/api/{slug}/seasons")
def franchise_seasons(slug: str):
    """List synced seasons for a franchise."""
    franchise = get_franchise_by_slug(slug)
    if not franchise:
        raise HTTPException(status_code=404, detail=f"Unknown franchise: {slug}")

    db = Database(slug)
    try:
        return get_all_seasons(db)
    finally:
        db.close()


@app.get("/api/{slug}/recap")
def recap(
    slug: str,
    week: int | None = Query(default=None),
    season: int | None = Query(default=None),
):
    """Get weekly recap data."""
    db = Database(slug)
    try:
        league_key = _resolve_league(slug, db, season)
        week = _resolve_week(db, league_key, week)

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


@app.get("/api/{slug}/teams")
def teams(
    slug: str,
    week: int | None = Query(default=None),
    season: int | None = Query(default=None),
):
    """Get power rankings / team profiles."""
    db = Database(slug)
    try:
        league_key = _resolve_league(slug, db, season)
        week = _resolve_week(db, league_key, week)

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


@app.get("/api/{slug}/managers")
def managers(slug: str):
    """Get all managers with cross-season records and H2H matrix."""
    franchise = get_franchise_by_slug(slug)
    if not franchise:
        raise HTTPException(status_code=404, detail=f"Unknown franchise: {slug}")

    db = Database(slug)
    try:
        history = ManagerHistory(db, franchise)
        return {
            "managers": history.managers(),
            "h2h": history.h2h_matrix(),
        }
    finally:
        db.close()


@app.get("/api/{slug}/records")
def records(slug: str):
    """Get all-time league records."""
    franchise = get_franchise_by_slug(slug)
    if not franchise:
        raise HTTPException(status_code=404, detail=f"Unknown franchise: {slug}")

    db = Database(slug)
    try:
        lr = LeagueRecords(db)
        return lr.records()
    finally:
        db.close()
