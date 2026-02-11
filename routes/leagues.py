"""League and franchise endpoints."""

from fastapi import APIRouter, HTTPException
from config import get_franchises, get_franchise_by_slug
from db import Database
from db.queries import get_league, get_latest_league, get_league_week_info, get_all_seasons

router = APIRouter(prefix="/api")


def resolve_league(slug: str, db: Database, season: int | None) -> str:
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


def resolve_week(db: Database, league_key: str, week: int | None) -> int:
    """Resolve week number, defaulting to latest completed week."""
    if week:
        return week
    row = get_league_week_info(db, league_key)
    if not row:
        raise HTTPException(status_code=404, detail="League not found in DB")
    return row["end_week"] if row["is_finished"] else max(row["current_week"] - 1, 1)


@router.get("/franchises")
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


@router.get("/{slug}/seasons")
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
