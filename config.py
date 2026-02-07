"""Central configuration: franchise and manager loading from franchises.yaml."""

from pathlib import Path

import yaml

_PROJECT_DIR = Path(__file__).resolve().parent
_FRANCHISES_FILE = _PROJECT_DIR / "franchises.yaml"


def _load_raw() -> dict:
    """Load raw franchise definitions from franchises.yaml."""
    if not _FRANCHISES_FILE.exists():
        return {}
    with open(_FRANCHISES_FILE) as f:
        return yaml.safe_load(f) or {}


class Franchise:
    """A fantasy league that spans multiple seasons."""

    def __init__(self, sport: str, data: dict):
        self.sport = sport
        self.name = data["name"]
        self.slug = data["slug"]
        self.is_default = data.get("default", False)
        self.seasons: dict[int, str] = {
            int(k): v for k, v in data.get("seasons", {}).items()
        }
        # Manager config: guid -> {name, short_name}
        self._managers: dict[str, dict] = data.get("managers", {}) or {}
        self._former_managers: dict[str, dict] = data.get("former_managers", {}) or {}

    @property
    def latest_season(self) -> int:
        return max(self.seasons) if self.seasons else 0

    @property
    def latest_league_key(self) -> str:
        return self.seasons.get(self.latest_season, "")

    def league_key_for_season(self, season: int) -> str | None:
        return self.seasons.get(season)

    def manager_name(self, guid: str) -> str | None:
        """Look up a manager's display name by GUID. Checks active then former."""
        mgr = self._managers.get(guid) or self._former_managers.get(guid)
        return mgr["name"] if mgr else None

    def manager_short_name(self, guid: str) -> str | None:
        """Look up a manager's short name by GUID."""
        mgr = self._managers.get(guid) or self._former_managers.get(guid)
        return mgr.get("short_name") if mgr else None

    @property
    def all_managers(self) -> dict[str, dict]:
        """All managers (active + former)."""
        return {**self._managers, **self._former_managers}


def get_franchises() -> dict[str, list[Franchise]]:
    """Load all franchises grouped by sport."""
    raw = _load_raw()
    result = {}
    for sport, franchise_list in raw.items():
        result[sport] = [Franchise(sport, f) for f in franchise_list]
    return result


def get_default_franchise(sport: str) -> Franchise | None:
    """Get the default franchise for a sport."""
    franchises = get_franchises()
    for f in franchises.get(sport, []):
        if f.is_default:
            return f
    sport_franchises = franchises.get(sport, [])
    return sport_franchises[0] if sport_franchises else None


def get_franchise_by_slug(slug: str) -> Franchise | None:
    """Find a franchise by its slug across all sports."""
    for franchise_list in get_franchises().values():
        for f in franchise_list:
            if f.slug == slug:
                return f
    return None
