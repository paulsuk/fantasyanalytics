"""Franchise and manager loading from franchises.yaml."""

from pathlib import Path

import yaml

_FRANCHISES_FILE = Path(__file__).resolve().parent / "franchises.yaml"


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
        # Franchise definitions: list of {name, managers: [{guid, from, to?}]}
        self._franchise_defs: list[dict] = data.get("franchises", []) or []

    @property
    def latest_season(self) -> int:
        return max(self.seasons) if self.seasons else 0

    @property
    def min_season(self) -> int:
        """Earliest season configured for this franchise."""
        return min(self.seasons) if self.seasons else 0

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
    def current_manager_guids(self) -> set[str]:
        """GUIDs of currently active managers."""
        return set(self._managers.keys())

    @property
    def all_managers(self) -> dict[str, dict]:
        """All managers (active + former)."""
        return {**self._managers, **self._former_managers}

    @property
    def has_franchises(self) -> bool:
        return len(self._franchise_defs) > 0

    def resolve_franchise(self, guid: str, season: int) -> str | None:
        """Map a (manager GUID, season) to a franchise ID like 'franchise_0'."""
        for i, fdef in enumerate(self._franchise_defs):
            for m in fdef["managers"]:
                if m["guid"] != guid:
                    continue
                if season < m["from"]:
                    continue
                if "to" in m and m["to"] is not None and season > m["to"]:
                    continue
                return f"franchise_{i}"
        return None

    def franchise_list(self) -> list[dict]:
        """Return franchise summaries for the API response."""
        result = []
        for i, fdef in enumerate(self._franchise_defs):
            # Current owner is the last manager entry (no 'to' or highest 'to')
            last_mgr = fdef["managers"][-1]
            current_name = self.manager_name(last_mgr["guid"]) or last_mgr["guid"]
            ownership = []
            for m in fdef["managers"]:
                mgr_name = self.manager_name(m["guid"]) or m["guid"]
                ownership.append({
                    "manager": mgr_name,
                    "guid": m["guid"],
                    "from": m["from"],
                    "to": m.get("to"),
                })
            result.append({
                "id": f"franchise_{i}",
                "name": fdef["name"],
                "current_manager": current_name,
                "ownership": ownership,
            })
        return result


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


def add_managers(slug: str, managers: dict[str, dict]) -> list[str]:
    """Add new managers to a franchise's config and save to disk.

    Args:
        slug: Franchise slug (e.g., "baseball")
        managers: {guid: {"name": "...", "short_name": "..."}, ...}

    Returns:
        List of GUIDs that were actually added (skips existing).
    """
    raw = _load_raw()
    added = []

    for sport, franchise_list in raw.items():
        for fdata in franchise_list:
            if fdata["slug"] != slug:
                continue

            if not fdata.get("managers"):
                fdata["managers"] = {}

            for guid, info in managers.items():
                if guid not in fdata["managers"]:
                    fdata["managers"][guid] = info
                    added.append(guid)

    if added:
        with open(_FRANCHISES_FILE, "w") as f:
            yaml.dump(raw, f, default_flow_style=False, sort_keys=False,
                      allow_unicode=True)

    return added
