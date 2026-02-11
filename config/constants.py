"""Sport-specific constants."""

from enum import Enum


class Sport(str, Enum):
    MLB = "mlb"
    NBA = "nba"


# Non-starter roster positions by sport.
BENCH_POSITIONS: dict[str, set[str]] = {
    "mlb": {"BN", "IL", "IL+", "NA", "DL"},
    "nba": {"BN", "IL", "IL+", "INJ", "NA"},
}
_DEFAULT_BENCH = {"BN", "NA", "IL", "IL+", "DL", "INJ"}


def bench_positions(sport: str) -> set[str]:
    """Return the set of non-starter position codes for a sport."""
    return BENCH_POSITIONS.get(sport, _DEFAULT_BENCH)
