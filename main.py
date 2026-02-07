"""Fantasy Analytics — entry point for exploration and demo."""

import sys

from yahoo.client import YahooClient
from baseball.data import MLBDataClient
from basketball.data import NBADataClient


def demo_yahoo(sport: str):
    """Connect to Yahoo Fantasy and show league info."""
    print(f"\n{'='*50}")
    print(f"Yahoo Fantasy — {sport.upper()}")
    print(f"{'='*50}")

    client = YahooClient()

    league = client.get_league(sport)
    print(f"League: {league.name}")
    print(f"Season: {league.season}")
    print(f"Current week: {league.current_week}")

    teams = client.get_teams()
    print(f"\nTeams ({len(teams)}):")
    for team in teams:
        print(f"  {team.name}")

    standings = client.get_standings(sport)
    print(f"\nStandings:")
    for team in standings.teams:
        ts = team.team_standings
        print(f"  {ts.rank}. {team.name} ({ts.outcome_totals.wins}-{ts.outcome_totals.losses})")


def demo_mlb():
    """Show a quick MLB data pull."""
    print(f"\n{'='*50}")
    print("MLB Data — FanGraphs Batting Leaders")
    print(f"{'='*50}")

    mlb = MLBDataClient()
    stats = mlb.get_batting_stats()
    if not stats.empty:
        cols = ["Name", "Team", "G", "AVG", "OBP", "SLG", "HR", "wOBA"]
        available = [c for c in cols if c in stats.columns]
        print(stats[available].head(10).to_string(index=False))
    else:
        print("No batting stats available (season may not have started)")


def demo_nba():
    """Show a quick NBA data pull."""
    print(f"\n{'='*50}")
    print("NBA Data — Player Game Logs")
    print(f"{'='*50}")

    nba = NBADataClient()
    player_id = nba.get_player_id("LeBron James")
    if player_id:
        logs = nba.get_player_game_logs(player_id)
        if not logs.empty:
            cols = ["GAME_DATE", "MATCHUP", "PTS", "REB", "AST", "STL", "BLK"]
            available = [c for c in cols if c in logs.columns]
            print(f"LeBron James — last 5 games:")
            print(logs[available].head(5).to_string(index=False))
    else:
        print("Player not found")


def main():
    print("Fantasy Analytics")
    print("=" * 50)

    args = sys.argv[1:]

    if not args:
        print("\nUsage:")
        print("  python main.py yahoo mlb    — Yahoo Fantasy Baseball demo")
        print("  python main.py yahoo nba    — Yahoo Fantasy Basketball demo")
        print("  python main.py mlb          — MLB stats demo (no Yahoo needed)")
        print("  python main.py nba          — NBA stats demo (no Yahoo needed)")
        print("  python main.py all          — Run everything")
        return

    cmd = args[0].lower()

    if cmd == "yahoo" and len(args) > 1:
        demo_yahoo(args[1].lower())
    elif cmd == "mlb":
        demo_mlb()
    elif cmd == "nba":
        demo_nba()
    elif cmd == "all":
        demo_mlb()
        demo_nba()
        if len(args) > 1:
            demo_yahoo(args[1].lower())
        else:
            print("\nSkipping Yahoo demo (pass sport: python main.py all mlb)")


if __name__ == "__main__":
    main()
