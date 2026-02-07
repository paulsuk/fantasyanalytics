"""Fantasy Analytics — entry point and CLI."""

import sys

from yahoo.client import YahooClient, get_franchises
from baseball.data import MLBDataClient
from basketball.data import NBADataClient


def _name(obj) -> str:
    """Decode a yfpy name field that may be bytes or str."""
    name = obj if isinstance(obj, str) else obj.name if hasattr(obj, "name") else obj
    if isinstance(name, bytes):
        return name.decode("utf-8", errors="replace")
    return str(name)


def list_franchises():
    """Show configured franchises and their season history."""
    print(f"\n{'='*50}")
    print("Configured Franchises")
    print(f"{'='*50}")

    franchises = get_franchises()
    for sport, franchise_list in sorted(franchises.items()):
        print(f"\n  {sport.upper()}:")
        for f in franchise_list:
            default = " (default)" if f.is_default else ""
            print(f"    {f.name}{default}  [slug: {f.slug}]")
            for year in sorted(f.seasons, reverse=True):
                print(f"      {year}: {f.seasons[year]}")


def list_seasons():
    """List all sport-seasons the user has participated in."""
    print(f"\n{'='*50}")
    print("Yahoo Fantasy — Your Sport Seasons")
    print(f"{'='*50}")

    client = YahooClient()
    query = client.get_user_query()
    sport_seasons = query.get_user_games()

    by_sport = {}
    for ss in sport_seasons:
        by_sport.setdefault(ss.code, []).append(ss)

    for sport_code, seasons in sorted(by_sport.items()):
        print(f"\n  {sport_code.upper()}:")
        for ss in sorted(seasons, key=lambda s: s.season, reverse=True):
            print(f"    {ss.season} — game_key={ss.game_key}")


def list_leagues(sport: str = None):
    """List leagues for current seasons. Optionally filter by sport."""
    print(f"\n{'='*50}")
    print("Yahoo Fantasy — Your Leagues")
    print(f"{'='*50}")

    client = YahooClient()
    query = client.get_user_query()
    sport_seasons = query.get_user_games()

    # Get the most recent sport-season per sport
    latest = {}
    for ss in sport_seasons:
        if sport and ss.code != sport:
            continue
        if ss.code not in latest or ss.season > latest[ss.code].season:
            latest[ss.code] = ss

    for sport_code, ss in sorted(latest.items()):
        print(f"\n  {sport_code.upper()} {ss.season} (game_key={ss.game_key}):")
        leagues = query.get_user_leagues_by_game_key(ss.game_key)
        for lg in leagues:
            print(f"    {_name(lg.name)}")
            print(f"      league_key={lg.league_key}  (id={lg.league_id})")


def show_league(sport: str):
    """Show league info, teams, and standings for a sport's default franchise."""
    print(f"\n{'='*50}")
    print(f"Yahoo Fantasy — {sport.upper()}")
    print(f"{'='*50}")

    client = YahooClient()

    league = client.get_league(sport)
    print(f"League: {_name(league.name)}")
    print(f"Season: {league.season}")
    print(f"Current week: {league.current_week}")

    teams = client.get_teams(sport)
    print(f"\nTeams ({len(teams)}):")
    for team in teams:
        print(f"  {_name(team.name)}")

    standings = client.get_standings(sport)
    print(f"\nStandings:")
    for team in standings.teams:
        ts = team.team_standings
        print(f"  {ts.rank}. {_name(team.name)} ({ts.outcome_totals.wins}-{ts.outcome_totals.losses})")


def show_mlb_stats():
    """Show FanGraphs batting leaders for current season."""
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


def show_nba_stats():
    """Show recent game logs for a sample player."""
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


USAGE = """
Usage:
  python main.py franchises         — Show configured franchises
  python main.py seasons            — List all your Yahoo sport-seasons
  python main.py leagues            — List your current-season leagues
  python main.py leagues mlb        — List only MLB leagues
  python main.py leagues nba        — List only NBA leagues
  python main.py yahoo mlb          — Show MLB league info + standings
  python main.py yahoo nba          — Show NBA league info + standings
  python main.py mlb                — FanGraphs batting leaders (no Yahoo)
  python main.py nba                — NBA player game logs (no Yahoo)
""".strip()


def main():
    args = sys.argv[1:]

    if not args:
        print("Fantasy Analytics\n")
        print(USAGE)
        return

    cmd = args[0].lower()

    if cmd == "franchises":
        list_franchises()
    elif cmd == "seasons":
        list_seasons()
    elif cmd == "leagues":
        sport_filter = args[1].lower() if len(args) > 1 else None
        list_leagues(sport_filter)
    elif cmd == "yahoo" and len(args) > 1:
        show_league(args[1].lower())
    elif cmd == "mlb":
        show_mlb_stats()
    elif cmd == "nba":
        show_nba_stats()
    else:
        print(f"Unknown command: {cmd}\n")
        print(USAGE)


if __name__ == "__main__":
    main()
