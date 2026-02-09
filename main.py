"""Fantasy Analytics — CLI entry point."""

import sys

from cli.display import (
    list_franchises, list_seasons, list_leagues,
    show_league, show_mlb_stats, show_nba_stats,
    sync_command, show_managers,
    cmd_value, cmd_teams, cmd_recap,
)

USAGE = """
Usage:
  python main.py franchises                     — Show configured franchises
  python main.py seasons                        — List all your Yahoo sport-seasons
  python main.py leagues [mlb|nba]              — List your current-season leagues
  python main.py yahoo mlb|nba                  — Show league info + standings
  python main.py mlb                            — FanGraphs batting leaders
  python main.py nba [player name]               — NBA player game logs

  python main.py sync <slug>                    — Sync all seasons for a franchise
  python main.py sync <slug> --season <year>    — Sync one season
  python main.py sync <slug> --incremental      — Sync latest unsynced week only
  python main.py managers <slug>                — Discover manager GUIDs for config

  python main.py value <slug> --week <N>        — Top players by z-score for a week
  python main.py teams <slug> <week>            — Team profiles / power rankings
  python main.py recap <slug> <week>            — Full weekly recap
  python main.py recap <slug> --latest          — Recap for most recent completed week
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
        player_name = " ".join(args[1:]) if len(args) > 1 else "LeBron James"
        show_nba_stats(player_name)
    elif cmd == "sync" and len(args) > 1:
        slug = args[1]
        season = None
        incremental = "--incremental" in args
        if "--season" in args:
            idx = args.index("--season")
            if idx + 1 < len(args):
                season = int(args[idx + 1])
        sync_command(slug, season=season, incremental=incremental)
    elif cmd == "managers" and len(args) > 1:
        show_managers(args[1])
    elif cmd == "value" and len(args) > 1:
        cmd_value(args[1:])
    elif cmd == "teams" and len(args) > 1:
        cmd_teams(args[1:])
    elif cmd == "recap" and len(args) > 1:
        cmd_recap(args[1:])
    else:
        print(f"Unknown command: {cmd}\n")
        print(USAGE)


if __name__ == "__main__":
    main()
