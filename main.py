"""Fantasy Analytics — CLI entry point."""

import sys

from cli.commands import list_franchises, list_seasons, sync_command, show_managers

USAGE = """
Usage:
  python main.py franchises                     — Show configured franchises
  python main.py seasons                        — List all your Yahoo sport-seasons

  python main.py sync <slug>                    — Sync all seasons for a franchise
  python main.py sync <slug> --season <year>    — Sync one season
  python main.py sync <slug> --incremental      — Sync latest unsynced week only
  python main.py managers <slug>                — Discover manager GUIDs for config
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
    else:
        print(f"Unknown command: {cmd}\n")
        print(USAGE)


if __name__ == "__main__":
    main()
