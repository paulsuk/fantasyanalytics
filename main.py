"""Fantasy Analytics — CLI entry point."""

import sys

from sync.yahoo_client import YahooClient
from config import get_franchises
from utils import decode_name


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


def sync_command(slug: str, season: int = None, incremental: bool = False,
                 sync_standings: bool = False):
    """Sync Yahoo data into the database."""
    from sync.yahoo_sync import YahooSync

    syncer = YahooSync(slug)
    try:
        if sync_standings:
            syncer.sync_standings()
        elif incremental:
            syncer.sync_incremental()
        elif season:
            syncer.sync_season(season)
        else:
            syncer.sync_all()
    finally:
        syncer.close()


def sync_keepers_command(slug: str, verbose: bool = False, force: bool = False):
    """Sync keeper designations from Yahoo API into the database."""
    from sync.yahoo_sync import YahooSync

    syncer = YahooSync(slug)
    try:
        if force:
            syncer.db.execute("DELETE FROM sync_log WHERE sync_type='keepers'")
            syncer.db.execute("DELETE FROM keeper")
            print("Cleared existing keeper data and sync log.")
        syncer.sync_keepers(verbose=verbose)
    finally:
        syncer.close()


def show_keepers(slug: str):
    """Show keeper data currently in the database."""
    from config import get_franchise_by_slug
    from db import Database

    franchise = get_franchise_by_slug(slug)
    if not franchise:
        print(f"Unknown franchise slug: '{slug}'")
        return

    db = Database(slug)
    try:
        rows = db.fetchall(
            "SELECT k.season, k.player_name, k.round_cost, k.kept_from_season, "
            "       t.name AS team_name, t.manager_name "
            "FROM keeper k "
            "JOIN team t ON k.league_key = t.league_key AND k.team_key = t.team_key "
            "ORDER BY k.season, t.name, k.player_name"
        )
        if not rows:
            print(f"No keepers found in database for '{slug}'")
            return

        print(f"\n{'='*50}")
        print(f"Keepers — {franchise.name}")
        print(f"{'='*50}")

        current_season = None
        current_team = None
        for r in rows:
            if r["season"] != current_season:
                current_season = r["season"]
                current_team = None
                print(f"\n  {current_season}:")
            if r["team_name"] != current_team:
                current_team = r["team_name"]
                team = r["team_name"].encode("ascii", "replace").decode()
                print(f"    {r['manager_name']} ({team}):")
            print(f"      {r['player_name']}")
    finally:
        db.close()


def show_managers(slug: str):
    """Discover manager GUIDs and auto-add unconfigured ones to franchises.yaml."""
    from config import get_franchise_by_slug, add_managers

    franchise = get_franchise_by_slug(slug)
    if not franchise:
        print(f"Unknown franchise slug: '{slug}'")
        return

    client = YahooClient()
    print(f"\n{'='*50}")
    print(f"Managers — {franchise.name}")
    print(f"{'='*50}")

    season = franchise.latest_season
    query = client.query_for_franchise(slug, season)
    teams = query.get_league_teams()

    to_add = {}
    print(f"\n  Season {season}:")
    for team in teams:
        mgrs = getattr(team, "managers", [])
        mgr = mgrs[0] if mgrs else None
        if not mgr:
            continue
        guid = getattr(mgr, "guid", "")
        nickname = getattr(mgr, "nickname", "")
        team_name = decode_name(team.name)
        configured = franchise.manager_name(guid)
        if configured:
            print(f"    {configured:<20} {team_name:<30} (configured)")
        else:
            print(f"    {nickname:<20} {team_name:<30} (new)")
            to_add[guid] = {"name": nickname, "short_name": nickname}

    if to_add:
        added = add_managers(slug, to_add)
        names = [to_add[g]["name"] for g in added]
        print(f"\n  Added {len(added)} manager(s) to franchises.yaml: {', '.join(names)}")
        print(f"  Edit franchises.yaml to set full names if needed.")
    else:
        print(f"\n  All managers configured.")

USAGE = """
Usage:
  python main.py franchises                     — Show configured franchises
  python main.py seasons                        — List all your Yahoo sport-seasons

  python main.py sync <slug>                    — Sync all seasons for a franchise
  python main.py sync <slug> --season <year>    — Sync one season
  python main.py sync <slug> --incremental      — Sync latest unsynced week only
  python main.py sync <slug> --sync-standings   — Update finish/playoff_seed from Yahoo
  python main.py managers <slug>                — Discover manager GUIDs for config

  python main.py keepers sync <slug>              — Sync keepers from Yahoo API into DB
  python main.py keepers sync <slug> --verbose   — Sync with diagnostic output per player
  python main.py keepers sync <slug> --force     — Clear existing data and resync
  python main.py keepers show <slug>             — Show keeper data from DB
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
        sync_standings = "--sync-standings" in args
        if "--season" in args:
            idx = args.index("--season")
            if idx + 1 < len(args):
                season = int(args[idx + 1])
        sync_command(slug, season=season, incremental=incremental,
                     sync_standings=sync_standings)
    elif cmd == "managers" and len(args) > 1:
        show_managers(args[1])
    elif cmd == "keepers" and len(args) > 2:
        subcmd = args[1].lower()
        slug = args[2]
        if subcmd == "sync":
            verbose = "--verbose" in args
            force = "--force" in args
            sync_keepers_command(slug, verbose=verbose, force=force)
        elif subcmd == "show":
            show_keepers(slug)
        else:
            print(f"Unknown keepers subcommand: {subcmd}\n")
            print(USAGE)
    else:
        print(f"Unknown command: {cmd}\n")
        print(USAGE)


if __name__ == "__main__":
    main()
