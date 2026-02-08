"""CLI display and command functions."""

from yahoo.client import YahooClient
from config import get_franchises
from sync.sport_data import MLBDataClient, NBADataClient
from utils import decode_name, is_mlb_league
from cli.resolve import resolve_league_key, parse_season_arg


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
            print(f"    {decode_name(lg.name)}")
            print(f"      league_key={lg.league_key}  (id={lg.league_id})")


def show_league(sport: str):
    """Show league info, teams, and standings for a sport's default franchise."""
    print(f"\n{'='*50}")
    print(f"Yahoo Fantasy — {sport.upper()}")
    print(f"{'='*50}")

    client = YahooClient()

    league = client.get_league(sport)
    print(f"League: {decode_name(league.name)}")
    print(f"Season: {league.season}")
    print(f"Current week: {league.current_week}")

    teams = client.get_teams(sport)
    print(f"\nTeams ({len(teams)}):")
    for team in teams:
        print(f"  {decode_name(team.name)}")

    standings = client.get_standings(sport)
    print(f"\nStandings:")
    for team in standings.teams:
        ts = team.team_standings
        print(f"  {ts.rank}. {decode_name(team.name)} ({ts.outcome_totals.wins}-{ts.outcome_totals.losses})")


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


def sync_command(slug: str, season: int = None, incremental: bool = False):
    """Sync Yahoo data into the database."""
    from sync.yahoo_sync import YahooSync

    syncer = YahooSync(slug)
    try:
        if incremental:
            syncer.sync_incremental()
        elif season:
            syncer.sync_season(season)
        else:
            syncer.sync_all()
    finally:
        syncer.close()


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


def cmd_value(args: list):
    """Show top players by z-score for a week."""
    from analytics.value import PlayerValue

    slug = args[0]
    week = None
    if "--week" in args:
        idx = args.index("--week")
        if idx + 1 < len(args):
            week = int(args[idx + 1])

    db, league_key = resolve_league_key(slug, parse_season_arg(args))
    if not db:
        return

    if not week:
        row = db.fetchone("SELECT current_week, end_week, is_finished FROM league WHERE league_key=?",
                          (league_key,))
        week = row["end_week"] if row["is_finished"] else max(row["current_week"] - 1, 1)

    pv = PlayerValue(db, league_key)

    print(f"\n{'='*60}")
    print(f"Player Value — Week {week}")
    print(f"{'='*60}")

    if is_mlb_league(db, league_key):
        batters = pv.top_batters(week, limit=5)
        print(f"\n  Batter of the Week:")
        for i, p in enumerate(batters):
            line = ", ".join(f"{k}={v}" for k, v in p.stat_line.items() if v)
            print(f"    {i+1}. {p.name} ({p.manager}) z={p.z_total:+.2f}")
            print(f"       {line}")

        pitchers = pv.top_pitchers(week, limit=5)
        print(f"\n  Pitcher of the Week:")
        for i, p in enumerate(pitchers):
            line = ", ".join(f"{k}={v}" for k, v in p.stat_line.items() if v)
            print(f"    {i+1}. {p.name} ({p.manager}) z={p.z_total:+.2f}")
            print(f"       {line}")
    else:
        players = pv.top_players(week, limit=10)
        print(f"\n  Player of the Week:")
        for i, p in enumerate(players):
            line = ", ".join(f"{k}={v}" for k, v in p.stat_line.items() if v)
            print(f"    {i+1}. {p.name} ({p.manager}) z={p.z_total:+.2f}")
            print(f"       {line}")

    db.close()


def cmd_teams(args: list):
    """Show team profiles / power rankings."""
    from analytics.teams import TeamProfiler

    slug = args[0]
    week = int(args[1]) if len(args) > 1 and args[1].isdigit() else None

    db, league_key = resolve_league_key(slug, parse_season_arg(args))
    if not db:
        return

    if not week:
        row = db.fetchone("SELECT current_week, end_week, is_finished FROM league WHERE league_key=?",
                          (league_key,))
        week = row["end_week"] if row["is_finished"] else max(row["current_week"] - 1, 1)

    profiler = TeamProfiler(db, league_key)
    profiles = profiler.build_profiles(week)

    print(f"\n{'='*60}")
    print(f"Power Rankings — Week {week}")
    print(f"{'='*60}")

    for p in profiles:
        rank_change = p.prev_rank - p.rank
        arrow = ""
        if rank_change > 0:
            arrow = f" (+{rank_change})"
        elif rank_change < 0:
            arrow = f" ({rank_change})"

        streak_str = f"W{p.streak}" if p.streak > 0 else (f"L{-p.streak}" if p.streak < 0 else "")
        form = "-".join(p.last_3)

        print(f"\n  {p.rank}. {p.team_name} ({p.manager}){arrow}")
        print(f"     Record: {p.wins}-{p.losses}-{p.ties}  |  Form: {form}  {streak_str}")
        print(f"     Strengths: {', '.join(p.cat_strengths)}  |  Weaknesses: {', '.join(p.cat_weaknesses)}")
        if p.mvp_name:
            print(f"     MVP: {p.mvp_name} (z={p.mvp_z:+.2f})")
        if p.opponent_name:
            print(f"     Next: vs {p.opponent_name} (H2H: {p.h2h_record})")

    db.close()


def cmd_recap(args: list):
    """Show weekly recap."""
    from analytics.recap import RecapAssembler

    slug = args[0]
    latest = "--latest" in args
    week = None

    if not latest and len(args) > 1 and args[1].isdigit():
        week = int(args[1])

    db, league_key = resolve_league_key(slug, parse_season_arg(args))
    if not db:
        return

    if not week:
        row = db.fetchone("SELECT current_week, end_week, is_finished FROM league WHERE league_key=?",
                          (league_key,))
        week = row["end_week"] if row["is_finished"] else max(row["current_week"] - 1, 1)

    assembler = RecapAssembler(db, league_key)
    recap = assembler.build(week)

    print(f"\n{'='*60}")
    print(f"Weekly Recap — {recap.league_name} Week {recap.week}")
    print(f"{recap.week_start} to {recap.week_end}")
    print(f"{'='*60}")

    # Matchups
    print(f"\n  MATCHUPS")
    for m in recap.matchups:
        tag = ""
        if m.is_playoffs:
            tag = " [PLAYOFF]"
        elif m.is_consolation:
            tag = " [CONSOLATION]"
        print(f"    {m.team_1_name} ({m.team_1_manager}) {m.cats_won_1}-{m.cats_won_2}-{m.cats_tied} "
              f"{m.team_2_name} ({m.team_2_manager}){tag}")

        # Category detail
        for c in m.categories:
            v1 = c["team_1_value"] if c["team_1_value"] is not None else "-"
            v2 = c["team_2_value"] if c["team_2_value"] is not None else "-"
            marker = "<" if c["winner"] == 1 else (">" if c["winner"] == 2 else "=")
            print(f"      {c['display_name']:>6}: {str(v1):>8} {marker} {str(v2):<8}")

    # Awards
    print(f"\n  AWARDS")
    if recap.batter_of_week:
        b = recap.batter_of_week
        line = ", ".join(f"{k}={v}" for k, v in b.stat_line.items() if v)
        print(f"    Batter of the Week: {b.name} ({b.manager}) z={b.z_total:+.2f}")
        print(f"      {line}")
    if recap.pitcher_of_week:
        p = recap.pitcher_of_week
        line = ", ".join(f"{k}={v}" for k, v in p.stat_line.items() if v)
        print(f"    Pitcher of the Week: {p.name} ({p.manager}) z={p.z_total:+.2f}")
        print(f"      {line}")
    if recap.player_of_week:
        p = recap.player_of_week
        line = ", ".join(f"{k}={v}" for k, v in p.stat_line.items() if v)
        print(f"    Player of the Week: {p.name} ({p.manager}) z={p.z_total:+.2f}")
        print(f"      {line}")

    # Standings
    print(f"\n  STANDINGS (through week {week})")
    for s in recap.standings:
        print(f"    {s['rank']:>2}. {s['team_name']:<30} {s['wins']}-{s['losses']}-{s['ties']}")

    # Power Rankings summary
    print(f"\n  POWER RANKINGS")
    for p in recap.profiles:
        streak_str = f"W{p.streak}" if p.streak > 0 else (f"L{-p.streak}" if p.streak < 0 else "")
        form = "-".join(p.last_3)
        print(f"    {p.rank}. {p.team_name} ({p.manager}) {p.wins}-{p.losses}-{p.ties} {streak_str} [{form}]")
        if p.mvp_name:
            print(f"       MVP: {p.mvp_name} (z={p.mvp_z:+.2f})")

    db.close()
