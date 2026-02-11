# Fantasy Analytics API

Python backend for fantasy sports analytics. Syncs Yahoo Fantasy league data (MLB + NBA) into SQLite and serves analytics via FastAPI.

## Setup

```bash
python -m venv venv
venv\Scripts\activate        # Windows
source venv/bin/activate     # macOS/Linux

pip install -r requirements.txt
```

Create a `.env` file with your Yahoo OAuth credentials:

```
YAHOO_CONSUMER_KEY=your_key
YAHOO_CONSUMER_SECRET=your_secret
```

## Syncing Data

```bash
python main.py sync baseball                # Sync all seasons
python main.py sync baseball --season 2024  # Sync one season
python main.py sync baseball --incremental  # Sync latest unsynced week
python main.py managers baseball            # Discover manager GUIDs
python main.py franchises                   # Show configured franchises
python main.py seasons                      # List Yahoo sport-seasons
```

## Running the API

```bash
uvicorn server:app --reload --port 8000
```

The API serves at `http://localhost:8000`. Interactive docs at `http://localhost:8000/docs`.

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Health check |
| `GET /api/franchises` | List configured franchises |
| `GET /api/{slug}/seasons` | List synced seasons for a franchise |
| `GET /api/{slug}/recap?week=N&season=Y` | Weekly recap (matchups, awards, standings, profiles) |
| `GET /api/{slug}/teams?week=N&season=Y` | Power rankings / team profiles |
| `GET /api/{slug}/managers` | Cross-season manager records + H2H matrix |
| `GET /api/{slug}/records` | All-time league records (streaks, blowouts, category records) |

`week` and `season` are optional. If omitted, defaults to the latest completed week of the latest season.

## Testing the API

Start the server, then test with curl:

```bash
# Health check
curl http://localhost:8000/health

# List franchises
curl http://localhost:8000/api/franchises

# Seasons for a franchise
curl http://localhost:8000/api/baseball/seasons

# Weekly recap (latest week)
curl http://localhost:8000/api/baseball/recap

# Specific week/season
curl "http://localhost:8000/api/baseball/recap?week=5&season=2024"

# Power rankings
curl http://localhost:8000/api/baseball/teams

# Manager history
curl http://localhost:8000/api/baseball/managers

# All-time records
curl http://localhost:8000/api/baseball/records
```

Or open `http://localhost:8000/docs` for the interactive Swagger UI.

## Project Structure

```
server.py              — FastAPI app factory (mounts routers)
main.py                — CLI entry point
utils.py               — decode_name, build_team_key
config/
  franchises.py        — Franchise class + YAML loader
  franchises.yaml      — League history + manager config
  constants.py         — Sport enum, bench positions
routes/
  health.py            — GET /health
  leagues.py           — Franchise/season endpoints + resolve helpers
  analytics.py         — Recap, teams, managers, records endpoints
db/
  database.py          — SQLite wrapper with transaction() context manager
  schema.sql           — 14-table schema
  queries/             — Read-only query functions by domain
    leagues.py, teams.py, matchups.py,
    players.py, transactions.py, history.py
sync/
  yahoo_client.py      — YahooClient (wraps yfpy)
  yahoo_sync.py        — Yahoo API -> SQLite pipeline
  sport_data.py        — MLBDataClient + NBADataClient
analytics/
  value.py             — Z-score player value
  teams.py             — Team profiles, power rankings
  recap.py             — Weekly recap assembler
  history.py           — Manager history, league records
cli/
  commands.py          — CLI command implementations
```

## Deployment

Deploys to Google Cloud Run via GitHub Actions on push to master.

```bash
# Local Docker test
docker build -t fantasy-api .
docker run -p 8080:8080 fantasy-api
```
