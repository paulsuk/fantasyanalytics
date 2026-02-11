-- Fantasy Analytics database schema
-- One SQLite file per franchise (e.g., data/baseball.db, data/basketball.db)

-- ============================================================
-- Metadata tables
-- ============================================================

CREATE TABLE IF NOT EXISTS league (
    league_key      TEXT PRIMARY KEY,   -- e.g. "458.l.25845"
    season          INTEGER NOT NULL,
    name            TEXT,
    sport           TEXT,               -- "mlb" or "nba"
    num_teams       INTEGER,
    scoring_type    TEXT,               -- "head" (H2H) or "roto"
    num_categories  INTEGER,
    current_week    INTEGER,
    start_week      INTEGER,
    end_week        INTEGER,
    playoff_start_week INTEGER,
    uses_faab       INTEGER DEFAULT 0,
    is_finished     INTEGER DEFAULT 0,
    fetched_at      TEXT                -- ISO timestamp
);

CREATE TABLE IF NOT EXISTS stat_category (
    league_key      TEXT NOT NULL,
    stat_id         INTEGER NOT NULL,
    name            TEXT,               -- e.g. "Runs"
    display_name    TEXT,               -- e.g. "R"
    abbr            TEXT,
    sort_order      INTEGER,            -- 1 = higher is better, 0 = lower is better
    position_type   TEXT,               -- "B" (batting) or "P" (pitching), NULL for NBA
    is_only_display INTEGER DEFAULT 0,
    is_scoring_stat INTEGER DEFAULT 1,  -- 1 if this stat is an H2H category
    PRIMARY KEY (league_key, stat_id),
    FOREIGN KEY (league_key) REFERENCES league(league_key)
);

CREATE TABLE IF NOT EXISTS team (
    league_key       TEXT NOT NULL,
    team_key         TEXT NOT NULL,      -- e.g. "458.l.25845.t.1"
    team_id          INTEGER,
    name             TEXT,
    manager_guid     TEXT,
    manager_nickname TEXT,               -- from Yahoo
    manager_name     TEXT,               -- from config (resolved at sync time)
    waiver_priority  INTEGER,
    faab_balance     REAL,
    PRIMARY KEY (league_key, team_key),
    FOREIGN KEY (league_key) REFERENCES league(league_key)
);

CREATE TABLE IF NOT EXISTS player (
    player_key       TEXT PRIMARY KEY,   -- e.g. "458.p.12345" (season-specific)
    player_id        INTEGER,
    full_name        TEXT,
    first_name       TEXT,
    last_name        TEXT,
    pro_team         TEXT,
    primary_position TEXT,
    eligible_positions TEXT,             -- comma-separated
    headshot_url     TEXT
);

-- ============================================================
-- Weekly data tables
-- ============================================================

CREATE TABLE IF NOT EXISTS matchup (
    league_key      TEXT NOT NULL,
    week            INTEGER NOT NULL,
    matchup_id      INTEGER NOT NULL,
    team_key_1      TEXT,
    team_key_2      TEXT,
    cats_won_1      INTEGER,            -- categories won by team 1
    cats_won_2      INTEGER,
    cats_tied       INTEGER DEFAULT 0,
    winner_team_key TEXT,               -- NULL if tied
    is_tied         INTEGER DEFAULT 0,
    is_playoffs     INTEGER DEFAULT 0,
    is_consolation  INTEGER DEFAULT 0,
    week_start      TEXT,               -- ISO date
    week_end        TEXT,               -- ISO date
    PRIMARY KEY (league_key, week, matchup_id),
    FOREIGN KEY (league_key) REFERENCES league(league_key)
);

CREATE TABLE IF NOT EXISTS matchup_category (
    league_key      TEXT NOT NULL,
    week            INTEGER NOT NULL,
    matchup_id      INTEGER NOT NULL,
    stat_id         INTEGER NOT NULL,
    team_1_value    REAL,
    team_2_value    REAL,
    winner_team_key TEXT,               -- NULL if tied
    PRIMARY KEY (league_key, week, matchup_id, stat_id),
    FOREIGN KEY (league_key, week, matchup_id)
        REFERENCES matchup(league_key, week, matchup_id)
);

CREATE TABLE IF NOT EXISTS weekly_roster (
    league_key      TEXT NOT NULL,
    week            INTEGER NOT NULL,
    team_key        TEXT NOT NULL,
    player_key      TEXT NOT NULL,
    selected_position TEXT,
    is_starter      INTEGER DEFAULT 1,
    PRIMARY KEY (league_key, week, team_key, player_key),
    FOREIGN KEY (league_key) REFERENCES league(league_key)
);

CREATE TABLE IF NOT EXISTS player_weekly_stat (
    league_key      TEXT NOT NULL,
    week            INTEGER NOT NULL,
    player_key      TEXT NOT NULL,
    stat_id         INTEGER NOT NULL,
    value           REAL,
    PRIMARY KEY (league_key, week, player_key, stat_id),
    FOREIGN KEY (league_key) REFERENCES league(league_key)
);

CREATE TABLE IF NOT EXISTS player_game_log (
    sport           TEXT NOT NULL,       -- "mlb" or "nba"
    player_id       TEXT NOT NULL,       -- sport-specific ID (mlbam_id or nba_player_id)
    game_date       TEXT NOT NULL,       -- ISO date
    player_name     TEXT,
    pro_team        TEXT,
    opponent        TEXT,
    stats           TEXT,               -- JSON blob of sport-specific stats
    PRIMARY KEY (sport, player_id, game_date)
);

CREATE TABLE IF NOT EXISTS team_weekly_score (
    league_key      TEXT NOT NULL,
    week            INTEGER NOT NULL,
    team_key        TEXT NOT NULL,
    stat_id         INTEGER NOT NULL,
    value           REAL,
    PRIMARY KEY (league_key, week, team_key, stat_id),
    FOREIGN KEY (league_key) REFERENCES league(league_key)
);

-- ============================================================
-- Transaction tables
-- ============================================================

CREATE TABLE IF NOT EXISTS transaction_record (
    transaction_key TEXT PRIMARY KEY,
    league_key      TEXT NOT NULL,
    type            TEXT,               -- "add", "drop", "trade", "add/drop"
    status          TEXT,
    timestamp       TEXT,               -- ISO timestamp
    week            INTEGER,            -- computed from timestamp + matchup date ranges
    trader_team_key TEXT,
    tradee_team_key TEXT,
    faab_bid        REAL,
    FOREIGN KEY (league_key) REFERENCES league(league_key)
);

CREATE TABLE IF NOT EXISTS transaction_player (
    transaction_key TEXT NOT NULL,
    player_key      TEXT NOT NULL,
    source_type     TEXT,               -- "freeagents", "waivers", "team"
    source_team_key TEXT,
    destination_type TEXT,              -- "team", "waivers", "freeagents"
    destination_team_key TEXT,
    type            TEXT,               -- "add" or "drop"
    PRIMARY KEY (transaction_key, player_key),
    FOREIGN KEY (transaction_key) REFERENCES transaction_record(transaction_key)
);

CREATE TABLE IF NOT EXISTS draft_pick (
    league_key      TEXT NOT NULL,
    pick            INTEGER NOT NULL,
    round           INTEGER,
    team_key        TEXT,
    player_key      TEXT,
    cost            REAL,
    PRIMARY KEY (league_key, pick),
    FOREIGN KEY (league_key) REFERENCES league(league_key)
);

-- ============================================================
-- Operational tables
-- ============================================================

CREATE TABLE IF NOT EXISTS sync_log (
    league_key      TEXT NOT NULL,
    sync_type       TEXT NOT NULL,      -- "metadata", "weekly", "transactions", "draft", etc.
    week            INTEGER NOT NULL DEFAULT 0,  -- 0 for non-weekly syncs
    started_at      TEXT,
    completed_at    TEXT,
    status          TEXT,               -- "running", "completed", "failed"
    records_written INTEGER DEFAULT 0,
    error_message   TEXT,
    PRIMARY KEY (league_key, sync_type, week)
);
