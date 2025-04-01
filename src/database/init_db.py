import sqlite3
from src.utils.config import DB_PATH

def init_database():
    """Initialize the SQLite database with required tables."""
    conn = sqlite3.connect(DB_PATH)
    
    conn.execute("""
    CREATE TABLE IF NOT EXISTS players (
        name TEXT NOT NULL UNIQUE NOT NULL,
        nba_api_id INTEGER UNIQUE NOT NULL,
        is_active BOOLEAN DEFAULT 1
    );
    """)


    conn.execute("""
    CREATE TABLE IF NOT EXISTS raw_ocr_bets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_id INTEGER,
        bet_type TEXT,
        score REAL,
        date DATE,
        bet_line TEXT,
        odds INTEGER, 
        image_source TEXT,
        raw_text TEXT NOT NULL,
        read_players TEXT,
        read_score_patterns TEXT,
        is_processed BOOLEAN DEFAULT 0,
        needs_review BOOLEAN DEFAULT 0,
        is_voided BOOLEAN DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (player_id) REFERENCES players(nba_api_id),
        UNIQUE(player_id, date, bet_type, score) --Players can have multiple bets of same type
    );
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS game_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_id INTEGER NOT NULL,
        date DATE NOT NULL,
        points REAL,
        assists REAL,
        rebounds REAL,
        three_pointers REAL,
        blocks REAL,
        steals REAL,
        turnovers REAL,
        par REAL,                    -- Points + Assists + Rebounds
        pts_rebs REAL,
        pts_asts REAL,
        rebs_asts REAL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (player_id) REFERENCES players(nba_api_id),
        UNIQUE(player_id, date)      -- Each player should have one stat line per date
    );
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS bet_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        raw_bet_id INTEGER NOT NULL,
        player_id INTEGER NOT NULL,
        game_stats_id INTEGER NOT NULL, -- Not UNIQUE because multiple bets can come from same player in one game
        bet_type TEXT NOT NULL,
        result TEXT CHECK (result IN ('Win', 'Loss')) NOT NULL,
        result_delta REAL NOT NULL,
        score_range TEXT NOT NULL,
        over_under TEXT NOT NULL CHECK (over_under IN ('Over', 'Under')), 
        stat_result REAL NOT NULL, -- Value of stat being bet (e.g. 25 if bet_type='points' and points = 25)
        line_value REAL NOT NULL, -- Line being bet (e.g. 6.5 in o6.5)
        is_uploaded BOOLEAN DEFAULT 0,
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (raw_bet_id) REFERENCES raw_ocr_bets(id) ON DELETE CASCADE,
        FOREIGN KEY (player_id) REFERENCES players(nba_api_id),
        FOREIGN KEY (game_stats_id) REFERENCES game_stats(id),
        UNIQUE(raw_bet_id) -- Each bet has one result
    );
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS unplayed_bets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        raw_bet_id INTEGER NOT NULL UNIQUE,
        player_id INTEGER NOT NULL,
        date DATE NOT NULL,
        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (raw_bet_id) REFERENCES raw_ocr_bets(id) ON DELETE CASCADE,
        FOREIGN KEY (player_id) REFERENCES players(nba_api_id)
    );
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS aggregated_results (
        sheet_cell TEXT PRIMARY KEY NOT NULL,
        bet_type TEXT NOT NULL,
        score_range TEXT NOT NULL,
        volume INTEGER,
        result REAL,
        updated_to DATE NOT NULL,
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    conn.commit()
    conn.close()

if __name__ == "__main__":
    init_database()