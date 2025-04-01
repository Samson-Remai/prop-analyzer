"""
NBA Betting Data Processor
-------------------------

Workflow:
1. Processes unprocessed bets from raw_ocr_bets table
2. Fetches game statistics from NBA API for each bet
3. Updates bet results based on actual game performance
4. Marks bets as processed or flags unplayed games
"""

import sqlite3
from datetime import datetime
from pathlib import Path
from time import sleep
from random import uniform
from typing import Dict, Any, Optional, Tuple, List

import pandas as pd
import numpy as np

from requests.exceptions import RequestException
from requests.exceptions import Timeout
from nba_api.stats.endpoints import playergamelog

from src.utils.logger import setup_logger
from src.utils.config import DB_PATH
from src.utils.constants import SEASON

logger = setup_logger(__name__)

class BetProcessor:
    """Processes betting data using NBA statistics and updates database.
    
    Retrieves unprocessed bets from database (raw_ocr_bets table), fetches game statistics (NBA API),
    and updates bet results (game_stats, bet_results, unplayed_bets tables). 

    Database Tables:
        - raw_ocr_bets: Source of unprocessed betting data, representing bet slips read from images
        - game_stats: Cached NBA game statistics for a player
        - bet_results: Processed results for a bet based on game stats and bet info
        - unplayed_bets: Records of bets for which the player did not play
    """

    def __init__(self) -> None:
        """Initialize the bet processor."""
        self.db_file = DB_PATH
        self.conn = sqlite3.connect(self.db_file)

    def process_new_bets(self) -> bool:
        """Process all unprocessed bets in database.
        
        Fetches bets without stats, retrieves game statistics,
        calculates results, and updates database accordingly.
        
        Database:
            Reads from:
                - raw_ocr_bets: bets marked as is_processed = 0, is_voided = 0, and needs_review = 0
                - players: player_id for NBA API and player_name for display
            Writes to:
                - game_stats with NBA API data for player_id and game_date: points, assists, rebounds, three_pointers, blocks, steals, turnovers, par, pts_rebs, pts_asts, rebs_asts 
                - bet_results with relevant results for a bet: raw_bet_id, player_id, game_stats_id, bet_type, result, result_delta, score_range, over_under, stat_result, line_value
                - unplayed_bets with raw_ocr_bets IDs for which no stats were found
            Updates:
                - raw_ocr_bets.is_processed to 1 if NBA API call was successful for player_id and game_date

        Returns:
            True if processing completed successfully
        """
        logger.info("Starting to process new bets")
        
        try:
            # Validate before processing
            self.validate_data_integrity()
            
            self._process_unprocessed_bets()
            self.conn.commit()
            
            # Validate after processing
            self.validate_data_integrity()
            
            logger.info("Successfully processed all new bets")
            return True
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Transaction rolled back. Error processing bets: {str(e)}", exc_info=True)
            return False

    def validate_data_integrity(self) -> None:
        """Validate database integrity, logging any issues found:

        @TODO: Remove redundant checks and consider additional checks.

        Data Quality Checks:
        - Players table contains null nba_api_id
        - Game stats contains duplicate player_id and date combinations
        - Raw bets marked as processed are not in bet_results or unplayed_bets
        - Bet lines have valid over/under format
        - Scores are within valid ranges
        - Dates are not in the future
        - Odds are within valid ranges
        - No orphaned records in bet_results or game_stats
        - No duplicate bets for same player/date/type
        - All required bet fields are non-null
        
        Database:
            - Reads from raw_ocr_bets, players, game_stats, bet_results, unplayed_bets
        """
        queries = [
            # 1. raw_ocr_bets player id is not in players table
            """
            SELECT r.id, r.player_id 
            FROM raw_ocr_bets r 
            LEFT JOIN players p ON r.player_id = p.nba_api_id 
            WHERE r.is_voided = 0 AND r.needs_review = 0 AND p.nba_api_id IS NULL
            """,

            # 2. game_stats contains duplicate player_id and date combinations
            """
            SELECT player_id, date, COUNT(*) 
            FROM game_stats 
            GROUP BY player_id, date 
            HAVING COUNT(*) > 1
            """,

            # 3. raw_ocr_bets is_processed = 1 but not in results
            """
            SELECT id FROM raw_ocr_bets 
            WHERE is_processed = 1 
            AND id NOT IN (
                SELECT raw_bet_id FROM bet_results 
                UNION 
                SELECT raw_bet_id FROM unplayed_bets
            )
            """,

            # 4. Invalid bet line format (must start with 'o' or 'u' followed by number and .5)
            """
            SELECT id, bet_line 
            FROM raw_ocr_bets 
            WHERE (bet_line NOT LIKE 'o%.5' AND bet_line NOT LIKE 'u%.5')
            AND is_voided = 0 AND needs_review = 0
            """,

            # 5. Scores outside valid range (20-100)
            """
            SELECT id, score 
            FROM raw_ocr_bets 
            WHERE (score < 20 OR score > 100)
            AND is_voided = 0 AND needs_review = 0
            """,

            # 6. Future dates
            """
            SELECT id, date 
            FROM raw_ocr_bets 
            WHERE date > date('now')
            AND is_voided = 0 AND needs_review = 0
            """,

            # 7. Invalid odds ranges (-1000 to +1000)
            """
            SELECT id, odds 
            FROM raw_ocr_bets 
            WHERE (odds < -1000 OR odds > 1000)
            AND is_voided = 0 AND needs_review = 0
            """,

            # 8. Orphaned records in bet_results
            """
            SELECT br.id, br.raw_bet_id
            FROM bet_results br
            LEFT JOIN raw_ocr_bets r ON br.raw_bet_id = r.id
            WHERE r.id IS NULL
            """,

            # 9. Orphaned records in game_stats
            """
            SELECT gs.id, gs.player_id
            FROM game_stats gs
            LEFT JOIN players p ON gs.player_id = p.nba_api_id
            WHERE p.nba_api_id IS NULL
            """,

            # 10. Duplicate bets for same player/date/type (not necessarily same bet)
            """
            SELECT player_id, date, bet_type, COUNT(*) as count
            FROM raw_ocr_bets
            WHERE is_voided = 0 AND needs_review = 0
            GROUP BY player_id, date, bet_type
            HAVING COUNT(*) > 1
            """,

            # 11. Missing required fields
            """
            SELECT id,
                CASE 
                    WHEN player_id IS NULL THEN 'player_id'
                    WHEN bet_type IS NULL THEN 'bet_type'
                    WHEN score IS NULL THEN 'score'
                    WHEN date IS NULL THEN 'date'
                    WHEN bet_line IS NULL THEN 'bet_line'
                    WHEN odds IS NULL THEN 'odds'
                END as missing_field
            FROM raw_ocr_bets
            WHERE (player_id IS NULL OR bet_type IS NULL OR score IS NULL 
                  OR date IS NULL OR bet_line IS NULL OR odds IS NULL)
            AND is_voided = 0 AND needs_review = 0
            """
        ]
        
        validation_messages = {
            0: "Invalid player IDs found",
            1: "uplicate game stats found",
            2: "Processed bets missing results",
            3: "Invalid bet line formats",
            4: "Scores outside valid range",
            5: "Future dates found",
            6: "Invalid odds ranges",
            7: "Orphaned bet results",
            8: "Orphaned game stats",
            9: "Potential duplicate bets found",
            10: "Missing required fields"
        }
        
        for i, query in enumerate(queries):
            try:
                results = self.conn.execute(query).fetchall()
                if results:
                    logger.warning(f"Data integrity issue - {validation_messages[i]}: {results}")
            except Exception as e:
                logger.error(f"Error running validation query {i}: {str(e)}")

    def _process_unprocessed_bets(self) -> None:
        """Process bets that haven't been processed yet.
        
        - Reads raw_ocr_bets where is_processed = 0, is_voided = 0, and needs_review = 0
        - Checks for cached data in game_stats and unplayed_bets tables
        - If no cached data, fetches from NBA API and updates game_stats or unplayed_bets tables
        - Marks bet as processed in raw_ocr_bets table
        """

        # Get unprocessed bets from raw_ocr_bets table
        unprocessed_ocr_bets = self._get_unprocessed_bets()
        if unprocessed_ocr_bets.empty:
            logger.info("No unprocessed bets found")
            return
        
        for _, ocr_bet in unprocessed_ocr_bets.iterrows():
            try:
                # Get player's game stats from game_stats, unplayed_bets, or NBA API.
                # Set in_db True if player's game stats came from DB and not API
                in_db, game_stats = self._get_player_game_stats(ocr_bet)

                # If stats don't exist
                if game_stats is None:
                    # Insert bet into unplayed_bets
                    logger.info(f"Inserting into *unplayed_bets* for bet {ocr_bet['id']}, player_id: {ocr_bet['player_id']}, date: {ocr_bet['date']}")
                    self.insert_into_unplayed_bets(ocr_bet)

                    # Mark bet as processed
                    logger.info(f"Marking bet {ocr_bet['id']} as processed")
                    self._mark_bet_processed(ocr_bet)
                
                #If stats exist
                else: 
                    # Insert bet into game_stats if it isn't already in DB
                    if not in_db:
                        logger.info(f"Inserting into *game_stats* for bet {ocr_bet['id']}, player_id: {ocr_bet['player_id']}, date: {ocr_bet['date']}")
                        self.insert_into_game_stats(ocr_bet, game_stats)

                    # Insert bet into bet_results and mark bet as processed
                    logger.info(f"Inserting into *bet_results* for bet {ocr_bet['id']}, player_id: {ocr_bet['player_id']}, date: {ocr_bet['date']}")
                    self.insert_into_bet_results(ocr_bet, game_stats)

                    # Mark bet as processed
                    logger.info(f"Marking bet {ocr_bet['id']} as processed")
                    self._mark_bet_processed(ocr_bet)

            except Exception as e:
                logger.error(f"Error processing bet {ocr_bet['id']}: {str(e)}")
                continue

    def insert_into_bet_results(self, ocr_bet: pd.Series, game_stats: Dict[str, Any]) -> None:
        """Insert results of bet and corresponding game_stats into bet_results table."""
        try:
            result, result_delta, stat_result, line_value, over_under = self._calculate_results(ocr_bet, game_stats)
            score_range = self._calculate_score_range(ocr_bet['score'])
            game_stats_id = self._get_game_stats_id(ocr_bet)
            
            with self.conn:
                self.conn.execute("""
                INSERT INTO bet_results (
                    raw_bet_id, player_id, game_stats_id, bet_type,
                    result, result_delta, score_range, over_under, stat_result, line_value
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    ocr_bet['id'], ocr_bet['player_id'], game_stats_id, ocr_bet['bet_type'],
                    result, result_delta, score_range, over_under, stat_result, line_value
                ))
                
        except Exception as e:
            logger.error(f"Error processing bet result for ocr bet {ocr_bet['id']}: {str(e)}", exc_info=True)
            raise

    def insert_into_game_stats(self, ocr_bet: pd.Series, game_stats: Dict[str, Any]) -> None:
        """Insert game stats into game_stats table"""

        try: 
            with self.conn: 
                self.conn.execute("""
                INSERT INTO game_stats (
                    player_id, date, points, assists, rebounds, blocks, steals, turnovers, three_pointers, par, pts_rebs, pts_asts, rebs_asts
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    ocr_bet['player_id'], 
                    ocr_bet['date'], 
                    game_stats['points'],
                    game_stats['assists'],
                    game_stats['rebounds'],
                    game_stats['blocks'],
                    game_stats['steals'],
                    game_stats['turnovers'],
                    game_stats['three_pointers'],
                    game_stats['par'],
                    game_stats['pts_rebs'],
                    game_stats['pts_asts'],
                    game_stats['rebs_asts']
                ))
        except Exception as e:
            logger.error(f"Error inserting game stats for {ocr_bet['player_id']} on {ocr_bet['date']}: {str(e)}")
            raise
  
    def insert_into_unplayed_bets(self, ocr_bet: pd.Series) -> None:
        """Add bet to unplayed_bets table.""" 
        try:
            with self.conn:
                self.conn.execute("""
                INSERT OR IGNORE INTO unplayed_bets (raw_bet_id, player_id, date)
                VALUES (?, ?, ?)
                """, (ocr_bet['id'], ocr_bet['player_id'], ocr_bet['date']))
                
        except Exception as e:
            logger.error(f"Error marking unplayed bet id: {ocr_bet['id']}, player_id: {ocr_bet['player_id']}, date: {ocr_bet['date']}: {str(e)}")
            raise

    def _standardize_stat_names(self, api_stats: Dict[str, float]) -> Dict[str, float]:
        """Standardize api_stat names, add derived stats, and return a dictionary with numeric values."""

        # Map of NBA API stat names to standardized stat names
        bet_type_map = {
            'PTS': 'points',
            'AST': 'assists',
            'REB': 'rebounds',
            'BLK': 'blocks',
            'STL': 'steals',
            'TOV': 'turnovers',
            'FG3M': 'three_pointers'
        }
        
        # Create new dict with renamed keys and ensure float values
        renamed_stats = {}
        for api_key, new_key in bet_type_map.items():
            renamed_stats[new_key] = float(api_stats[api_key])
        
        # Ensure all base stats exist with at least 0.0
        # @TODO: Redundant? Previously had errors with derived stats.
        for stat in ['points', 'assists', 'rebounds']:  
            renamed_stats[stat] = renamed_stats.get(stat, 0.0)
        
        # Add derived stats
        renamed_stats['par'] = renamed_stats['points'] + renamed_stats['rebounds'] + renamed_stats['assists']
        renamed_stats['pts_rebs'] = renamed_stats['points'] + renamed_stats['rebounds']
        renamed_stats['pts_asts'] = renamed_stats['points'] + renamed_stats['assists']
        renamed_stats['rebs_asts'] = renamed_stats['rebounds'] + renamed_stats['assists']

        return renamed_stats

    def _get_unprocessed_bets(self) -> pd.DataFrame:
        """Fetch bets from raw_ocr_bets table where is_processed=0, is_voided=0, and needs_review=0."""
        query = """
        SELECT id, player_id, bet_type, score, date, bet_line, odds
        FROM raw_ocr_bets
        WHERE is_processed = 0 AND needs_review = 0 AND is_voided = 0
        """
        return pd.read_sql_query(query, self.conn)

    def _get_game_stats_id(self, ocr_bet: pd.Series) -> int:
        """Get the id of the game stats record for a bet."""
        query = """
        SELECT id FROM game_stats
        WHERE player_id = ? AND date = ?
        """
        result = self.conn.execute(query, (ocr_bet['player_id'], ocr_bet['date'])).fetchone()[0]
        return result

    def _get_player_game_stats(self, ocr_bet: pd.Series) -> Tuple[bool, Optional[Dict[str, float]]]:
        """Fetch game statistics from cache or NBA API for a bet.

        Returns:
            Tuple of (bool, Dict {stat_name: stat_value})
            bool: True if player/date is already in game_stats or unplayed_bets table, False otherwise
            Dict: {stat_name: stat_value} for player/date of bet if bool is False, None otherwise
        """
        # Check if stats are cached in game_stats table (i.e. we processed this player/date before)
        cached_stats = self._get_cached_stats(ocr_bet)
        if cached_stats:
            logger.info(f"Found cached stats for bet {ocr_bet['id']}, player_id: {ocr_bet['player_id']}, date: {ocr_bet['date']}")
            return True, cached_stats

        # Check if player/date is already marked as unplayed
        if self._in_unplayed_bets(ocr_bet):
            logger.info(f"Found unplayed bet for player_id: {ocr_bet['player_id']}, date: {ocr_bet['date']}")
            return True, None

        # Get stats from API
        logger.info(f"Fetching stats from API for bet {ocr_bet['id']}, player_id: {ocr_bet['player_id']}, date: {ocr_bet['date']}")
        return False, self._get_api_stats(ocr_bet)

    def _get_cached_stats(self, ocr_bet: pd.Series) -> Optional[Dict[str, float]]:
        """Get cached game stats from database if they exist."""
        query = """
        SELECT points, assists, rebounds, blocks, steals, turnovers, three_pointers,
               par, pts_rebs, pts_asts, rebs_asts
        FROM game_stats
        WHERE player_id = ? AND date = ?
        """
        try:
            cached_stats = self.conn.execute(query, (ocr_bet['player_id'], ocr_bet['date'])).fetchone()
            if cached_stats:
                return {
                    'points': float(cached_stats[0]),
                    'assists': float(cached_stats[1]),
                    'rebounds': float(cached_stats[2]),
                    'blocks': float(cached_stats[3]),
                    'steals': float(cached_stats[4]),
                    'turnovers': float(cached_stats[5]),
                    'three_pointers': float(cached_stats[6]),
                    'par': float(cached_stats[7]),
                    'pts_rebs': float(cached_stats[8]),
                    'pts_asts': float(cached_stats[9]),
                    'rebs_asts': float(cached_stats[10])
                }
            return None
        except Exception as e:
            logger.error(f"Error accessing cached stats for bet {ocr_bet['id']}: {str(e)}")
            return None

    def _in_unplayed_bets(self, ocr_bet: pd.Series) -> bool:
        """Check if bet is marked as unplayed for the given date."""
        query = """
        SELECT r.player_id, r.date
        FROM raw_ocr_bets r
        INNER JOIN unplayed_bets u ON r.player_id = u.player_id AND r.date = u.date
        WHERE r.player_id = ? AND r.date = ?
        """
        try:
            result = self.conn.execute(query, (ocr_bet['player_id'], ocr_bet['date'])).fetchone()
            if result:
                logger.info(f"Player {ocr_bet['player_id']} on {ocr_bet['date']} is in unplayed_bets table")
                return True
            return False
        except Exception as e:
            logger.error(f"Error checking if player is in unplayed_bets table for bet {ocr_bet['id']}, player_id: {ocr_bet['player_id']}, date: {ocr_bet['date']}: {str(e)}")
            return False

    def _get_api_stats(self, ocr_bet: pd.Series) -> Optional[Dict[str, float]]:
        """Fetch game stats from NBA API for the given bet."""
        try:
            # Wait for API to not be rate limited
            sleep(uniform(0.5, 1))

            # Pull player game log from NBA API
            gamelog = playergamelog.PlayerGameLog(
                player_id=int(ocr_bet['player_id']),
                season=SEASON
            )
            game_log_dfs = gamelog.get_data_frames()
            if not game_log_dfs or game_log_dfs[0].empty:
                raise ValueError(f"No API stats found for player {ocr_bet['player_id']} on {ocr_bet['date']}")
            
            # Ensure both dates are in YYYY-MM-DD format
            game_log_df = game_log_dfs[0]
            game_log_df['GAME_DATE'] = pd.to_datetime(game_log_df['GAME_DATE'], format='%b %d, %Y').dt.strftime('%Y-%m-%d')
            game_date = pd.to_datetime(ocr_bet['date']).strftime('%Y-%m-%d')

            # Get stats for the specific date
            api_stats = game_log_df[game_log_df['GAME_DATE'] == game_date]
            if not api_stats.empty:
                logger.info(f"Found API stats for player {ocr_bet['player_id']} on {ocr_bet['date']}")
                return self._standardize_stat_names(api_stats.iloc[0].to_dict())
            
            logger.info(f"No API stats found for player {ocr_bet['player_id']} on {ocr_bet['date']}")
            return None

        except (Timeout, RequestException) as e:
            logger.error(f"API error for bet {ocr_bet['id']}: {str(e)}")
            quit()
        except Exception as e:
            logger.error(f"Unexpected error processing bet {ocr_bet['id']}: {str(e)}", exc_info=True)
            raise

    def _calculate_results(self, ocr_bet: pd.Series, game_stats: Dict[str, Any]) -> Tuple[str, float, float, float, str]:
        """Calculate bet result and associated delta of ocr_bet based on game_stats"
        
        Returns: Tuple of [result, result_delta, stat_value, line_value, over_under]
        """
        
        # Value of stat being bet. (e.g. if bet_type = 'points' and points = 21, stat_value = 21) 
        stat_value = float(game_stats[ocr_bet['bet_type']])

        # Value of line being bet. (e.g. if bet_line = 'o21.5', line_value = 21.5)
        line_value = float(ocr_bet['bet_line'][1:])

        # Determine over/under 
        if ocr_bet['bet_line'].startswith('o'):
            over_under = "Over" 
        elif ocr_bet['bet_line'].startswith('u'):
            over_under = "Under"
        else:
            # @TODO: Check if redundant; we already validated this? 
            raise ValueError(f"Invalid bet line format: {ocr_bet['bet_line']}. Must start with 'o' or 'u'")
        
        result = (
            "Win" if (stat_value > line_value and over_under == "Over") or
                    (stat_value < line_value and over_under == "Under") else
            "Loss" if (stat_value < line_value and over_under == "Over") or
                    (stat_value > line_value and over_under == "Under") else
            "Push"
        )
        
        # Design decision: all bet lines have been .5, so 'push' result more likely to be an OCR error than a true push 
        if result == "Push":
            raise ValueError("Bet resulted in a push")
            
        # Calculate result delta based on bet odds
        if result == "Win": 
            result_delta = 100
        else:
            odds = float(ocr_bet['odds'])
            result_delta = -np.round((100 / (odds / 100)), 0) if odds > 0 else odds
            
        return result, result_delta, stat_value, line_value, over_under

    def _calculate_score_range(self, score: float) -> str:
        """Calculate score range category."""

        score_ranges = [
        (20, 25, "20-25"),
        (25, 30, "25-30"),
        (30, 35, "30-35"),
        (35, 40, "35-40"),
        (40, 45, "40-45"),
        (45, 50, "45-50")
        ]

        score = float(score)
        if score < 20:
            # @TODO: Check if redundant; we already validated this? 
            raise ValueError("Score is less than 20")
            
        for low, high, range_str in score_ranges:
            if low <= score < high:
                return range_str
                
        return "50+"

    def _mark_bet_processed(self, ocr_bet: pd.Series) -> None:
        """Mark bet as processed in raw_ocr_bets table."""
        self.conn.execute("""
        UPDATE raw_ocr_bets 
        SET is_processed = 1
        WHERE id = ?
        """, (ocr_bet['id'],))

    # Backup Methods
    def create_backup(self) -> None:
        """Create timestamped backup of database."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_path = Path('backups') / f'sports_bets_{timestamp}.db'
        backup_path.parent.mkdir(exist_ok=True)
        shutil.copy2(self.db_file, backup_path)
        logger.info(f"Created database backup: {backup_path}")

    def restore_from_backup(self, backup_file: str) -> None:
        """Restore database from backup file."""
        shutil.copy2(backup_file, self.db_file)
        logger.info(f"Restored database from backup: {backup_file}")