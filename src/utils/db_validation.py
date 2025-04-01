import sqlite3
import pandas as pd
from datetime import datetime, date
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('database_validation.log'),
        logging.StreamHandler()
    ]
)

def validate_database(db_file="sports_bets.db"):
    """Validate database integrity and check for potential issues"""
    conn = sqlite3.connect(db_file)
    issues_found = False

    def log_issue(message):
        nonlocal issues_found
        issues_found = True
        logging.error(message)

    try:
        # 1. Check for orphaned records
        logging.info("Checking for orphaned records...")
        
        orphaned_bets = pd.read_sql("""
            SELECT id, player_id 
            FROM raw_ocr_bets r
            WHERE player_id NOT IN (SELECT nba_api_id FROM players)
            AND player_id IS NOT NULL
        """, conn)
        if not orphaned_bets.empty:
            log_issue(f"Found {len(orphaned_bets)} bets with invalid player_ids")
            print(orphaned_bets)

        # 2. Check for inconsistent processing status
        logging.info("Checking processing status consistency...")
        
        inconsistent_status = pd.read_sql("""
            SELECT id, is_processed, needs_review, is_voided
            FROM raw_ocr_bets
            WHERE (is_processed = 1 AND needs_review = 1)
            OR (is_processed = 1 AND is_voided = 1)
            OR (needs_review = 1 AND is_voided = 1)
        """, conn)
        if not inconsistent_status.empty:
            log_issue(f"Found {len(inconsistent_status)} bets with inconsistent status flags")
            print(inconsistent_status)

        # 3. Check for duplicate entries
        logging.info("Checking for duplicate bets...")
        
        duplicates = pd.read_sql("""
            SELECT p.name, r.date, r.bet_type, r.bet_line, r.score, COUNT(*) as count
            FROM raw_ocr_bets r
            JOIN players p ON r.player_id = p.nba_api_id
            WHERE r.is_voided = 0 AND r.needs_review = 0
            GROUP BY r.player_id, r.date, r.bet_type, r.bet_line, r.score, p.name
            HAVING count > 1
        """, conn)
        if not duplicates.empty:
            log_issue(f"Found {len(duplicates)} potential duplicate bets")
            print(duplicates)

        # 4. Validate bet_results integrity
        logging.info("Validating bet results...")
        
        invalid_results = pd.read_sql("""
            SELECT br.id, br.result, br.result_delta, r.bet_line
            FROM bet_results br
            JOIN raw_ocr_bets r ON br.raw_bet_id = r.id
            WHERE (br.result = 'Win' AND br.result_delta != 100)
            OR (br.result = 'Loss' AND br.result_delta >= 0)
            OR (br.result = 'Push' AND br.result_delta != 0)
        """, conn)
        if not invalid_results.empty:
            log_issue(f"Found {len(invalid_results)} inconsistent bet results")
            print(invalid_results)

        # 5. Check for missing game stats
        logging.info("Checking for missing game stats...")
        
        missing_stats = pd.read_sql("""
            SELECT br.id, br.player_id, br.game_stats_id
            FROM bet_results br
            LEFT JOIN game_stats gs ON br.game_stats_id = gs.id
            WHERE gs.id IS NULL
        """, conn)
        if not missing_stats.empty:
            log_issue(f"Found {len(missing_stats)} bet results with missing game stats")
            print(missing_stats)

        # 6. Validate date ranges
        logging.info("Validating date ranges...")
        
        invalid_dates = pd.read_sql("""
            SELECT id, date 
            FROM raw_ocr_bets
            WHERE date > CURRENT_DATE
            OR date < '2023-01-01'
        """, conn)
        if not invalid_dates.empty:
            log_issue(f"Found {len(invalid_dates)} bets with suspicious dates")
            print(invalid_dates)

        # 7. Check for unprocessed bets in unplayed_bets
        logging.info("Checking unplayed bets consistency...")
        
        unplayed_issues = pd.read_sql("""
            SELECT r.id, r.is_processed
            FROM raw_ocr_bets r
            JOIN unplayed_bets u ON r.id = u.raw_bet_id
            WHERE r.is_processed = 0
        """, conn)
        if not unplayed_issues.empty:
            log_issue(f"Found {len(unplayed_issues)} unplayed bets marked as unprocessed")
            print(unplayed_issues)

        # 8. Validate bet line format
        logging.info("Validating bet line format...")
        
        invalid_lines = pd.read_sql("""
            SELECT id, bet_line
            FROM raw_ocr_bets
            WHERE bet_line NOT LIKE 'o%' 
            AND bet_line NOT LIKE 'u%'
            AND bet_line IS NOT NULL
        """, conn)
        if not invalid_lines.empty:
            log_issue(f"Found {len(invalid_lines)} bets with invalid bet line format")
            print(invalid_lines)

        # 9. Summary statistics
        logging.info("Generating summary statistics...")
        
        for table in ['raw_ocr_bets', 'bet_results', 'game_stats', 'unplayed_bets', 'players']:
            count = pd.read_sql(f"SELECT COUNT(*) as count FROM {table}", conn).iloc[0]['count']
            logging.info(f"{table}: {count} rows")

        if not issues_found:
            logging.info("No issues found in database validation!")
        else:
            logging.warning("Database validation completed with issues. Check the log for details.")

    except Exception as e:
        logging.error(f"Error during validation: {str(e)}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    validate_database()