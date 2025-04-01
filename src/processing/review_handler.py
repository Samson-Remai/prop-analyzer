import pandas as pd
import sqlite3
from datetime import datetime
import shutil
from pathlib import Path
import sys
import os
from typing import List
import re

# Add the project root directory to Python path
project_root = str(Path(__file__).parent.parent.parent)
if project_root not in sys.path:
    sys.path.append(project_root)

from src.utils.config import (
    DB_PATH, IMAGES_DIR, REVIEW_IMAGES_DIR, REVIEW_CSV_DIR
)
from src.utils.logger import setup_logger
from src.utils.constants import TYPE_REPLACEMENTS, VALID_RANGES
logger = setup_logger(__name__)

class ReviewHandler:
    def __init__(self):
        """Initialize the ReviewHandler."""
        REVIEW_IMAGES_DIR.mkdir(parents=True, exist_ok=True)
        REVIEW_CSV_DIR.mkdir(parents=True, exist_ok=True)

    def get_entries_for_review(self):
        """Get all entries from raw_ocr_bets table that need review."""
        query = """
        SELECT 
            r.id,
            r.date, 
            r.bet_type,
            r.score, 
            r.bet_line, 
            r.odds, 
            p.name as player, 
            r.read_players,
            r.read_score_patterns,
            r.raw_text,
            r.image_source
        FROM raw_ocr_bets r
        LEFT JOIN players p ON r.player_id = p.nba_api_id
        WHERE r.needs_review = 1 
        AND r.is_voided = 0
        """
        
        with sqlite3.connect(DB_PATH) as conn:
            df = pd.read_sql_query(query, conn)
            if df.empty:
                logger.info("No entries need review")
            return df

    def _clear_review_folder(self):
        """Clear all files in the review images folder."""
        for img in REVIEW_IMAGES_DIR.glob('*'):
            try:
                img.unlink()  # Delete file
            except Exception as e:
                logger.error(f"Error deleting {img}: {e}")
        logger.info(f"Cleared review images folder: {REVIEW_IMAGES_DIR}")

    def export_for_review(self, output_file=None):
        """Export entries needing review to CSV and copy images to review folder."""
        df = self.get_entries_for_review()
        if df.empty:
            logger.info("No entries need review")
            return None

        # Generate output filename with timestamp if not provided
        if output_file is None:
            timestamp = datetime.now().strftime("%m-%d")
            output_file = REVIEW_CSV_DIR / f"reviews_{timestamp}.csv"

        # Export to CSV
        df.to_csv(output_file, index=False)
        
        # Copy images to review folder
        for img in df['image_source'].unique():
            try:
                source = IMAGES_DIR / img
                dest = REVIEW_IMAGES_DIR / img
                if source.exists():
                    shutil.copy2(source, dest)
            except Exception as e:
                logger.error(f"Error copying {img}: {e}")

        logger.info(f"Exported {len(df)} entries to {output_file}")
        logger.info(f"{len(df['image_source'].unique())} images copied to {REVIEW_IMAGES_DIR}")
        return output_file

    # @TODO: this is currently a near-duplicate of the function in image_processor.py
    def bet_needs_review(self, bet_entry: pd.Series) -> tuple[bool, List[str]]:
        """Determine if a bet needs manual review.
        A bet needs review if any of these conditions are met:
        - Missing or invalid player name
        - Player name has no player_id in players table
        - Missing or invalid bet type
        - Score is missing or outside range set in constants.py
        - Bet line is missing or invalid format
        - Odds are missing or outside range set in constants.py
        - Date is missing or invalid
        
        Args:
            bet_entry: Series containing the bet entry data
            
        Returns:
            Tuple of (needs_review, list of reasons for review)
        """
        reasons = set()

        # Check player
        player_name = bet_entry['player'] if not pd.isna(bet_entry['player']) else None
        if not player_name:
            reasons.add("Missing player name")

        # Check player_id
        if player_name:
            conn = sqlite3.connect(DB_PATH)
            player_id = conn.execute("""
                SELECT nba_api_id FROM players WHERE name = ?
            """, (player_name,)).fetchone()[0]
            if not player_id:
                reasons.add(f"Player not found: {player_name}")

        # Check bet type
        bet_type = bet_entry['bet_type'] if not pd.isna(bet_entry['bet_type']) else None
        if not bet_type:
            reasons.add("Missing bet type")
        elif bet_type not in TYPE_REPLACEMENTS.values():
            reasons.add(f"Invalid bet type: {bet_type}")
                
        # Check score
        if pd.isna(bet_entry['score']):
            reasons.add("Missing score")
        else:
            try:
                score_lower_bound, score_upper_bound = VALID_RANGES['score']
                score = float(bet_entry['score'])
                if not score_lower_bound <= score <= score_upper_bound:
                    reasons.add(f"Score out of expected range [{score_lower_bound},{score_upper_bound}]: {score}")
            except ValueError:
                reasons.add(f"Invalid score format: {bet_entry['score']}")
                    
        # Check bet line
        if pd.isna(bet_entry['bet_line']):
            reasons.add("Missing bet line")
        else:
            if not re.match(r'^[ou]\d+\.5$', str(bet_entry['bet_line'])):
                reasons.add(f"Invalid bet line format: {bet_entry['bet_line']}")
                    
        # Check odds
        if pd.isna(bet_entry['odds']):
            reasons.add("Missing odds")
        else:
            try:
                odds = int(bet_entry['odds'])
                odds_lower_bound, odds_upper_bound = VALID_RANGES['odds']
                if not odds_lower_bound <= odds <= odds_upper_bound:
                    reasons.add(f"Odds out of expected range [{odds_lower_bound},{odds_upper_bound}]: {odds}")
            except ValueError:
                reasons.add(f"Invalid odds format: {bet_entry['odds']}")
                    
        # Check date
        if pd.isna(bet_entry['date']):
            reasons.add("Missing date")
        else:
            try:
                pd.to_datetime(bet_entry['date'])
            except:
                reasons.add(f"Invalid date format: {bet_entry['date']}")

        return (len(reasons) > 0, list(reasons))

    def update_reviewed_entries(self, input_file):
        """Update database with reviewed entries.
        
        For entries in the csv file:
        - If entry exists in DB but not in CSV: Mark as voided in DB
        - If entry exists in CSV but is not updated: Keep in DB as needs_review = 1
        - If entry exists in CSV and is updated: Update in DB and set needs_review = 0
        
        Args:
            input_file: Path to CSV file containing reviewed entries
        """
        # If input file is not found, check if its in the review csv directory
        if not Path(input_file).exists():
            input_file = REVIEW_CSV_DIR / input_file
            if not input_file.exists():
                logger.error(f"Review file not found: {input_file}")
                return

        # Load reviewed entries from CSV
        csv_df = pd.read_csv(input_file)
        if csv_df.empty:
            logger.info("No entries to update")
            return

        with sqlite3.connect(DB_PATH) as conn:
            # Get all entries that currently need review
            db_df = pd.read_sql_query("""
                SELECT id, bet_type, score, bet_line, odds
                FROM raw_ocr_bets
                WHERE needs_review = 1 AND is_voided = 0
            """, conn)

            # Find entries deleted from CSV and mark as voided in DB
            deleted_ids = set(db_df['id']) - set(csv_df['id'])
            if deleted_ids:
                conn.execute("""
                    UPDATE raw_ocr_bets
                    SET is_voided = 1
                    WHERE id IN ({})
                """.format(','.join('?' * len(deleted_ids))), list(deleted_ids))
                logger.info(f"Marked {len(deleted_ids)} deleted entries as voided")

            # Process CSV entries
            for _, bet in csv_df.iterrows():
                try:        
                    # If entry is updated, update entry in database and mark as reviewed
                    needs_review, reasons = self.bet_needs_review(bet)

                    # Grab player ID of new name
                    player_id = conn.execute("""
                        SELECT nba_api_id FROM players WHERE name = ?
                    """, (bet['player'],)).fetchone()[0]

        
                    # Update entry in database  
                    if not needs_review: 
                        conn.execute("""
                        UPDATE raw_ocr_bets
                        SET needs_review = 0,
                            player_id = ?,
                            bet_type = ?,
                            score = ?,
                            bet_line = ?,
                            odds = ?
                        WHERE id = ?
                        """, (
                            player_id,
                            bet['bet_type'],
                            bet['score'],
                            bet['bet_line'],
                            bet['odds'],
                            bet['id']
                        ))
                        logger.info(f"Updated valid entry {bet['id']}")

                    # If entry is still invalid, ignore
                    else:
                        logger.debug(f"Ignoring invalid bet {bet['id']}: {reasons}")
                        
                except Exception as e:
                    logger.error(f"Error processing entry {bet['id']}: {e}")
            
            conn.commit()
        
        logger.info(f"Processed {len(csv_df)} reviewed entries")
        logger.info(f"Marked {len(deleted_ids)} entries as voided")
        logger.info(f"Updating csv and image folders...")
        
        # Clear review folder and export new entries
        self._clear_review_folder()
        self.export_for_review()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Handle review of betting entries')
    parser.add_argument('--export', action='store_true', help='Export entries needing review')
    parser.add_argument('--update', type=str, help='CSV file with reviewed entries')
    parser.add_argument('--output', type=str, help='Output file for export (optional)')
    
    args = parser.parse_args()
    handler = ReviewHandler()

    if args.export:
        handler.export_for_review(args.output)
    elif args.update:
        handler.update_reviewed_entries(args.update)
    else:
        logger.error("Please specify --export or --update")