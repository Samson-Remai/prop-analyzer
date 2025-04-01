"""
Image processing module for OCR extraction of betting slip data.
"""

import re
import os
import json
from typing import Optional, List, Dict, Any

import pandas as pd
import sqlite3
import easyocr
import torch
from nba_api.stats.static import players

from src.database.init_db import init_database
from src.utils.config import DB_PATH, IMAGES_DIR
from src.utils.constants import (
    NAME_REPLACEMENTS,
    TYPE_REPLACEMENTS,
    ORDERED_OCR_BET_TYPES,
    VALID_RANGES
)
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

class ImageProcessor:
    """Handles OCR (Optical Character Recognition) processing of betting slip images and database operations.
    
    It uses EasyOCR to extract text from images and processes them into structured betting data, 
    maintaining a SQLite database of processed bets and player information.
    
    EasyOCR-related inconsistencies are flagged as needs_review = 1 to be manually reviewed in review_handler.py

    Unflagged bets will be processed by process_bets.py

    Database Tables:
        - raw_ocr_bets: Stores processed bet data from OCR 
        - players: Maintains active player list and IDs
    """
    
    def __init__(self) -> None:
        """Initialize the ImageProcessor.
            - Initializes database schema if it doesn't exists
            - Creates players and raw_ocr_bets tables
            - Fills players table with active NBA players and their IDs
        """
        init_database()
        self.reader = self._init_ocr_reader()
        self._cache_player_data()

    def process_folder(self, folder_path: str, year: int) -> bool:
        """Process all images in a folder.
        
        Args:
            folder_path: Directory containing image files
            year: Year to use for date processing
            
        Returns:
            True if processing completed (even if some images failed)
        
        """
        self.year = year
        processed_count = 0
        
        logger.info(f"Starting folder processing: {folder_path}")
        for filename in os.listdir(folder_path):
            if filename.lower().endswith(('.png', '.jpg', '.jpeg')):
                file_path = os.path.join(folder_path, filename)
                result = self.process_image(file_path)
                if result is not None:
                    processed_count += len(result)
        
        logger.info(f"Completed processing {processed_count} bets from {folder_path}")
        return True

    def process_image(self, file_path: str) -> Optional[pd.DataFrame]:
        """Process a single image and save results to database.

        - Generates raw OCR text
        - Extracts bet data from OCR text
        - Cleans and standardizes extracted betting data
        - Saves processed data to database (table: raw_ocr_bets) 
        
        Returns: fully processed bet data  
        """
        image_name = os.path.basename(file_path)
        logger.info(f"Processing image: {image_name}", exc_info=False)
        
        with sqlite3.connect(DB_PATH) as conn:
            # Return existing data if it exists and doesn't need review
            existing_data = self._get_existing_data(conn, image_name)
            if existing_data is not None:
                return existing_data
            
            raw_text = self._get_ocr_text(file_path)
            if not raw_text:
                return None

            try:
                extracted_data = self._extract_bet_data(raw_text)
                if not extracted_data:
                    logger.error(f"Failed to extract data from {image_name}")
                    return None

                extracted_data = pd.DataFrame(extracted_data)
                extracted_data['image_source'] = image_name
                
                cleaned_entries = self.clean_data(extracted_data)
                self._save_to_database(cleaned_entries)
                return cleaned_entries

            except Exception as e:
                logger.error(f"Error processing {image_name}: {str(e)}")
                return None

    def clean_data(self, extracted_data: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize extracted betting data, ignoring NA values

        Args:
            extracted_data: DataFrame containing betting data extracted from OCR text

        Returns:
            DataFrame with cleaned and standardized values:
            - Removes whitespace from score, odds, bet_line
            - Standardizes odds format (fixes OCR errors for minus signs)
            - Standardizes bet line format (fixes 0 vs o confusion)
            - Converts dates to YYYY-MM-DD format
            - Removes + and % from score values
        """
        df = extracted_data.copy()
        
        # Remove whitespace in string columns
        for col in ['score', 'odds', 'bet_line']:
            df.loc[df[col].notna(), col] = df.loc[df[col].notna(), col].apply(lambda x: x.replace(" ", ""))
        
        # Account for common OCR errors in odds
        df.loc[df['odds'].notna(), 'odds'] = (
            df.loc[df['odds'].notna(), 'odds']
            .str.replace(r'^[74~"]', '-', regex=True)  # Fix leading minus signs
            .str.replace(r'(?<!^)-', '4', regex=True)  # Fix non-leading 4s
            .astype('Int64')  # Use nullable integer type
        )
        
        # Account for common OCR errors in bet line
        df.loc[df['bet_line'].notna(), 'bet_line'] = (
            df.loc[df['bet_line'].notna(), 'bet_line']
            .str.replace(r'^0', 'o', regex=True)  # Fix 0 vs o confusion
        )
        
        # Clean score - remove + and %
        df.loc[df['score'].notna(), 'score'] = (
            df.loc[df['score'].notna(), 'score']
            .str[1:-1]  # Remove + and %
        )
        
        # Convert dates to YYYY-MM-DD
        df.loc[df['date'].notna(), 'date'] = pd.to_datetime(
                df.loc[df['date'].notna(), 'date'].apply(lambda x: f"{x}/{self.year}"),
                format='%m/%d/%Y'
            ).dt.strftime('%Y-%m-%d')

        return df

    def _extract_bet_data(self, raw_text: str) -> Optional[List[Dict[str, Any]]]:
        """Parses raw OCR text to extract structured bet information.

        Note that bet_type, scores, lines, odds, and players can have NA values if OCR misses something.
        In this case, we return NA for the entire column. 
            
        Returns:
            List of dictionaries containing structured bet data
            None if no dates or score_lines are found 
        """

        # Extract date M/DD or MM/DD 
        dates = re.findall(r'\b\d{1,2}/\d{2}\b', raw_text)
        if not dates:
            logger.error(f"No date found in {raw_text}")
            return None

        # Extract bet type and standardize
        bet_type = self._find_bet_type(raw_text) # None if no bet type found
        bet_type = TYPE_REPLACEMENTS.get(bet_type, bet_type) 

        # Extract score, line, and odds using one pattern to ignore bets with scores < 20
        score_pattern = r'(\+[2-9]\d\s*\.\s*\d{2}%)\s*([0|u|o]\d*\s*+\.\s*5)\s*([\-|\+|7|4|~|"]\d{3})'
        score_lines = re.findall(score_pattern, raw_text)
        
        if not score_lines:
            logger.error(f"No score_lines found in {raw_text}")
            return None

        # Extract scores, lines, and odds from that one pattern
        scores = [item[0] for item in score_lines]
        lines = [item[1] for item in score_lines]
        odds = [item[2] for item in score_lines]

        # Extract and clean player names
        players_in_text = self._find_players_in_text(raw_text)
        players_in_text = [NAME_REPLACEMENTS.get(item, item) for item in players_in_text]

        # Make columns NA if they don't match max length as OCR missed something
        max_length = max(len(players_in_text), len(lines), len(scores), len(odds))
        # (If one of scores, lines, and odds is NA, all will be NA, so this is a bit redundant, albeit harmless)
        scores = scores if len(scores) == max_length else [pd.NA] * max_length
        lines = lines if len(lines) == max_length else [pd.NA] * max_length
        odds = odds if len(odds) == max_length else [pd.NA] * max_length
        players = players_in_text if len(players_in_text) == max_length else [pd.NA] * max_length
        
        # Pad debug variables
        score_lines = score_lines if len(score_lines) == max_length else score_lines + [pd.NA] * (max_length - len(score_lines))
        players_in_text = players_in_text if len(players_in_text) == max_length else players_in_text + [pd.NA] * (max_length - len(players_in_text))

        return [{
            'date': dates[0],
            'bet_type': bet_type,
            'bet_line': lines[i],
            'score': scores[i],
            'odds': odds[i],
            'player': players[i],
            'read_score_patterns': score_lines[i],
            'read_players': players_in_text[i],
            'raw_text': raw_text
        } for i in range(max_length)]

    def _image_needs_review(self, image_df: pd.DataFrame) -> tuple[bool, List[str]]:
        """Determine if an image needs manual review.
        
        An image needs review if any bet in it needs review.
        All bets in an image share the same review status.
        
        Args:
            image_df: DataFrame containing all bets in an image
            
        Returns:
            Tuple of (needs_review, list of reasons for review)
        """
        all_reasons = []
        
        for _, bet_entry in image_df.iterrows():
            needs_review, reasons = self._bet_needs_review(bet_entry)
            if needs_review:
                all_reasons.extend(reasons)
        
        return (len(all_reasons) > 0, all_reasons)

    def _bet_needs_review(self, bet_entry: pd.Series) -> tuple[bool, List[str]]:
        """Determine if a bet needs manual review.
        
        A bet needs review if any of these conditions are met:
        - Missing or invalid player name
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
        if player_name and player_name not in self.player_ids:
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

    def _image_needs_review(self, image_df: pd.DataFrame) -> tuple[bool, List[str]]:
        """Determine if an image needs manual review.
        
        An image needs review if any bet in it needs review.
        All bets in an image share the same review status.
        
        Args:
            image_df: DataFrame containing all bets in an image
            
        Returns:
            Tuple of (needs_review, list of reasons for review)
        """
        all_reasons = set()
        
        for _, bet_entry in image_df.iterrows():
            needs_review, reasons = self._bet_needs_review(bet_entry)
            if needs_review:
                all_reasons.update(reasons)
        
        return (len(all_reasons) > 0, list(all_reasons))

    def _save_to_database(self, cleaned_entries: pd.DataFrame) -> None:
        """ Saves cleaned entries to raw_ocr_bets table 

        - Determines if each image needs review
        - Sets needs_review flag based on validation
        - Inserts each entry into raw_ocr_bets database table  
        """
        if cleaned_entries.empty:
            logger.warning("No data to save to database")
            return 
        
        images = cleaned_entries.groupby('image_source')

        with sqlite3.connect(DB_PATH) as conn:
            for image_name, image_df in images:
                # Check needs_review on an image level as all bets in the image will have same flag 
                needs_review, reasons = self._image_needs_review(image_df)
                if needs_review:
                    logger.warning(
                        f"Image needs review: {image_name}\n"
                        f"Review reasons: {', '.join(reasons)}"
                    )

                for _, bet_entry in image_df.iterrows():
                    try:
                        player_name = bet_entry['player'] if not pd.isna(bet_entry['player']) else None
                        player_id = self.player_ids.get(player_name) if player_name else None
                        
                        # Convert debug variables to JSON strings if they contain valid data
                        read_score_patterns = (
                            json.dumps(bet_entry['read_score_patterns']) 
                            if pd.notna(bet_entry['read_score_patterns'])
                            else None
                        )
                        read_players = (
                            json.dumps(bet_entry['read_players'])
                            if pd.notna(bet_entry['read_players'])
                            else None
                        )

                        conn.execute("""
                        INSERT OR IGNORE INTO raw_ocr_bets 
                        (player_id, bet_type, score, date, bet_line, odds, 
                        image_source, needs_review, raw_text, 
                        read_score_patterns, read_players)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            player_id,
                            bet_entry['bet_type'] if not pd.isna(bet_entry['bet_type']) else None,
                            float(bet_entry['score']) if not pd.isna(bet_entry['score']) else None,
                            bet_entry['date'] if not pd.isna(bet_entry['date']) else None,
                            bet_entry['bet_line'] if not pd.isna(bet_entry['bet_line']) else None,
                            int(bet_entry['odds']) if not pd.isna(bet_entry['odds']) else None,
                            bet_entry['image_source'],
                            needs_review,
                            bet_entry['raw_text'],
                            read_score_patterns,
                            read_players
                        ))
                        conn.commit()
                        
                    except Exception as e:
                        logger.error(f"Error saving bet to database: {str(e)}", exc_info=True)
                        logger.debug(f"Problematic entry: {bet_entry}")
                        conn.rollback()

    def _init_ocr_reader(self) -> easyocr.Reader:
        """Initialize OCR reader with GPU if available."""

        gpu = torch.cuda.is_available()
        if gpu:
            logger.info(f"GPU detected: {torch.cuda.get_device_name(0)}")
        else:
            logger.warning("No GPU detected, falling back to CPU")
            
        return easyocr.Reader(['en', 'fr', 'de', 'tr', 'hr', 'rs_latin', 'lt', 'lv','pl'], gpu=gpu)

    def _get_ocr_text(self, file_path: str) -> Optional[str]:
        """Extract text from image using OCR."""
        try:
            return ' '.join([result[1] for result in self.reader.readtext(file_path)])
        except Exception as e:
            logger.error(f"OCR failed at {file_path}: {str(e)}")
            return None

    def _cache_player_data(self) -> None:
        """Cache player data for efficient lookups.
        
        Maintains existing player data while checking for and adding new players:
        1. Loads existing players from database
        2. Fetches current active players from NBA API
        3. Identifies and adds any new players
        4. Updates player status (active/inactive) as needed
        """
        with sqlite3.connect(DB_PATH) as conn:
            # First load existing data
            existing_players = dict(conn.execute(
                "SELECT name, nba_api_id FROM players WHERE is_active = 1"
            ).fetchall())
            
            # Get current active players from API
            logger.info("Checking for new players from NBA API...")
            active_players = players.get_active_players()                       
            current_players = {p['full_name']: int(p['id']) for p in active_players} 
            
            # Find new players to add
            new_players = {
                name: player_id 
                for name, player_id in current_players.items() 
                if name not in existing_players
            }
            
            # Find players who are no longer active
            inactive_players = {
                name: player_id 
                for name, player_id in existing_players.items() 
                if name not in current_players
            }
            
            if new_players:
                # Add new players
                for player_name, nba_api_id in new_players.items():
                    conn.execute("""
                    INSERT OR IGNORE INTO players (name, nba_api_id, is_active)
                    VALUES (?, ?, 1)
                    """, (player_name, nba_api_id))
                logger.info(f"Added {len(new_players)} new players to database")
            
            if inactive_players:
                # Mark inactive players
                for player_name in inactive_players:
                    conn.execute("""
                    UPDATE players 
                    SET is_active = 0
                    WHERE name = ?
                    """, (player_name,))
                logger.info(f"Marked {len(inactive_players)} players as inactive")
            
            # Cache final data in memory
            self.player_ids = current_players # dict of player name and id
            self.all_players = list(current_players.keys()) + list(NAME_REPLACEMENTS.keys()) # list of player names

            logger.info(f"Player cache updated. Total active players: {len(self.player_ids)}", exc_info=False)
            conn.commit()

    def _get_existing_data(self, conn: sqlite3.Connection, image_name: str) -> Optional[pd.DataFrame]:    
        """
        IF image is in raw_ocr_bets and doesn't need review: returns existing data for image
        ELSE deletes image from raw_ocr_bets and returns None
        """

        query = """
        SELECT * FROM raw_ocr_bets 
        WHERE image_source = ? AND needs_review = 0
        """
        df = pd.read_sql_query(query, conn, params=(image_name,))
        
        if not df.empty:
            logger.info(f"Using existing data for {image_name}")
            return df
            
        conn.execute("DELETE FROM raw_ocr_bets WHERE image_source = ?", (image_name,))
        conn.commit()
        return None

    def _find_bet_type(self, raw_text: str) -> Optional[str]:
        """Returns first ORDERED_OCR_BET_TYPE to appear in raw_text; None if no match.

        @TODO: Could be more efficient? We call this for each image, and search through raw text for each bet type

        Normalizes spaces around '+' to handle variations like:
        - "Pts + Reb + Ast"
        - "Pts+Reb+Ast"
        - "Pts+ Reb +Ast"
        """
        raw_text = raw_text.lower()
        raw_text = re.sub(r'\s*\+\s*', '+', raw_text)
        
        for search_string in ORDERED_OCR_BET_TYPES:
            if search_string.lower() in raw_text:
                return search_string

        return None

    def _find_players_in_text(self, raw_text: str) -> List[str]:
        """Returns all_players ordered by appearance in raw_text
        @TODO: Could be more efficient. 
               We're currently iterating over all players and checking if they're in the raw_text for each image
        """
        raw_text = raw_text.lower()
        return sorted(
            [s for s in self.all_players if s.lower() in raw_text],
            key=lambda x: raw_text.index(x.lower())
        )

if __name__ == "__main__":
    processor = ImageProcessor()
    processor.process_folder(IMAGES_DIR / '2025', 2025)
