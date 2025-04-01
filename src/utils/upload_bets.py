"""
Google Sheets integration for uploading and tracking bet results.
"""

import json
import sqlite3
from typing import Dict, List, Tuple, Any

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

from src.utils.logger import setup_logger
from src.utils.config import DB_PATH, CREDENTIALS_PATH, UPDATES_DIR
from src.utils.sensitive.sheets_data import SHEET_NAME, WORKSHEET_NAME, SHEET_CELL_MAPPING

logger = setup_logger(__name__)

def upload_to_sheets(dry_run=False) -> None:
    """Upload results to Google Sheets

    Calculates the sum of results and counts the number of bets for each bet type and score range, corresponding to a cell in the Google Sheet.
    Updates Google Sheet with the adjusted results and volumes.
    Marks the bets as uploaded in the bet_results table.
    Logs the summary of all updates
    
    Args:
        dry_run (bool): If True, log changes without applying them
    
    Database:
        Reads from raw_ocr_bets (raw_bet_id, bet_line, odds, date, score), players (name, nba_api_id), bet_results (id, is_uploaded)
        Updates bet_results table (is_uploaded)

    Returns:    
        bool: True if upload successful, False otherwise
    """
    conn = None
    try:
        scope = ["https://www.googleapis.com/auth/spreadsheets", 
                "https://www.googleapis.com/auth/drive"]

        creds = Credentials.from_service_account_file(
            CREDENTIALS_PATH,
            scopes=scope
        )
        client = gspread.authorize(creds)
        sheet = client.open(SHEET_NAME)
        worksheet = sheet.worksheet(WORKSHEET_NAME)

        conn = sqlite3.connect(DB_PATH)

        unuploaded_bets = _get_unuploaded_bets(conn)
        if unuploaded_bets.empty:
            logger.info("No unuploaded bets to process.")
            return True

        grouped_bets = _group_bets(unuploaded_bets)
        last_date = unuploaded_bets["date"].max()
        logger.info(f"Processing {len(unuploaded_bets)} new uploads")

        updates = _get_updates(worksheet, grouped_bets, last_date)

        if not dry_run:
            worksheet.batch_update(updates)
            _mark_bets_as_uploaded(conn, unuploaded_bets['bet_id'])
            logger.info(f"Batch update completed successfully with {len(unuploaded_bets)} uploads")
            
        return True

    except Exception as e:
        logger.error(f"Upload failed: {str(e)}", exc_info=True)
        return False

    finally:
        if conn:
            conn.close()

def _get_unuploaded_bets(conn) -> pd.DataFrame:
    """Get unuploaded bet results from database."""
    query = """
    SELECT br.id as bet_id, 
        p.name as player, 
        br.bet_type, 
        br.result, 
        br.result_delta, 
        br.score_range, 
        r.bet_line, 
        r.odds, 
        r.date,
        r.score
    FROM bet_results br
    JOIN raw_ocr_bets r ON br.raw_bet_id = r.id
    JOIN players p ON br.player_id = p.nba_api_id
    WHERE br.is_uploaded = 0
    ORDER BY br.id 
    """
    return pd.read_sql_query(query, conn)

def _group_bets(bets: pd.DataFrame) -> pd.DataFrame:
    """Group bets by bet_type and score_range and aggregate results and volumes."""
    return bets.groupby(['bet_type', 'score_range']).agg(
        ResultSum=('result_delta', 'sum'),
        Count=('result_delta', 'size')
    ).reset_index()

def _get_current_values(worksheet: gspread.Worksheet, grouped: pd.DataFrame) -> tuple[dict, dict]:
    """
    Get current values from Google Sheet for results and volumes of grouped. 
    
    Args:
        worksheet: Google Sheet worksheet
        grouped: DataFrame of grouped bets
        
    Returns:
        tuple[results, volumes] where 
            results = {cell location: current result}
            volumes = {cell location: current volume}
    """
    # Get cell location of result and volume for each grouped bet
    result_cells = [SHEET_CELL_MAPPING[(row["bet_type"], row["score_range"], "Result")] 
                    for _, row in grouped.iterrows()]
    volume_cells = [SHEET_CELL_MAPPING[(row["bet_type"], row["score_range"], "Volume")] 
                    for _, row in grouped.iterrows()]

    # Batch get all values in result_cells and volume_cells
    all_cells = result_cells + volume_cells
    all_values = worksheet.batch_get(all_cells)

    # Split and process values
    results_values = all_values[:len(result_cells)]
    volumes_values = all_values[len(result_cells):]

    # Create maps of cell locations to current values
    current_results = {cell: int(val[0][0].replace('$', '').replace(',', '')) 
                 for cell, val in zip(result_cells, results_values)}
    current_volumes = {cell: int(val[0][0]) 
                 for cell, val in zip(volume_cells, volumes_values)}

    return current_results, current_volumes

def _save_aggregated_results(bet_group: pd.Series, last_date: str, new_result: int, new_volume: int) -> None:
    """Save aggregated results to the database. Updates existing records if sheet_cells exist, otherwise inserts new records."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        # Save result cell data
        result_cell = SHEET_CELL_MAPPING[(bet_group["bet_type"], bet_group["score_range"], "Result")]
        cursor.execute("""
        INSERT INTO aggregated_results 
            (sheet_cell, bet_type, score_range, volume, result, updated_to, processed_at)
        VALUES (?, ?, ?, NULL, ?, ?, ?)
        ON CONFLICT(sheet_cell) DO UPDATE SET
            result = excluded.result,
            updated_to = excluded.updated_to,
            processed_at = excluded.processed_at
        """, (
            result_cell,
            bet_group["bet_type"],
            bet_group["score_range"],
            new_result,
            last_date,
            pd.Timestamp.now().strftime('%Y-%m-%d')   
        ))

        # Save volume cell data
        volume_cell = SHEET_CELL_MAPPING[(bet_group["bet_type"], bet_group["score_range"], "Volume")]
        cursor.execute("""
        INSERT INTO aggregated_results 
            (sheet_cell, bet_type, score_range, volume, result, updated_to, processed_at)
        VALUES (?, ?, ?, ?, NULL, ?, ?)
        ON CONFLICT(sheet_cell) DO UPDATE SET
            volume = excluded.volume,
            updated_to = excluded.updated_to,
            processed_at = excluded.processed_at
        """, (
            volume_cell,
            bet_group["bet_type"],
            bet_group["score_range"],
            new_volume,
            last_date,
            pd.Timestamp.now().strftime('%Y-%m-%d')   
        ))

        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Error saving aggregated results: {str(e)}")
        raise
    finally:
        conn.close()

def _create_updates(grouped_bets: pd.DataFrame, current_results: dict, current_volumes: dict, last_date: str, dry_run: bool) -> list[dict]:
    """Create updates for Google Sheets.
    
    Args:
        grouped_bets: DataFrame of bets grouped by bet_type and score_range
        current_results: Dictionary of current results {cell_location: result}
        current_volumes: Dictionary of current volumes {cell_location: volume}
        last_date: Date of the last processed bet
        dry_run: Boolean indicating if the update is a dry run 
    
    Returns:
        list[dict]: List of update dictionaries {range: [values]}
    """
    updates = []
    update_records = []
    for _, bet_group in grouped_bets.iterrows():
        # Locations for updates
        result_cell = SHEET_CELL_MAPPING[(bet_group["bet_type"], bet_group["score_range"], "Result")]
        volume_cell = SHEET_CELL_MAPPING[(bet_group["bet_type"], bet_group["score_range"], "Volume")]

        # Current values to be updated
        curr_result = current_results[result_cell]
        curr_volume = current_volumes[volume_cell]

        # Calculate new values
        result_delta = bet_group["ResultSum"]
        volume_delta = bet_group["Count"]
        new_result = curr_result + result_delta
        new_volume = curr_volume + volume_delta

        # Update aggregated results in database
        _save_aggregated_results(bet_group, last_date, new_result, new_volume)

        # Collect update information for logging
        update_records.extend([
            {
                'Bet Type': bet_group['bet_type'],
                'Score Range': bet_group['score_range'],
                'Location': result_cell,
                'Type': 'Result',
                'Current': curr_result,
                'Change': result_delta,
                'New Value': new_result
            },
            {
                'Bet Type': bet_group['bet_type'],
                'Score Range': bet_group['score_range'],
                'Location': volume_cell,
                'Type': 'Volume',
                'Current': curr_volume,
                'Change': volume_delta,
                'New Value': new_volume
            }
        ])

        # Add updates to return list (naming required by Google Sheets API)
        updates.append({"range": result_cell, "values": [[new_result]]})
        updates.append({"range": volume_cell, "values": [[new_volume]]})

    _log_updates(update_records, dry_run)

    return updates

def _log_updates(update_records: list[dict], dry_run: bool) -> None:
    """Log update information using a DataFrame."""
    # Create and log summary DataFrame
    update_df = pd.DataFrame(update_records)
    
    # Ensure full DataFrame is displayed in logger
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        logger.info("\nUpdate Summary:")
        logger.info("\n" + str(update_df))
    
    # Save to CSV with timestamp
    timestamp = pd.Timestamp.now().strftime('%Y%m%d')
    csv_path = UPDATES_DIR / f'updates_{timestamp}.csv'
    update_df.to_csv(csv_path, index=False)
    logger.info(f"Update details saved to: {csv_path}")
    
    if dry_run:
        logger.info("\nDRY RUN - No changes will be made to the database")

def _get_updates(worksheet: gspread.Worksheet, grouped: pd.DataFrame, last_date: str) -> list[dict]:
    """
    Get batch updates for Google Sheets.
    
    Args:
        worksheet: Google Sheet worksheet
        grouped: DataFrame of grouped bets        
    Returns:
        list[dict]: List of update dictionaries
    """
    current_results, current_volumes = _get_current_values(worksheet, grouped)
    updates = _create_updates(grouped, current_results, current_volumes, last_date, dry_run=False)
    return updates

def _mark_bets_as_uploaded(conn, bet_ids: list[int]):
    """Mark bets as uploaded in bet_results table."""
    cursor = conn.cursor()
    try:
        cursor.executemany("""
            UPDATE bet_results 
            SET is_uploaded = 1 
            WHERE id = ?
        """, [(id,) for id in bet_ids])
        conn.commit()
        logger.info(f"{len(bet_ids)} bets marked as uploaded in database")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to mark bets as uploaded: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    success = upload_to_sheets()