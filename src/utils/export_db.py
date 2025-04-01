import sqlite3
import pandas as pd
from datetime import datetime
import os

def export_database(db_file="sports_bets.db", output_dir=None):
    """Export database tables to CSV files with readable player names and dates."""
    
    # Create timestamped output directory if not specified
    if output_dir is None:
        timestamp = datetime.now().strftime("%Y%m%d")
        output_dir = f"database_export_{timestamp}"
    
    os.makedirs(output_dir, exist_ok=True)
    
    conn = sqlite3.connect(db_file)
    
    try:
        # Dictionary of table names and their corresponding queries
        table_queries = {
            'raw_ocr_bets': """
                SELECT r.*, p.name as player_name
                FROM raw_ocr_bets r
                LEFT JOIN players p ON r.player_id = p.nba_api_id
            """,
            
            'bet_results': """
                SELECT br.*, p.name as player_name,
                       r.bet_line, r.odds
                FROM bet_results br
                LEFT JOIN players p ON br.player_id = p.nba_api_id
                LEFT JOIN raw_ocr_bets r ON br.raw_bet_id = r.id
            """,
            
            'game_stats': """
                SELECT gs.*, p.name as player_name
                FROM game_stats gs
                LEFT JOIN players p ON gs.player_id = p.nba_api_id
            """,
            
            'players': "SELECT * FROM players",
            'aggregated_results': "SELECT * FROM aggregated_results",
            
            # Additional filtered views
            'needs_review_bets': """
                SELECT r.*, p.name as player_name
                FROM raw_ocr_bets r
                LEFT JOIN players p ON r.player_id = p.nba_api_id
                WHERE r.needs_review = 1 AND r.is_voided = 0 AND r.is_processed = 0
            """,
            
            'voided_bets': """
                SELECT r.*, p.name as player_name
                FROM raw_ocr_bets r
                LEFT JOIN players p ON r.player_id = p.nba_api_id
                WHERE r.is_voided = 1
            """,

            'unplayed_bets': """
                SELECT ub.*, p.name as player_name
                FROM unplayed_bets ub
                LEFT JOIN players p ON ub.player_id = p.nba_api_id
            """,
            
            'unprocessed_bets': """
                SELECT r.*, p.name as player_name
                FROM raw_ocr_bets r
                LEFT JOIN players p ON r.player_id = p.nba_api_id
                WHERE r.is_processed = 0 AND r.is_voided = 0 AND r.needs_review = 0
            """,
            
            'processed_bets': """
                SELECT r.*, p.name as player_name
                FROM raw_ocr_bets r
                LEFT JOIN players p ON r.player_id = p.nba_api_id
                WHERE r.is_processed = 1
            """
        }
        
        # Export each table
        for table_name, query in table_queries.items():
            try:
                df = pd.read_sql_query(query, conn)
                output_file = os.path.join(output_dir, f"{table_name}.csv")
                df.to_csv(output_file, index=False)
                print(f"Exported {table_name}: {len(df)} rows")
            except Exception as e:
                print(f"Error exporting {table_name}: {str(e)}")
                
    finally:
        conn.close()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Export database tables to CSV')
    parser.add_argument('--db-file', type=str, help='Path to database file')
    parser.add_argument('--output-dir', type=str, help='Output directory for CSV files')
    
    args = parser.parse_args()
    export_database(
        db_file=args.db_file if args.db_file else "sports_bets.db",
        output_dir=args.output_dir
    )
