"""
Main entry point for the NBA Props Analyzer.
"""

import argparse
from pathlib import Path

from src.ocr.image_processor import ImageProcessor
from src.processing.process_bets import BetProcessor
from src.utils.upload_bets import upload_to_sheets
from src.utils.logger import setup_logger
    
logger = setup_logger(__name__)

def process_images(folder_path: str, year: int, reprocess: bool = False) -> bool:
    """Process betting slip images through OCR.
    
    Args:
        folder_path: Path to folder containing betting slip images
        year: Year of bet image to use for date processing
        reprocess: If True, reprocess entries previously marked for review
        
    Returns:
        bool: True if processing completed successfully
    """
    ocr_processor = ImageProcessor()
    if reprocess:
        return ocr_processor.reprocess_flagged_entries()
    return ocr_processor.process_folder(folder_path, year)

def update_database() -> bool:
    """Process unprocessed bets and update database with results.
    
    Returns:
        bool: True if database update completed successfully
    """
    processor = BetProcessor()
    return processor.process_new_bets()

def upload_results(dry_run: bool = False) -> bool:
    """Upload processed results to Google Sheets.
    
    Args:
        dry_run: If True, log changes without applying them
        
    Returns:
        bool: True if upload completed successfully
    """
    return upload_to_sheets(dry_run)

def main():
    """Main workflow orchestration."""
    
    parser = argparse.ArgumentParser(description='Process betting slips and update results.')
    parser.add_argument('--images', type=str, help='Path to folder containing betting slip images')
    parser.add_argument('--year', type=int, default=2025, help='Year for date processing')
    parser.add_argument('--dry-run', action='store_true', help='Log changes without applying them')
    parser.add_argument('--skip-ocr', action='store_true', help='Skip OCR processing')
    parser.add_argument('--skip-update', action='store_true', help='Skip database update')
    parser.add_argument('--skip-upload', action='store_true', help='Skip upload to sheets')
    parser.add_argument('--reprocess', action='store_true', help='Reprocess entries marked for review')
    
    args = parser.parse_args()
    
    try:
        # Create data directory if it doesn't exist
        Path('data').mkdir(exist_ok=True)

        # Process images if path provided and not skipped
        if (args.images or args.reprocess) and not args.skip_ocr:
            if not process_images(args.images, args.year, args.reprocess):
                logger.error("Exiting due to OCR processing failure")
                return
                
        # Update database if not skipped
        if not args.skip_update:
            if not update_database():
                logger.error("Exiting due to database update failure")
                return
                
        # Upload to sheets if not skipped
        if not args.skip_upload:
            if not upload_results(args.dry_run):
                logger.error("Exiting due to upload failure")
                return
                
        logger.info("Workflow completed successfully")
        
    except Exception as e:
        logger.error(f"Workflow failed: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()