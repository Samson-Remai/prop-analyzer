"""
Path and environment configuration for the NBA Props Analyzer.
"""

from pathlib import Path

# Root directory of the project (one level up from src/)
ROOT_DIR = Path(__file__).parent.parent.parent

# Directory paths
DATA_DIR = ROOT_DIR / "data"
LOGS_DIR = ROOT_DIR / "logs"
SRC_DIR = ROOT_DIR / "src"
# Image directories
IMAGES_DIR = DATA_DIR / "images" / "2025"  # Source images

# Review information directories
REVIEWS_DIR = DATA_DIR / "review_info"
REVIEW_CSV_DIR = REVIEWS_DIR / "review_csvs"  # Review CSVs
REVIEW_IMAGES_DIR = REVIEWS_DIR / "review_images"  # Images for review
VOIDED_IMAGES_DIR = REVIEWS_DIR / "voided_images"  # Voided bet images
UPDATES_DIR = REVIEWS_DIR / "update_csvs"  # Updates

# Database
DB_PATH = ROOT_DIR / "sports_bets.db"

# Credentials
CREDENTIALS_PATH = SRC_DIR / "utils" / "sensitive" / "credentials.json"



# Ensure paths are strings for sqlite
DB_PATH = str(DB_PATH)
CREDENTIALS_PATH = str(CREDENTIALS_PATH)

# Create necessary directories
for directory in [DATA_DIR, LOGS_DIR, IMAGES_DIR, REVIEW_IMAGES_DIR, 
                 VOIDED_IMAGES_DIR, REVIEW_CSV_DIR, UPDATES_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

__all__ = [
    'ROOT_DIR',
    'DATA_DIR',
    'LOGS_DIR',
    'IMAGES_DIR',
    'REVIEW_IMAGES_DIR',
    'VOIDED_IMAGES_DIR',
    'REVIEW_CSV_DIR',
    'DB_PATH',
    'CREDENTIALS_PATH',
] 