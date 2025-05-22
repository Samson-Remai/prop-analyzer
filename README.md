
# NBA Props Analyzer

## Project Overview
A comprehensive data pipeline for processing and analyzing NBA player prop bets. This project automates the extraction of betting data from image slips, validates outcomes against real NBA statistics, generates performance analytics, and uploads results to a dashboard. This pipeline increases scalability, allowing analysis of every potential NBA player prop bet, and eliminates roughly 200 hours of manual labor per season. 

## Workflow

### 1. Data Extraction
- PDF screenshots of betting slips are processed through OCR (EasyOCR)
- The system extracts player names, bet types, lines, odds, and custom metrics while ignoring noise
- Initial validation flags potential data quality issues for review

### 2. Data Validation & Correction
- Extracted data undergoes validation against expected patterns and ranges
- Flagged entries are exported to CSV for manual review
- Corrected data is reintegrated into the processing pipeline

### 3. Performance Analysis
- The system queries the NBA API for official game statistics 
- Player performance is compared against betting lines
- Results and other performance metrics are calculated for each bet

### 4. Results Processing
- Bet outcomes are grouped by bet type and custom metric ranges
- Raw results are adjusted by bet odds
- Aggregated results are calculated to identify performance patterns

### 5. Reporting & Visualization
- Results are exported to private Google Sheets for dashboard visualization
- References are stored in database to enable future analysis
- Processed bets are marked to prevent duplicates

## SQL Architecture
```
┌─────────────┐     ┌──────────────┐     ┌──────────────┐     ┌────────────┐
│  OCR Image  │     │ Data Quality │     │  NBA Stats   │     │ Aggregated │
│  Processor  │────▶│    Review   │────▶│  Processing  │────▶│   Results │
└─────────────┘     └──────────────┘     └──────────────┘     └────────────┘
       │                   │                    │                   │
       ▼                   ▼                    ▼                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                            SQLite Database                               │
└─────────────────────────────────────────────────────────────────────────┘
```

1. **image_processor.py**: 
    - Writes to `raw_ocr_bets`
    - References `players`
2. **review_handler.py**: 
    - Reads from `raw_ocr_bets` where `needs_review = 1`
    - References `players`
    - Updates `raw_ocr_bets.needs_review = 0` 
    - Updates `raw_ocr_bets.is_voided = 1` if bet is deleted in manual review
3.  **process_bets.py**: 
    - Reads `raw_ocr_bets` where `is_processed = 0`, `is_voided = 0` and `needs_review = 0`
    - Reads from `game_stats` and `unplayed_bets` to check for cached data and eliminate redundant API calls
    - References `players`
    - Writes to `unplayed_bets` if player has no stats
    - Writes to `game_stats` and `bet_results` if player has stats
    - Updates `raw_ocr_bets.is_processed = 1`
4. **upload_bets.py**: 
    - Reads `bet_results` where `is_uploaded = 0`
    - References `players`
    - Writes to `aggregated_results`
    - Updates `bet_results.is_uploaded = 1`

## Data Flow

1. Betting slip images → OCR extraction → Raw bet data
2. Raw bet data → Validation → Review or processing queue
3. Processing queue → NBA API lookup → Game statistics → Bet outcomes
4. Bet outcomes → Aggregation → Performance analysis → Google Sheets

## Some Challenges

- Some of the images are quite messy, with cursors, text highlighting, screenshot sizing, etc. throwing off OCR. Considering this is financial data, validation is the single most important part of this project. I designed a flagging system which exports suspect entries to a CSV and their corresponding screenshots to a folder. This allows for easy manual data entry, where I can fill in whatever OCR misread and re-run the script to update the database with my changes. 

- The images include anywhere from 2 to 20 bets with only those greater than a 20% 'score' being relevant. I couldn't simply use regex to find all odds or betting lines because it would include odds/lines with an associated score < 20. I worked around this by applying regex sequentially, first extracting full bet strings related to a 20%+ score, then extracting detailed information from that string.


## TODO
This project isn't being updated as the NBA regular season ended and it won't be used until Fall 2025. While functional, there are areas that could be cleaned up:

1. Improve consistency of documentation
2. Remove redundancies (e.g. validation; main; logging; imports; image_processor.process_folder return type)
3. Extract database and player table initialization from image_processor.py
4. Move upload_bets.py to it's own src folder
