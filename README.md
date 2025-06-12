
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

## Data Flow

1. Betting slip images → OCR extraction → Raw bet data
2. Raw bet data → Validation → Review or processing queue
3. Processing queue → NBA API lookup → Game statistics → Bet outcomes
4. Bet outcomes → Aggregation → Performance analysis → Google Sheets

## Why?

Prior to this workflow, bets in the images had to be manually researched and entered to a Google Sheet that only stored aggregated results and volume of each bet type and score. This process took around 50 seconds per entry, was prone to manual entry errors, and had extremely one-dimensional analysis. I built this pipeline to improve efficiency, scale the project to handle every bet possible, and properly analyze results to improve ROI.  

## Results 
(specific results and strategy not included)

Key results were derived from plots showing Cumulative ROI vs. Number of Bets for each bet type (e.g. Points, Assists) and 'score' range (custom edge metric). These plots allowed us to examine trends and variability in our results, as well as relationships between bet types and score. I presented these plots with simple rule recommendations for future bets: A. ignore one of the 11 bet types as it's extremely variable with low ROI. B. Slightly raise the minimum score range for 3 of the 10 other bet types as they're losing money at low score ranges but profitable above that. 

These rules would filter out only 30% of the investments and increase ROI% by 2.8 and raw profit by 26.4%. 

Note these are simple rules made with careful consideration of overfitting on past results, understanding that anyone can look at past investments and make "rules" with insane profit. My rules were derived primarily from domain knowledge of basketball and our "score" metric, with a sample size strong enough to support them early in the season.


## TODO
This project isn't being updated as the NBA regular season ended and it won't be used until Fall 2025. While functional, there are areas that could be cleaned up:

1. Improve consistency of documentation
2. Remove redundancies (e.g. validation; main; logging; imports; image_processor.process_folder return type)
3. Extract database and player table initialization from image_processor.py
4. Move upload_bets.py to it's own src folder
5. Add visualizations to GitHub (filtering out sensitive data)
