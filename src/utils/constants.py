"""
Constants used throughout the NBA Props Analyzer.
"""

# NBA Season 
SEASON = '2024-25'

# Validation ranges
VALID_RANGES = {
    'score': (20.00, 100.00),
    'odds': (-1000, 1000)
}

# Player name standardization mappings
NAME_REPLACEMENTS = {
    "Alex Sarr": "Alexandre Sarr",
    "Shai Gilgeous-Alex": "Shai Gilgeous-Alexander",
    "Jabari Smith": "Jabari Smith Jr.",
    "Victor Wembanyan": "Victor Wembanyama",
    "Victor Wembanyam": "Victor Wembanyama",
    "Jaren Jackson": "Jaren Jackson Jr.",
    "Tim Hardaway": "Tim Hardaway Jr.",
    "Gary Trent": "Gary Trent Jr.",
    "Kevin Porter": "Kevin Porter Jr.",
    "Larry Nance": "Larry Nance Jr.",
    "Karl-Anthony Town": "Karl-Anthony Towns",
    "Alperen Şengün": "Alperen Sengun",
    "Brandin Podziemsk": "Brandin Podziemski",
    "Giannis Antetokour": "Giannis Antetokounmpo",
    "Bogdan Bogdanovi": "Bogdan Bogdanović",
    "Guerschon Yabuse": "Guerschon Yabusele",
    "Ron Holland": "Ronald Holland II",
    "Simone Fontecchia": "Simone Fontecchio",
    "Bennedict Mathurir": "Bennedict Mathurin",
    "Haywood Highsmit": "Haywood Highsmith",
    "Dorian Finney-Smit": "Dorian Finney-Smith",
    "Nickeil Alexander-V": "Nickeil Alexander-Walker",
    "Kristaps Porziņgis": "Kristaps Porziņģis",
    "Trayce Jackson-Da": "Trayce Jackson-Davis",
    "Kentavious Caldwe": "Kentavious Caldwell-Pope",
    "Gary Payton": "Gary Payton II",
    "Ricky Council": "Ricky Council IV",
    "Nick Smith": "Nick Smith Jr.",
    "Dereck Lively": "Dereck Lively II",
    "Andre Jackson": "Andre Jackson Jr.",
    "Jaime Jaquez": "Jaime Jaquez Jr.",
    "Kelly Oubre": "Kelly Oubre Jr.",
    "Wendell Carter": "Wendell Carter Jr.",
    "Trey Murphy": "Trey Murphy III",
    "Derrick Jones": "Derrick Jones Jr.",
    "Wendell Carter": "Wendell Carter Jr.",
    "Scotty Pippen": "Scotty Pippen Jr.",
    "Michael Porter": "Michael Porter Jr.",
    "Dante Exum": "Danté Exum",
    "Tristan Vukčević": "Tristan Vukcevic",
    "Jakob Poeltl": "Jakob Pöltl",
    "Vince Williams": "Vince Williams Jr.",
    "Sandro Mamukelas": "Sandro Mamukelashvili",
    "Jeff Dowtin": "Jeff Dowtin Jr.",
    "Jalen Hood-Schifin": "Jalen Hood-Schifino",
    
    # Players that don't have names in API for whatever reason
    # Adding them here allows the other bets in images to be processed
    "Bismack Biyombo": "Bismack Biyombo", 
    "Markelle Fultz": "Markelle Fultz", 
    "Damion Baugh": "Damion Baugh", 
    "A.J. Lawson": "A.J. Lawson"
}

# Bet type standardization mappings
TYPE_REPLACEMENTS = {
    'Reb+Ast': 'rebs_asts',
    'Pts+Ast': 'pts_asts',
    'Pts+Reb': 'pts_rebs',
    '3pts': 'three_pointers',
    'Pts+Reb+Ast': 'par',
    'Blocks': 'blocks',
    'Steals': 'steals',
    'Turnovers': 'turnovers', 
    'Points': 'points',
    'Assists': 'assists',
    'Rebounds': 'rebounds'
}

# Ordered bet types as they appear in images
ORDERED_OCR_BET_TYPES = [
    "Pts+Reb+Ast", "Pts+Reb", "Pts+Ast", "Reb+Ast",
    "Blocks", "Steals", "Turnovers", "Points",
    "Assists", "Rebounds", "3pts"
]

