# Video Game Library Enrichment ETL Pipline
A Python data pipelnie that enriches my personal video game library data with external sources, creating datasets ready for further analytic and machine learning projects.

## Project Overview

### Purpose
The purpose of this ETL pipeline to automatically build out a personal video game analytics data model based on the games available in my personal library (managed by the Playnite application).

### What it Does:
- **Generates and Extracts** game data from the following sources:
    - **Playnite** - a universal game library manager
    - **HowLongToBeat (HLTB)** - a community-driven website containing aggregated playtimes for thousands of video games
    - **Internet Games Database (IGDB)** - an online database containing comprehensive information about thousands of video games
- **Connects** the three data sources together via search and fuzzy matching
- **Reports** on matching quality and unmatched entries for both HLTB and IGDB matches
- **Outputs** a clean, ready-to-consume data model for use in future analytic projects


## Architecture & Data Flow

### Pipeline:
![game-library-enrichment-pipeline](https://github.com/user-attachments/assets/50d7e008-70f8-40ce-8f74-1ab0060b15ee)

### Core ETL Components
- **`playnite_library_extract.ps1`** - PowerShell script embedded within the Playnite application that exports library data (not run within this project)
- **`pipeline.py`** - Main orchestrator that coordinates all ETL processes
- **`game_library.py`** - Extracts and cleans Playnite library exports
- **`how_long_to_beat.py`** - Utilizes the howlongtobeatpy Python API to search and match library games with HLTB games to extract community playtime data
- **`internet_games_database.py`** - Retrieves datasets form the IGDB API's endpoints, and performs fuzzy matching with library data to match games
- **`utils.py`** - Utilities script for configuration, logging, and CLI handling

## Key Technical Features

### Smart Data Matching
- **Fuzzy matching algorithms** to connect games across different databases
- **Comprehensive reporting** on match quality and unmatched entries

### Flexible & Reliable Processing
- **Modular design** - run the full pipeline or individual components
- **Configuration-driven** - easily switch input and output files and paths
- **Detailed logging** - track progress and troubleshoot issues

### Automation Ready
- **CLI interface** designed for scheduled execution
- **Sample data mode** for testing without API credentials
- **Task Scheduler compatible** for automated daily/weekly runs

### Prerequisites
- Python 3.12+
- *Optional: IGDB API credentials (free from Twitch Developer Portal)*
    - *Sample file exists to skip IGDB API credentials (see configuration setup below)*
- *Optional: Playnite installation for full functionality*
    - *NOTE: without Playnite, many manual steps will be required to setup library data extraction for pipeline*

### Installation
```bash
git clone https://github.com/tkautzman13/game-library-enrichment-etl.git
cd game-library-enrichment-etl
pip install -r requirements.txt
```

### Configuration
If you wish to run your own library through the pipeline:
1. Copy `sample.yaml` to `config.yaml`
2. Update file paths and add your IGDB API credentials:
```yaml
igdb:
  client_id: "your_client_id"
  client_secret: "your_client_secret"

data:
  library_source_file: "data/input/game_library.csv"
```
3. The project will output CSVs for the data model within the project by default. If you wish to change this, adjust the following variables:
```yaml
data:
  raw_path: "data/test/raw/"
  processed_path: "data/test/processed/"
  hltb_raw_path: "data/test/raw/hltb_extracts/"
  hltb_issues_report_path: "data/test/reports/hltb_issues/"
  igdb_raw_path: "data/test/raw/igdb/"
  igdb_issues_report_path: "data/test/reports/igdb_issues/"
```

If you want to run the script without using your own library and/or without setting up igdb credentials, simply rename sample.yaml to config.yaml and make sure you specify the --skip_igdb argument (see Quick Start walkthrough below).


### Quick Start
```bash
# Test with provided sample data (no API credentials needed) - IF you renamed sample.yaml to config.yaml
python pipeline.py --skip_igdb

# Test with provided sample data (no API credentials needed) - IF you didn't rename sample.yaml
python pipeline.py --config sample.yaml --skip_igdb 

# Run full enrichment pipeline
python pipeline.py

# Run specific components only
python pipeline.py --config config.yaml --library
python pipeline.py --config config.yaml --hltb
python pipeline.py --config config.yaml --library --igdb
python pipeline.py --config config.yaml --library --skip_igdb
```

## Output & Results

![game-library-enrichment-erd](https://github.com/user-attachments/assets/0b4106e6-7b09-4c95-8ab3-36bc4076247d)

### Generated Datasets
- **`playnite_library_igdb.csv`** - Cleaned library with IGDB game IDs
- **`hltb_playtimes.csv`** - HowLongToBeat community playtime data
- **`igdb_games.csv`** - Main IGDB games table
- **`igdb_game_types.csv`** - game types (main game, expansion, DLC, etc.)
- **`igdb_genres.csv`** - game genres (Point-and-click, Fighting, Shooter, etc.)
- **`igdb_franchises.csv`** - game franchises (Halo, Call of Duty, Madden, etc.)
- **`igdb_keywords.csv`** - words or phrases that get tagged to a game ('World War 2', 'Steampunk', etc.)
- **`igdb_themes.csv`** - game theme (Drama, Non-fiction, Sandbox, etc.)
* **`igdb_player_perspectives.csv` - describes the view/perspective of the player in a game (First person, Third Person, Text, etc.)
- **`hltb_matching_reports/`** - Detailed reports on data quality and matching success for HLTB
- **`igdb_matching_reports/`** - Detailed reports on data quality and matching success for IGDB

Note that there are many more endpoints available via the IGDB API that could be added as output datasets. The ones listed above are those that I personally picked that believed would be relevant to future analytic projects.

## What's Next

This ETL pipeline creates the foundation for several downstream projects:

- **Playtime Prediction Models**: ML models to predict personal game completion times
- **Integration with Other Gaming ETL Pipelines**: Connect enriched library data with other datasets such as personal playtime history, game reflections, game screenshots and other datasets to allow for the following projects:
    - **Daily Playtime Forecasting**: Forecast future daily playtimes
    - **Game Reflection NLP Projects**: Further enhance knowledge of personal gaming preferences
    - **Game Screenshot Computer Vision Projects**: Cluster games with similar artistic design, identify charcters and UI elements on screen, etc.

## Contributing

This is primarily a personal project, but suggestions and improvements are welcome! Please open an issue to discuss any major changes.

## License

MIT License - feel free to adapt for your own gaming data projects.